package kvraft

import (
	"github.com/alioth4j/6.824/src/labgob"
	"github.com/alioth4j/6.824/src/labrpc"
	"github.com/alioth4j/6.824/src/raft"
	//"log"
	"sync"
	"sync/atomic"
	"time"
)

//const Debug = 0
//
//func DPrintf(format string, a ...interface{}) (n int, err error) {
//	if Debug > 0 {
//		log.Printf(format, a...)
//	}
//	return
//}

const timeout = 500 * time.Millisecond

type Op struct {
	Key       string
	Value     string // only used in PutAppend
	Type      string // "Get", "Put", "Append"
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	applyCh chan raft.ApplyMsg
	rf      *raft.Raft

	kv               map[string]string
	getResults       map[int]chan string   // key: index; value: value
	putAppendResults map[int]chan struct{} // key: index; value: struct{}{}
	lastApplied      int
	duplicateMap     map[int64]int64 // key: clientId; value: Op

	maxraftstate int // snapshot if log grows this big

	dead int32 // set by Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	// new and set
	kv := new(KVServer)
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kv = make(map[string]string)
	kv.getResults = make(map[int]chan string)
	kv.putAppendResults = make(map[int]chan struct{})
	kv.lastApplied = 0
	kv.duplicateMap = make(map[int64]int64)
	kv.maxraftstate = maxraftstate

	// apply with goroutine
	go kv.apply()

	// return
	return kv
}

func (kv *KVServer) apply() {
	for applyMsg := range kv.applyCh {
		if kv.killed() {
			return
		}

		commandValid := applyMsg.CommandValid
		command := applyMsg.Command
		commandIndex := applyMsg.CommandIndex
		if commandValid {
			kv.mu.Lock()

			if commandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}

			op := command.(Op)
			switch op.Type {
			case "Get":
				if resultChan, ok := kv.getResults[commandIndex]; ok {
					if value, ok := kv.kv[op.Key]; ok {
						go func() {
							resultChan <- value
						}()
					} else {
						go func() {
							resultChan <- ""
						}()
					}
				}
			case "Put", "Append":
				if lastRequestId, ok := kv.duplicateMap[op.ClientId]; ok {
					if op.RequestId == lastRequestId {
						if resultChan, ok := kv.putAppendResults[commandIndex]; ok {
							go func() {
								resultChan <- struct{}{}
							}()
						}
						kv.lastApplied = commandIndex
						kv.mu.Unlock()
						continue
					}
				}
				kv.duplicateMap[op.ClientId] = op.RequestId
				if op.Type == "Put" {
					kv.kv[op.Key] = op.Value
				} else if op.Type == "Append" {
					kv.kv[op.Key] += op.Value
				}
				if resultChan, ok := kv.putAppendResults[commandIndex]; ok {
					go func() {
						resultChan <- struct{}{}
					}()
				}
			}

			kv.lastApplied = commandIndex
			kv.mu.Unlock()
		} else {
			// ignore
		}
	}
}

func (kv *KVServer) IsLeader(args *IsLeaderArgs, reply *IsLeaderReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	reply.Leader = isLeader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// lock
	kv.mu.Lock()

	// leader check
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// send request to raft
	op := Op{
		Key:      args.Key,
		Type:     "Get",
		ClientId: args.ClientId,
	}
	index, _, isLeader := kv.rf.Start(op)

	// leader check
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// make the result channel
	resultChan := make(chan string, 1)
	kv.getResults[index] = resultChan

	// unlock and wait for the reply
	kv.mu.Unlock()
	select {
	case value := <-resultChan:
		reply.Err = OK
		reply.Value = value
	case <-time.After(timeout):
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	delete(kv.getResults, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// lock
	kv.mu.Lock()

	// leader check
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// pre-deduplicate
	if lastRequestId, ok := kv.duplicateMap[args.ClientId]; ok && args.ClientId == lastRequestId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	// send request to raft
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Type:      args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := kv.rf.Start(op)

	// leader check
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// make the result channel
	resultChan := make(chan struct{}, 1)
	kv.putAppendResults[index] = resultChan

	// wait for the reply
	kv.mu.Unlock()
	select {
	case <-resultChan:
		reply.Err = OK
	case <-time.After(timeout):
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	delete(kv.putAppendResults, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
