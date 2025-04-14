package kvraft

import (
	"github.com/alioth4j/6.824/src/labgob"
	"github.com/alioth4j/6.824/src/labrpc"
	"github.com/alioth4j/6.824/src/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	//"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string // only used in PutAppend
	Type      string
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	kvmap       map[string]string
	lastApplied int
	opresults   map[int64]chan OpResult
}

type OpResult struct {
	Err   Err
	Value string // only used in GET
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

	kv := new(KVServer)
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.maxraftstate = maxraftstate
	kv.kvmap = make(map[string]string)
	kv.lastApplied = 0
	kv.opresults = make(map[int64]chan OpResult)

	go func() {
		for {
			applyMsg := <-kv.applyCh
			if !applyMsg.CommandValid {
				continue
			}
			kv.mu.Lock()
			op := applyMsg.Command.(Op)
			opResult := OpResult{
				Err: OK,
			}
			if op.Type == "Get" {
				value, ok := kv.kvmap[op.Key]
				if !ok {
					opResult.Err = ErrNoKey
				} else {
					opResult.Value = value
				}
			} else if op.Type == "Put" {
				kv.kvmap[op.Key] = op.Value
			} else if op.Type == "Append" {
				oldValue, ok := kv.kvmap[op.Key]
				if ok {
					kv.kvmap[op.Key] = oldValue + op.Value
				} else {
					kv.kvmap[op.Key] = op.Value
				}
			}

			if _, ok := kv.opresults[op.RequestId]; ok {
				kv.lastApplied = applyMsg.CommandIndex
				kv.opresults[op.RequestId] <- opResult
			}

			kv.mu.Unlock()
		}
	}()

	return kv
}

func (kv *KVServer) IsLeader(args *IsLeaderArgs, reply *IsLeaderReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	reply.Leader = isLeader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	op := Op{
		Key:       args.Key,
		Type:      "Get",
		RequestId: args.RequestId,
	}
	kv.opresults[op.RequestId] = make(chan OpResult, 1)
	_, _, isLeader = kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	select {
	case opResult := <-kv.opresults[op.RequestId]:
		reply.Err = opResult.Err
		reply.Value = opResult.Value
		kv.mu.Lock()
		delete(kv.opresults, op.RequestId)
		kv.mu.Unlock()
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Type:      args.Op,
		RequestId: args.RequestId,
	}
	kv.opresults[op.RequestId] = make(chan OpResult, 1)
	_, _, isLeader = kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	select {
	case opResult := <-kv.opresults[op.RequestId]:
		reply.Err = opResult.Err
		kv.mu.Lock()
		delete(kv.opresults, op.RequestId)
		kv.mu.Unlock()
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
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
