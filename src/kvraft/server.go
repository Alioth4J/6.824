package kvraft

import (
	"github.com/alioth4j/6.824/src/labgob"
	"github.com/alioth4j/6.824/src/labrpc"
	"github.com/alioth4j/6.824/src/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const timeout = 500 * time.Millisecond

type Op struct {
	Key      string
	Value    string // only used in PutAppend
	Type     string // "Get", "Put", "Append"
	ClientId int64
	Seq      int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	applyCh chan raft.ApplyMsg
	rf      *raft.Raft

	kvmap            map[string]string
	lastAppliedOpMap map[int64]*AppliedOp
	pendingOpMap     map[int64]*PendingOp

	maxraftstate int // snapshot if log grows this big

	dead int32 // set by Kill()
}

type OpResult struct {
	Err   Err
	Value string // only used in GET
}

type AppliedOp struct {
	Seq      int64
	OpResult OpResult
}

type PendingOp struct {
	seq    int64
	index  int
	result chan OpResult
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
	kv.kvmap = make(map[string]string)
	kv.lastAppliedOpMap = make(map[int64]*AppliedOp)
	kv.pendingOpMap = make(map[int64]*PendingOp)
	kv.maxraftstate = maxraftstate

	go func() {
		for !kv.killed() {
			applyMsg := <-kv.applyCh
			if !applyMsg.CommandValid {
				continue
			}
			kv.mu.Lock()
			op := applyMsg.Command.(Op)
			lastApplied := kv.getLastApplied(op.ClientId)
			opResult := OpResult{}

			if op.Seq > lastApplied.Seq {
				if op.Type == "Get" {
					value, ok := kv.kvmap[op.Key]
					if !ok {
						opResult.Err = ErrNoKey
					} else {
						opResult.Err = OK
						opResult.Value = value
					}
				} else if op.Type == "Put" {
					kv.kvmap[op.Key] = op.Value
					opResult.Err = OK
				} else if op.Type == "Append" {
					oldValue, ok := kv.kvmap[op.Key]
					if ok {
						kv.kvmap[op.Key] = oldValue + op.Value
					} else {
						kv.kvmap[op.Key] = op.Value
					}
					opResult.Err = OK
				}
				lastApplied.Seq = op.Seq
				lastApplied.OpResult = opResult
			} else {
				opResult = lastApplied.OpResult
			}

			if pendingOp, ok := kv.pendingOpMap[op.ClientId]; ok && pendingOp.seq == op.Seq {
				pendingOp.result <- opResult
				delete(kv.pendingOpMap, op.ClientId)
			}

			kv.mu.Unlock()
		}
	}()

	return kv
}

func (kv *KVServer) getLastApplied(clientId int64) *AppliedOp {
	if lastApplied, ok := kv.lastAppliedOpMap[clientId]; ok {
		return lastApplied
	}
	lastApplied := &AppliedOp{
		Seq: -1,
	}
	kv.lastAppliedOpMap[clientId] = lastApplied
	return lastApplied
}

func (kv *KVServer) IsLeader(args *IsLeaderArgs, reply *IsLeaderReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	reply.Leader = isLeader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()

	// is leader?
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	lastApplied := kv.getLastApplied(args.ClientId)

	// has been applied?
	if args.Seq <= lastApplied.Seq {
		reply.Err = lastApplied.OpResult.Err
		reply.Value = lastApplied.OpResult.Value
		kv.mu.Unlock()
		return
	}

	// pending?
	if pendingOp, ok := kv.pendingOpMap[args.ClientId]; ok && args.Seq == pendingOp.seq {
		kv.mu.Unlock()
		time.Sleep(timeout)
		lastApplied := kv.getLastApplied(args.ClientId)
		if args.Seq <= lastApplied.Seq {
			reply.Err = lastApplied.OpResult.Err
			reply.Value = lastApplied.OpResult.Value
		} else {
			reply.Err = ErrTimeout
		}
		return
	}

	// send request to raft
	op := Op{
		Key:      args.Key,
		Type:     "Get",
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	resultChan := make(chan OpResult, 1)
	kv.pendingOpMap[args.ClientId] = &PendingOp{
		seq:    args.Seq,
		index:  index,
		result: resultChan,
	}

	kv.mu.Unlock()

	select {
	case opResult := <-resultChan:
		reply.Err = opResult.Err
		reply.Value = opResult.Value
	case <-time.After(timeout):
		kv.mu.Lock()
		currentTerm, currentIsLeader := kv.rf.GetState()
		if currentTerm != term || !currentIsLeader {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = ErrTimeout
		}
		if pendingOp, ok := kv.pendingOpMap[args.ClientId]; ok && pendingOp.seq == args.Seq {
			delete(kv.pendingOpMap, args.ClientId)
		}
		kv.mu.Unlock()
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

	// has been applied?
	lastApplied := kv.getLastApplied(args.ClientId)
	if args.Seq <= lastApplied.Seq {
		reply.Err = lastApplied.OpResult.Err
		kv.mu.Unlock()
		return
	}

	// pending?
	if pendingOp, ok := kv.pendingOpMap[args.ClientId]; ok && args.Seq == pendingOp.seq {
		kv.mu.Unlock()
		time.Sleep(timeout)
		lastApplied := kv.getLastApplied(args.ClientId)
		if args.Seq <= lastApplied.Seq {
			reply.Err = lastApplied.OpResult.Err
		} else {
			reply.Err = ErrTimeout
		}
		return
	}

	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		Type:     args.Op,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	resultChan := make(chan OpResult, 1)
	kv.pendingOpMap[args.ClientId] = &PendingOp{
		seq:    args.Seq,
		index:  index,
		result: resultChan,
	}

	kv.mu.Unlock()

	select {
	case opResult := <-resultChan:
		reply.Err = opResult.Err
	case <-time.After(timeout):
		kv.mu.Lock()
		currentTerm, currentIsLeader := kv.rf.GetState()
		if currentTerm != term || !currentIsLeader {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = ErrTimeout
		}
		if pendingOp, ok := kv.pendingOpMap[args.ClientId]; ok && pendingOp.seq == args.Seq {
			delete(kv.pendingOpMap, args.ClientId)
		}
		kv.mu.Unlock()
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
