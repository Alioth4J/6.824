package kvraft

import (
	"bytes"
	//"log"
	"fmt"
	"github.com/alioth4j/6.824/src/labgob"
	"github.com/alioth4j/6.824/src/labrpc"
	"github.com/alioth4j/6.824/src/raft"
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
	Key      string
	Value    string // only used in PutAppend
	Type     string // "Get", "Put", "Append"
	ClientId int64
	Seq      int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	applyCh chan raft.ApplyMsg
	rf      *raft.Raft

	kv          map[string]string
	pendingCh   map[string]*NotifyCh
	lastApplied map[int64]int // key: clientId, value: seq

	maxraftstate int // snapshot if log grows this big

	dead int32 // set by Kill()
}

type NotifyCh struct {
	getCh       chan string
	putAppendCh chan struct{}
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

	kv := &KVServer{
		me:           me,
		applyCh:      make(chan raft.ApplyMsg),
		kv:           make(map[string]string),
		pendingCh:    make(map[string]*NotifyCh),
		lastApplied:  make(map[int64]int),
		maxraftstate: maxraftstate,
		dead:         0,
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// apply with goroutine
	go kv.apply()

	// return
	return kv
}

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kv)
	e.Encode(kv.lastApplied)
	return w.Bytes()
}

func (kv *KVServer) decodeSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvInSnapshot map[string]string
	var clientSeqs map[int64]int
	if d.Decode(&kvInSnapshot) != nil || d.Decode(&clientSeqs) != nil {
		// ignore error
	} else {
		kv.kv = kvInSnapshot
		kv.lastApplied = clientSeqs
	}
}

func (kv *KVServer) apply() {
	for applyMsg := range kv.applyCh {
		if kv.killed() {
			return
		}

		kv.mu.Lock()
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			pendingKey := fmt.Sprintf("%d-%d", op.ClientId, op.Seq)

			_, exists := kv.lastApplied[op.ClientId]
			if !exists {
				kv.lastApplied[op.ClientId] = -1
			}

			if op.Seq <= kv.lastApplied[op.ClientId] {
				if ch, ok := kv.pendingCh[pendingKey]; ok {
					switch op.Type {
					case "Get":
						ch.getCh <- kv.kv[op.Key]
						break
					case "Put", "Append":
						ch.putAppendCh <- struct{}{}
					}
				}
				kv.mu.Unlock()
				continue
			}

			if op.Seq > kv.lastApplied[op.ClientId]+1 {
				kv.mu.Unlock()
				continue
			}

			// Exactly: op.Seq == kv.lastApplied[op.ClientId] + 1
			switch op.Type {
			case "Get":
				value := kv.kv[op.Key]
				if ch, ok := kv.pendingCh[pendingKey]; ok {
					ch.getCh <- value
				}
				break
			case "Put":
				kv.kv[op.Key] = op.Value
				if ch, ok := kv.pendingCh[pendingKey]; ok {
					ch.putAppendCh <- struct{}{}
				}
				break
			case "Append":
				kv.kv[op.Key] += op.Value
				if ch, ok := kv.pendingCh[pendingKey]; ok {
					ch.putAppendCh <- struct{}{}
				}
				break
			}
			kv.lastApplied[op.ClientId] = op.Seq

			// snapshot
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
				snapshot := kv.encodeSnapshot()
				kv.rf.Snapshot(applyMsg.CommandIndex, snapshot)
			}
		} else if applyMsg.IsSnapshot {
			kv.decodeSnapshot(applyMsg.Snapshot)
			kv.pendingCh = make(map[string]*NotifyCh)
		} else {
			// ignore
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Key:      args.Key,
		Type:     "Get",
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	pendingKey := fmt.Sprintf("%d-%d", args.ClientId, args.Seq)
	ch := &NotifyCh{getCh: make(chan string, 1)}
	kv.mu.Lock()
	kv.pendingCh[pendingKey] = ch
	kv.mu.Unlock()
	select {
	case value := <-ch.getCh:
		reply.Err = OK
		reply.Value = value
	case <-time.After(timeout):
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.pendingCh, pendingKey)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		Type:     args.Op,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	pendingKey := fmt.Sprintf("%d-%d", args.ClientId, args.Seq)
	ch := &NotifyCh{putAppendCh: make(chan struct{}, 1)}
	kv.mu.Lock()
	kv.pendingCh[pendingKey] = ch
	kv.mu.Unlock()
	select {
	case <-ch.putAppendCh:
		reply.Err = OK
	case <-time.After(timeout):
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.pendingCh, pendingKey)
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
