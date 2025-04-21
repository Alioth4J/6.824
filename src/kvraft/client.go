package kvraft

import (
	"crypto/rand"
	"github.com/alioth4j/6.824/src/labrpc"
	"math/big"
	"sync/atomic"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leader   int32
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.clientId = nrand()
	//ck.updateLeader()
	return ck
}

//func (ck *Clerk) updateLeader() {
//	args := &IsLeaderArgs{}
//	reply := &IsLeaderReply{}
//
//	if ok := ck.servers[ck.leader].Call("KVServer.IsLeader", args, reply); ok && reply.Leader {
//		return
//	}
//
//	for i := 0; i < len(ck.servers); i++ {
//		if i == ck.leader {
//			continue
//		}
//		if ok := ck.servers[i].Call("KVServer.IsLeader", args, reply); ok && reply.Leader {
//			ck.leader = i
//			return
//		}
//	}
//}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// serial, no need to use lock
	args := &GetArgs{
		Key:      key,
		ClientId: ck.clientId,
	}
	leader := atomic.LoadInt32(&ck.leader)
	for {
		reply := &GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", args, reply)
		if ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				ck.leader = ck.nextLeader(leader)
				continue
			} else if reply.Err == ErrTimeout {
				continue
			}
		} else {
			ck.leader = ck.nextLeader(leader)
			continue
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// serial, no need to use lock
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: nrand(),
	}
	leader := atomic.LoadInt32(&ck.leader)
	for {
		reply := &PutAppendReply{}
		ok := ck.servers[leader].Call("KVServer.PutAppend", args, reply)
		if ok {
			if reply.Err == OK {
				return
			} else if reply.Err == ErrWrongLeader {
				leader = ck.nextLeader(leader)
				continue
			} else if reply.Err == ErrTimeout {
				continue
			}
		} else {
			ck.leader = ck.nextLeader(leader)
			continue
		}
	}
}

func (ck *Clerk) nextLeader(current int32) int32 {
	next := (current + 1) % int32(len(ck.servers))
	atomic.StoreInt32(&ck.leader, next)
	return next

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
