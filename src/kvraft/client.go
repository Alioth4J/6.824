package kvraft

import (
	"crypto/rand"
	"github.com/alioth4j/6.824/src/labrpc"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	leader  int
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
	ck.updateLeader()
	return ck
}

func (ck *Clerk) updateLeader() {
	for i := 0; i < len(ck.servers); i++ {
		isLeaderArgs := &IsLeaderArgs{}
		isLeaderReply := &IsLeaderReply{}
		ck.servers[i].Call("KVServer.IsLeader", isLeaderArgs, isLeaderReply)
		if isLeaderReply.Leader {
			ck.leader = i
			break
		}
	}
}

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
	for {
		getArgs := &GetArgs{
			Key:       key,
			RequestId: nrand(),
		}
		getReply := &GetReply{}
		ck.servers[ck.leader].Call("KVServer.Get", getArgs, getReply)
		if getReply.Err == OK {
			return getReply.Value
		} else if getReply.Err == ErrNoKey {
			return ""
		} else if getReply.Err == ErrWrongLeader {
			ck.updateLeader()
		} else if getReply.Err == ErrTimeout {
			// retry
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
	for {
		putAppendArgs := &PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			RequestId: nrand(),
		}
		putAppendReply := &PutAppendReply{}
		ck.servers[ck.leader].Call("KVServer.PutAppend", putAppendArgs, putAppendReply)
		if putAppendReply.Err == OK {
			return
		} else if putAppendReply.Err == ErrWrongLeader {
			ck.updateLeader()
		} else if putAppendReply.Err == ErrTimeout {
			// retry
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
