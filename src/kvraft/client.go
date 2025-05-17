package kvraft

import (
	"crypto/rand"
	"math/big"
	"github.com/alioth4j/6.824/src/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leader   int
	clientId int64
	seq int
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
	ck.seq = 0
	return ck
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
	args := &GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		Seq: ck.seq,
	}
	ck.seq++
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
		if !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			return reply.Value
		} else if reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		} else if reply.Err == ErrTimeout {
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
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		Seq: ck.seq,
	}
	ck.seq++
	for {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
		if !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			return
		} else if reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		} else if reply.Err == ErrTimeout {
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