package kvraft

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type GetArgs struct {
	Key      string
	ClientId int64
	Seq int
}

type GetReply struct {
	Err   Err
	Value string
}

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	Seq int
}

type PutAppendReply struct {
	Err Err
}