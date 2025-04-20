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
	RequestId int64
}

type PutAppendReply struct {
	Err Err
}

type IsLeaderArgs struct{}

type IsLeaderReply struct {
	Leader bool
}
