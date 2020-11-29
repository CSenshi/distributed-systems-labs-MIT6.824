package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type OpType int

const (
	OpGet    = iota
	OpPut    = iota
	OpAppend = iota
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    OpType // "Put" or "Append"
	ID    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ID  int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
