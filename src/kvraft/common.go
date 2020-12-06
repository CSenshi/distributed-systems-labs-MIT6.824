package kvraft

import "fmt"
import "time"

type Err string

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type OpType int

const (
	OpGet    = iota
	OpPut    = iota
	OpAppend = iota
)

func (e OpType) String() string {
	switch e {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpAppend:
		return "Append"
	default:
		return fmt.Sprintf("Undefined Opperation:%d", int(e))
	}
}

const (
	LogWaitTTL = time.Second
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    OpType // "Put" or "Append"
	ID    int64
	CID   int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ID  int64
	CID int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
