package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Request Task
type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	Map      MapTask
	Red      RedTask
	TaskType TaskType
	NReduce  int // Total number of reducer tasks (used to hash kv)
}

type DoneTaskArgs struct {
	TaskID int
}

type DoneTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/821-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
