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

// RequestTaskReply RPC
type RequestTaskReply struct {
	Map      MapTask
	Red      RedTask
	TaskType TaskType
	NReduce  int // Total number of reducer tasks (used to hash kv)
	NMap     int // Total number of maper tasks (used to iterate in reduce phase)
}

// DoneTaskArgs RPC
type DoneTaskArgs struct {
	TaskID   int
	TaskType TaskType
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/820-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
