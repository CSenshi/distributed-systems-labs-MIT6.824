package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	TaskID   int    // ID of MapTask in Master server
	FileName string // Name of file to be proccessed by mappers
	NReduce  int    // Total number of reducer tasks (used to hash kv)
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
