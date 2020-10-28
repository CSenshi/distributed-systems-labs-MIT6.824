package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type MapTask struct {
	state State
}

type RedTask struct {
	state State
}

type Master struct {
	mapTasks []MapTask
	redTasks []RedTask
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	DPrintf(makeMasterRequest("New Requst Task from worker"))
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	DPrintf(makeMasterRequest("New Master Server Created! ToTal Files: %v | nReduce: %v "), len(files), nReduce)
	m := Master{}
	// Your code here.

	m.server()
	return &m
}
