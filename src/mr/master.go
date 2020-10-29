package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type MapTask struct {
	state    State
	fileName string
	ID       int
}

type RedTask struct {
	state State
}

type Master struct {
	mu       sync.Mutex
	mapTasks []MapTask
	redTasks []RedTask

	mapTasksToFinish int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Send Map Tasks
	for i, task := range m.mapTasks {
		if task.state == idle {
			reply.FileName = task.fileName
			reply.TaskID = task.ID
			m.mapTasks[i].state = inProgress
			return nil
		}
	}

	// Send Reduce Tasks
	// ToDo
	return nil
}

func (m *Master) TaskDone(args *DoneTaskArgs, reply *DoneTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mapTasks[args.TaskID].state = completed
	m.mapTasksToFinish--
	DPrintf("Worker Finished Working, Left : %v", m.mapTasksToFinish)
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
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.mapTasksToFinish <= 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	DPrintf(makeMasterRequest("New Master Server Created!"))
	m := Master{}

	// Create MapTasks with given files
	m.mapTasks = make([]MapTask, len(files))
	for i, file := range files {
		m.mapTasks[i].fileName = file
		m.mapTasks[i].state = idle
		m.mapTasks[i].ID = i
	}
	DPrintf(makeMasterRequest("Map Tasks | Total: %v"), len(files))
	m.mapTasksToFinish = len(m.mapTasks)

	// Create RedTasks with given nReduce count
	m.redTasks = make([]RedTask, nReduce)
	DPrintf(makeMasterRequest("Reduce Tasks | Total: %v"), nReduce)

	// Serve
	m.server()
	return &m
}
