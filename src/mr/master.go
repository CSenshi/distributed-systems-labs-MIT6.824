package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

// MapTask used for Map phase tasks
type MapTask struct {
	state    State
	FileName string
	ID       int
}

// RedTask used for Reduce phase tasks
type RedTask struct {
	state State
	ID    int
}

// Master keeps track of all data that is needed
type Master struct {
	mu   *sync.Mutex
	cond *sync.Cond

	mapTasks []*MapTask
	redTasks []*RedTask

	nReduce          int // Number of Reduce Tasks
	nMap             int // Number of Map Tasks
	mapTasksToFinish int
	redTasksToFinish int
}

// RequestTask RPC handles giving tasks to workers
// We can ignore RequestTaskArg because tehre is nothing needed from worker
// We just need RequestTaskReply to send response back
func (m *Master) RequestTask(_ *struct{}, reply *RequestTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	reply.NReduce = m.nReduce
	reply.NMap = m.nMap
	reply.TaskType = nop

	// Send Map Tasks
	for _, task := range m.mapTasks {
		if task.state == idle {
			reply.Map = *task
			reply.TaskType = mapTask
			task.state = inProgress
			return nil
		}
	}

	// Wait for all reducers to complete their task
	for m.mapTasksToFinish > 0 {
		m.cond.Wait()
	}

	// Send Reduce Tasks
	for _, task := range m.redTasks {
		if task.state == idle {
			reply.Red = *task
			reply.TaskType = redTask
			task.state = inProgress
			return nil
		}
	}
	return nil
}

// TaskDone handles marking tasks as completed when workers send notify
// We can ignore DoneTaskReply because there is nothing to send back
// We just care about DoneTaskArgs to know which task was completed
func (m *Master) TaskDone(args *DoneTaskArgs, _ *struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.TaskType == mapTask {
		m.mapTasks[args.TaskID].state = completed
		m.mapTasksToFinish--
		DPrintf("Map Worker Finished Working, Left : %v", m.mapTasksToFinish)
		if m.mapTasksToFinish == 0 {

			DPrintf(newPhase("%[1]v Starting Reduce Phase %[1]v"), strings.Repeat("-", 20))
			m.cond.Broadcast()
		}
	} else if args.TaskType == redTask {
		m.redTasks[args.TaskID].state = completed
		m.redTasksToFinish--
		DPrintf("Reduce Worker Finished Working, Left : %v", m.redTasksToFinish)
	}

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

// Done is periodically called by main/mrmaster.go to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.mapTasksToFinish <= 0 && m.redTasksToFinish <= 0
}

// MakeMaster is called by main/mrmaster.go.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	DPrintf(makeMasterRequest("New Master Server Created!"))
	m := Master{}
	m.mu = &sync.Mutex{}
	m.cond = sync.NewCond(m.mu)

	// Create MapTasks with given files
	m.mapTasks = make([]*MapTask, len(files))
	for i, file := range files {
		m.mapTasks[i] = new(MapTask)
		m.mapTasks[i].FileName = file
		m.mapTasks[i].state = idle
		m.mapTasks[i].ID = i
	}
	DPrintf(makeMasterRequest("Map Tasks | Total: %v"), len(files))

	// Create RedTasks with given nReduce count
	m.redTasks = make([]*RedTask, nReduce)
	for i := 0; i < nReduce; i++ {
		m.redTasks[i] = new(RedTask)
		m.redTasks[i].state = idle
		m.redTasks[i].ID = i
	}
	DPrintf(makeMasterRequest("Reduce Tasks | Total: %v"), nReduce)

	// Save number of Map/Reduce Tasks
	m.nMap = len(files)
	m.nReduce = nReduce
	m.mapTasksToFinish = len(m.mapTasks)
	m.redTasksToFinish = len(m.redTasks)

	// Serve
	DPrintf(newPhase("%[1]v Starting Map Phase %[1]v"), strings.Repeat("-", 20))
	m.server()
	return &m
}
