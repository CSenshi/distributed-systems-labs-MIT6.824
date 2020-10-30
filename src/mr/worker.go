package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

const intermediateFname = "mr-%d-%d"

// KeyValue slices are returned by Map functions
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is called by main/mrworker.go
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// 1. Create RPC Argument
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}

		// 2. Send RPC
		ok := sendRequestTaskRPC(&args, &reply)
		if !ok {
			DPrintf(fail("Connection Error: sendRequestTask | worker -> master"))
		}

		// 3. Process RPC Reply
		processRequestTaskReply(&args, &reply, mapf, reducef)

		select {
		case <-time.After(1 * time.Second):
		}
	}
}

func processRequestTaskReply(args *RequestTaskArgs, reply *RequestTaskReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	switch reply.TaskType {
	case mapTask:
		processMapTask(reply, mapf)
	case redTask:
		processRedTask(reply, reducef)
	}
}

func processMapTask(reply *RequestTaskReply, mapf func(string, string) []KeyValue) {
	task := reply.Map
	DPrintf(newTask("Worker Received Map Task: %+v"), task)

	// 1. Open file which master provided
	file, err := os.Open(task.FileName)
	if err != nil {
		DPrintf(fail("Can not open file: %v"), task.FileName)
		return
	}

	// 2. Read file content
	content, err := ioutil.ReadAll(file)
	if err != nil {
		DPrintf(fail("Can not read file content: %v"), task.FileName)
		return
	}
	file.Close()

	// 3. Run Map Task
	kva := mapf(task.FileName, string(content))

	// 4. Save in the file
	// 4.1 Create All Reducers Encoders
	encoders := make([]*json.Encoder, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		intermediateFileName := fmt.Sprintf(intermediateFname, task.ID, i)
		file, err := os.Create(intermediateFileName)
		if err != nil {
			DPrintf(fail("Can not create file: %v"), intermediateFileName)
			return
		}
		encoders[i] = json.NewEncoder(file)
	}

	// 4.2 Write KeyValues to specific file via encoder
	for _, kv := range kva {
		hash := ihash(kv.Key) % reply.NReduce
		err := encoders[hash].Encode(&kv)
		if err != nil {
			DPrintf(fail("Can not write \"%+v\"", kv))
		}
	}

	// 4.3 Close all files
	// ToDo

	// 5. Send Back Task Done RPC
	newArgs := &DoneTaskArgs{TaskID: task.ID}
	ok := sendDoneTaskRPC(newArgs, &DoneTaskReply{})
	if !ok {
		DPrintf(fail("Connection Error: sendDoneTaskRPC worker -> master"))
	}
}

func processRedTask(reply *RequestTaskReply, reducef func(string, []string) string) {
	task := reply.Red
	DPrintf(newTask("Worker Received Map Task: %+v"), task)

}

func sendDoneTaskRPC(args *DoneTaskArgs, reply *DoneTaskReply) bool {
	ok := call("Master.TaskDone", args, reply)
	return ok
}

func sendRequestTaskRPC(args *RequestTaskArgs, reply *RequestTaskReply) bool {
	ok := call("Master.RequestTask", args, reply)
	return ok
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
