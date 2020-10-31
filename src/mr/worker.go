package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const intermediateFname = "mr-%d-%d"
const outputFname = "mr-out-%d"

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
		reply := RequestTaskReply{}

		// 2. Send RPC
		ok := sendRequestTaskRPC(&reply)
		if !ok {
			DPrintf(fail("Connection Error: sendRequestTask | worker -> master"))
		}

		// 3. Process RPC Reply
		success := processRequestTaskReply(&reply, mapf, reducef)
		if !success {
			break
		}

		select {
		case <-time.After(requestTaskTTL * time.Millisecond):
		}
	}
}

func processRequestTaskReply(reply *RequestTaskReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) bool {
	switch reply.TaskType {
	case mapTask:
		processMapTask(reply, mapf)
	case redTask:
		processRedTask(reply, reducef)
	case nop:
		return processNOPTask(reply)
	}
	return true
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
	type writer struct {
		encoder *json.Encoder
		file    *os.File
	}
	writers := make([]writer, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		intermediateFileName := fmt.Sprintf(intermediateFname, task.ID, i)
		file, err := os.Create(intermediateFileName)
		if err != nil {
			DPrintf(fail("Can not create file: %v {%v}"), intermediateFileName, err)
			return
		}
		writers[i] = writer{json.NewEncoder(file), file}
	}

	// 4.2 Write KeyValues to specific file via encoder
	for _, kv := range kva {
		hash := ihash(kv.Key) % reply.NReduce
		err := writers[hash].encoder.Encode(&kv)
		if err != nil {
			DPrintf(fail("Can not write \"%+v\""), kv)
		}
	}

	// 4.3 Close all files
	for _, w := range writers {
		err = w.file.Close()
		if err != nil {
			DPrintf(fail("Can not close file %v"), w.file.Name())
		}
	}

	// 5. Send Back Task Done RPC
	newArgs := &DoneTaskArgs{TaskID: task.ID, TaskType: mapTask}
	ok := sendDoneTaskRPC(newArgs)
	if !ok {
		DPrintf(fail("Connection Error: sendDoneTaskRPC worker -> master"))
	}
}

func processRedTask(reply *RequestTaskReply, reducef func(string, []string) string) {
	task := reply.Red
	DPrintf(newTask("Worker Received Reduce Task: %+v"), task)

	// 1. Open files
	type reader struct {
		decoder *json.Decoder
		file    *os.File
	}
	readers := make([]reader, reply.NMap)
	for i := 0; i < reply.NMap; i++ {
		intermediateFileName := fmt.Sprintf(intermediateFname, i, task.ID)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			return
		}
		readers[i] = reader{json.NewDecoder(file), file}
	}

	// 2. Read KeyValues from all files
	kvm := make(map[string][]string)
	for _, r := range readers {
		// 2.1 Read KeyValues from one file
		for {
			var kv KeyValue
			if err := r.decoder.Decode(&kv); err != nil {
				break
			}
			kvm[kv.Key] = append(kvm[kv.Key], kv.Value)
		}
		// 2.2 Close file
		err := r.file.Close()
		if err != nil {
			DPrintf(fail("Can not close file %v"), r.file.Name())
		}
	}

	// 3. Sort Map Keys
	var keys []string
	for key := range kvm {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// 4. Write into file
	outFName := fmt.Sprintf(outputFname, task.ID)
	outFile, _ := os.Create(outFName)
	for _, key := range keys {
		res := reducef(key, kvm[key])
		fmt.Fprintf(outFile, "%v %v\n", key, res)
	}

	// 5. Send Back Task Done RPC
	newArgs := &DoneTaskArgs{TaskID: task.ID, TaskType: redTask}
	ok := sendDoneTaskRPC(newArgs)
	if !ok {
		DPrintf(fail("Connection Error: sendDoneTaskRPC worker -> master"))
	}
}

func processNOPTask(reply *RequestTaskReply) bool {
	DPrintf(newTask("Worker Received NOP Task | No More Tasks"))
	return false
}

func sendDoneTaskRPC(args *DoneTaskArgs) bool {
	ok := call("Master.TaskDone", args, new(struct{}))
	return ok
}

func sendRequestTaskRPC(reply *RequestTaskReply) bool {
	ok := call("Master.RequestTask", new(struct{}), reply)
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
