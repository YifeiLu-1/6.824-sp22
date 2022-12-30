package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// Your worker implementation here.
		// get a task
		reply := CallForTask()
		if reply.IsSleep {
			time.Sleep(2 * time.Second)
			//fmt.Printf("worker sleep 2s\n")
			continue
		}
		if reply.TaskType == "map" {
			filename := reply.MapFileName
			//fmt.Printf("doing map on %v\n", filename)
			doMap(mapf, filename, reply.NReduce, reply.MapTaskNum)
			FinishMapTask(filename)
		} else if reply.TaskType == "reduce" {
			//fmt.Printf("doing reduce on task %v \n", reply.ReduceTaskNum)
			reduceTaskNum := reply.ReduceTaskNum
			doReduce(reducef, reduceTaskNum, reply.TotalMapTaskNum)
			FinishReduceTask(reduceTaskNum)
			//fmt.Printf("doing reduce on task %v Done\n", reply.ReduceTaskNum)
		}
	}

}

func CallForTask() *WorkerAskTaskReply {
	args := &WorkerAskTaskArgs{}
	reply := &WorkerAskTaskReply{}

	ok := call("Coordinator.DispatchTask", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call for task failed!\n")
		return nil
	}
}

func FinishMapTask(mapTaskFilename string) *WorkerFinishTaskReply {
	args := &WorkerFinishTaskArgs{
		TaskType: "map",
		FileName: mapTaskFilename,
	}
	reply := &WorkerFinishTaskReply{}

	ok := call("Coordinator.FinishTask", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call for finish map task failed!\n")
		return nil
	}
}

func FinishReduceTask(reduceTaskNum int) *WorkerFinishTaskReply {
	args := &WorkerFinishTaskArgs{
		TaskType:      "reduce",
		ReduceTaskNum: reduceTaskNum,
	}
	reply := &WorkerFinishTaskReply{}

	ok := call("Coordinator.FinishTask", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call for finish reduce task failed!\n")
		return nil
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
