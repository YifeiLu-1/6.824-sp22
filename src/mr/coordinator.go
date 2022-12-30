package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mode                       string // map/reduce
	nReduce                    int
	filesToMap                 []string
	mapTaskFinishStatus        map[string]bool
	mapTaskFileNameAndNumPairs map[string]int
	mapTaskNumTarget           int
	mapTaskFinishedNum         int
	reduceTasks                []int
	reduceTaskFinishStatus     map[int]bool
	reduceTaskNumTarget        int
	reduceTaskFinishedNum      int
	mu                         sync.Mutex
	done                       bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) DispatchTask(args *WorkerAskTaskArgs, reply *WorkerAskTaskReply) error {
	c.mu.Lock()
	mode := c.mode
	c.mu.Unlock()
	if mode == "map" {
		fileName := c.getNextMapTaskFileName()
		if fileName == "" {
			reply.IsSleep = true
		} else {
			// ask for result
			go c.checkMapTaskFinished(fileName)
			reply.MapFileName = fileName
			reply.TaskType = "map"
			reply.NReduce = c.nReduce
			reply.MapTaskNum = c.mapTaskFileNameAndNumPairs[fileName]
			reply.IsSleep = false
		}

	} else if mode == "reduce" {
		//fmt.Printf("into reduce phase\n")
		reduceTaskNum := c.getNextReduceTaskNum()
		if reduceTaskNum == -1 {
			reply.IsSleep = true
		} else {
			go c.checkReduceTaskFinished(reduceTaskNum)
			reply.TaskType = "reduce"
			reply.ReduceTaskNum = reduceTaskNum
			reply.TotalMapTaskNum = c.mapTaskNumTarget
		}
	}
	return nil
}

func (c *Coordinator) getNextMapTaskFileName() string {
	c.mu.Lock()
	var nextMapFileName string
	if len(c.filesToMap) == 0 {
		nextMapFileName = ""
	} else {
		nextMapFileName = c.filesToMap[0]
		c.filesToMap = c.filesToMap[1:]
	}
	c.mu.Unlock()
	return nextMapFileName
}

func (c *Coordinator) checkMapTaskFinished(fileName string) {
	time.Sleep(10 * time.Second)

	c.mu.Lock()
	if !c.mapTaskFinishStatus[fileName] {
		//fmt.Printf("checking %v map task status failed\n", fileName)
		c.filesToMap = append(c.filesToMap, fileName)
	} else {
		//fmt.Printf("checking %v map task status success\n", fileName)
	}
	c.mu.Unlock()
}

func (c *Coordinator) getNextReduceTaskNum() int {
	c.mu.Lock()
	var nextReduceTaskNum int
	if len(c.reduceTasks) == 0 {
		nextReduceTaskNum = -1
	} else {
		nextReduceTaskNum = c.reduceTasks[0]
		c.reduceTasks = c.reduceTasks[1:]
	}
	c.mu.Unlock()
	return nextReduceTaskNum
}

func (c *Coordinator) checkReduceTaskFinished(reduceTaskNum int) {
	time.Sleep(10 * time.Second)
	//fmt.Printf("checking %v reduce task status\n", reduceTaskNum)
	c.mu.Lock()
	if !c.reduceTaskFinishStatus[reduceTaskNum] {
		fmt.Printf("checking %v reduce task status failed, reassign\n", reduceTaskNum)
		c.reduceTasks = append(c.reduceTasks, reduceTaskNum)
	} else {

	}
	c.mu.Unlock()
}

func (c *Coordinator) FinishTask(args *WorkerFinishTaskArgs, reply *WorkerFinishTaskReply) error {
	if args.TaskType == "map" {
		fileName := args.FileName
		c.mu.Lock()
		if c.mapTaskFinishStatus[fileName] {
			return nil
		}
		c.mapTaskFinishStatus[fileName] = true
		c.mapTaskFinishedNum++
		if c.mapTaskFinishedNum == c.mapTaskNumTarget {
			c.mode = "reduce"
		}
		c.mu.Unlock()
	} else if args.TaskType == "reduce" {
		reduceTaskNum := args.ReduceTaskNum
		c.mu.Lock()
		if c.reduceTaskFinishStatus[reduceTaskNum] {
			return nil
		}
		c.reduceTaskFinishStatus[reduceTaskNum] = true
		c.reduceTaskFinishedNum++
		if c.reduceTaskFinishedNum == c.reduceTaskNumTarget {
			c.done = true
		}
		c.mu.Unlock()
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	ret := c.done
	c.mu.Unlock()
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mode:                       "map",
		nReduce:                    nReduce,
		mapTaskFileNameAndNumPairs: make(map[string]int),
		mapTaskNumTarget:           len(files),
		mapTaskFinishedNum:         0,
		mapTaskFinishStatus:        make(map[string]bool),
		reduceTasks:                make([]int, nReduce),
		reduceTaskFinishStatus:     make(map[int]bool),
		reduceTaskNumTarget:        nReduce,
		reduceTaskFinishedNum:      0,
		done:                       false,
	}

	c.mu.Lock()
	c.filesToMap = files
	for index, file := range files {
		c.mapTaskFileNameAndNumPairs[file] = index
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = i
	}
	c.mu.Unlock()

	// Your code here.

	c.server()
	return &c
}
