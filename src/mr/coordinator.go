package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

const (
	IDLE = iota
	IN_PROG
	COMPLETE
)
const (
	FREE = iota
	BUSY
)

const (
	MAP = iota
	REDUCE
	WAIT
	NOTHING
)

type Coordinator struct {
	// Your definitions here.
	mu            sync.Mutex
	mapTaskStatus map[string]int
	redTaskStatus map[int]int
	machinesID    map[int]int
	mapCnt        int
	redCnt        int
	// mapDone       bool
	// redDone       bool
}

// Your code here -- RPC handlers for the worker to call.

// func used to dispatch unstarted work to the FREE workers
func (c *Coordinator) Dispatch(args *DispatchArgs, reply *DispatchReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.machinesID[args.WorkID] = BUSY
	// fmt.Println(len(c.mapTaskStatus))

	if c.mapCnt != len(c.mapTaskStatus)*COMPLETE {
		taskCnt := 0
		for k, v := range c.mapTaskStatus {
			taskCnt++
			// fmt.Println(k, v)
			if v == IDLE {
				reply.FileName = k
				reply.TaskType = MAP
				fmt.Println(reply.FileName + "\n")
				c.mapTaskStatus[reply.FileName] = IN_PROG
				c.mapCnt += IN_PROG
				break
			}
			if taskCnt == len(c.mapTaskStatus)-1 && v != IDLE {
				reply.TaskType = WAIT
			}
		}

	} else if c.redCnt != len(c.redTaskStatus)*COMPLETE {
		taskCnt := 0
		for k, v := range c.redTaskStatus {
			taskCnt++
			if v == IDLE {
				reply.FileName = strconv.Itoa(k)
				reply.TaskType = REDUCE
				fmt.Println(reply.FileName + "\n")
				c.redTaskStatus[k] = IN_PROG
				c.redCnt += IN_PROG
				break
			}
			if taskCnt == len(c.redTaskStatus)-1 && v != IDLE {
				reply.TaskType = WAIT
			}
		}
	} else {
		reply.TaskType = NOTHING
		fmt.Println("All Job Has Done")
	}

	return nil
}

// func used to recv info from workers
func (c *Coordinator) Recv(args *RecvArgs, reply *RecvReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.machinesID[args.WorkID] = FREE
	if args.TaskType == MAP {
		c.mapTaskStatus[args.FileName] = COMPLETE
		c.mapCnt++
	} else if args.TaskType == REDUCE {
		redNum, _ := strconv.Atoi(args.FileName)
		c.redTaskStatus[redNum] = COMPLETE
		c.redCnt++
	}
	reply.Status = "Done"
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapCnt == len(c.mapTaskStatus)*COMPLETE && c.redCnt == len(c.redTaskStatus)*COMPLETE {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	numFiles := len(files)
	// allocate memory to the c.taskStatues and c.machinesID
	c.mapTaskStatus = make(map[string]int)
	c.redTaskStatus = make(map[int]int)
	c.machinesID = make(map[int]int)
	for i := 0; i < numFiles; i++ {
		c.mapTaskStatus[files[i]] = IDLE
	}
	for i := 0; i < 3; i++ {
		c.redTaskStatus[i] = IDLE
	}
	for k, v := range c.mapTaskStatus {
		fmt.Println(k, " ", v)
	}
	// Your code here.

	c.server()
	return &c
}
