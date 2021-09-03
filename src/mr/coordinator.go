package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	IDLE = iota
	COMPLETE
	IN_PROG
)
const (
	FREE = iota
	BUSY
)

type Coordinator struct {
	// Your definitions here.
	mu         sync.Mutex
	taskStatus map[string]int
	machinesID map[int]int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Call(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.machinesID[args.workID] = BUSY
	for k, v := range c.taskStatus {
		if v == IDLE {
			reply.fileName = k
		}
	}
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
	c.taskStatus = make(map[string]int)
	c.machinesID = make(map[int]int)
	for i := 0; i < numFiles; i++ {
		c.taskStatus[files[i]] = IDLE
	}
	// Your code here.

	c.server()
	return &c
}
