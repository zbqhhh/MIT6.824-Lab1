package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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
	mu               sync.Mutex
	mapTaskStatus    map[string]int
	mapTaskBeginTime map[string]int64
	redTaskStatus    map[int]int
	redTaskBeginTime map[int]int64
	machinesID       map[int]int
	mapCnt           int
	redCnt           int
	NReduce          int
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
				reply.NReduce = c.NReduce

				fmt.Println(reply.FileName + "\n")
				c.mapTaskStatus[reply.FileName] = IN_PROG
				c.mapTaskBeginTime[reply.FileName] = time.Now().Unix()
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
				reply.NReduce = c.NReduce

				fmt.Println(reply.FileName + "\n")
				c.redTaskStatus[k] = IN_PROG
				c.redTaskBeginTime[k] = time.Now().Unix()
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

// func used to delete repeat file
func (c *Coordinator) deleteRepeat(workerID int) {
	pwd, _ := os.Getwd()
	//获取文件或目录相关信息
	fileInfoList, err := ioutil.ReadDir(pwd)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(fileInfoList))
	patternStr := "map_out_" + strconv.Itoa(workerID)
	for _, v := range fileInfoList {
		if strings.Contains(v.Name(), patternStr) {
			os.Remove(v.Name())
		}
	}
}

// func used to recv info from workers
func (c *Coordinator) Recv(args *RecvArgs, reply *RecvReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.machinesID[args.WorkID] = FREE
	if args.TaskType == MAP {
		if c.mapTaskStatus[args.FileName] == COMPLETE {
			c.deleteRepeat(args.WorkID)
		} else if c.mapTaskStatus[args.FileName] == IN_PROG {
			c.mapTaskStatus[args.FileName] = COMPLETE
			c.mapCnt++
		} else {
			c.mapTaskStatus[args.FileName] = COMPLETE
			c.mapCnt += COMPLETE
		}
	} else if args.TaskType == REDUCE {
		redNum, _ := strconv.Atoi(args.FileName)
		if c.redTaskStatus[redNum] == COMPLETE {

		} else if c.redTaskStatus[redNum] == IN_PROG {
			c.redTaskStatus[redNum] = COMPLETE
			c.redCnt++
		} else {
			c.redTaskStatus[redNum] = COMPLETE
			c.redCnt += COMPLETE
		}
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
// periodically check in-progessing task has been time out or not
//

const threshold = 10

func (c *Coordinator) checkTask() {
	for {
		time.Sleep(2000 * time.Millisecond)
		c.mu.Lock()
		now := time.Now().Unix()
		if c.mapCnt != len(c.mapTaskStatus)*COMPLETE {
			for k, v := range c.mapTaskStatus {
				if v == IN_PROG && now-c.mapTaskBeginTime[k] > threshold {
					c.mapTaskStatus[k] = IDLE
					c.mapCnt--
				}
			}
		}
		if c.redCnt != len(c.redTaskStatus)*COMPLETE {
			for k, v := range c.redTaskStatus {
				if v == IN_PROG && now-c.redTaskBeginTime[k] > threshold {
					c.redTaskStatus[k] = IDLE
					c.redCnt--
				}
			}
		}
		c.mu.Unlock()
	}
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
		c.removeIntermediateFiles()
		ret = true
	}
	return ret
}

func (c *Coordinator) removeIntermediateFiles() {
	pwd, _ := os.Getwd()
	//获取文件或目录相关信息
	fileInfoList, err := ioutil.ReadDir(pwd)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(fileInfoList))
	for i := range fileInfoList {
		fileName := fileInfoList[i].Name()
		if strings.Contains(fileName, "map_out") {
			os.Remove(fileName)
		}
	}
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
	c.mapTaskBeginTime = make(map[string]int64)
	c.redTaskBeginTime = make(map[int]int64)
	c.NReduce = nReduce
	for i := 0; i < numFiles; i++ {
		c.mapTaskStatus[files[i]] = IDLE
	}
	for i := 0; i < c.NReduce; i++ {
		c.redTaskStatus[i] = IDLE
	}
	for k, v := range c.mapTaskStatus {
		fmt.Println(k, " ", v)
	}
	// Your code here.

	c.server()

	// start one thread to periodically find out which task has crashed already
	go c.checkTask()
	return &c
}
