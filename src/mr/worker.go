package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % reply.NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// const reply.NReduce = 10
// const TEST = 1000

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	fmt.Println("hihihih")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	args := DispatchArgs{}

	// Your worker implementation here.
	for {
		args.WorkID = r.Intn(1000)
		reply := DispatchReply{}
		call("Coordinator.Dispatch", &args, &reply)
		fmt.Println(reply.FileName)
		if reply.TaskType == MAP {
			fmt.Println("MAP JOB START")
			mapWorker(args, reply, mapf)
			fmt.Println("MAP JOB FINISHED")
			// time.Sleep(10 * time.Millisecond)
		} else if reply.TaskType == REDUCE {
			fmt.Println("REDUCE JOB START")
			reduceWorker(args, reply, reducef)
			fmt.Println("REDUCE JOB FINISHED")
			// time.Sleep(1000 * time.Millisecond)
		} else if reply.TaskType == WAIT {
			fmt.Println("JUST WAIT")
			time.Sleep(5000 * time.Millisecond)
		} else {
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func mapWorker(args DispatchArgs, reply DispatchReply,
	mapf func(string, string) []KeyValue) {
	content := getFileContent(reply.FileName)
	kva := mapf(reply.FileName, string(content))

	ofile := make([]*os.File, reply.NReduce)

	for i := 0; i < reply.NReduce; i++ {
		outputFile := "map_out_" + strconv.Itoa(args.WorkID) + "_" + strconv.Itoa(i)
		ofile[i], _ = os.Create(outputFile)
	}

	for _, v := range kva {
		redNum := ihash(v.Key) % reply.NReduce
		fmt.Fprintf(ofile[redNum], "%v %v\n", v.Key, v.Value)
	}

	recvArgs := RecvArgs{
		reply.TaskType,
		reply.FileName,
		args.WorkID,
	}
	recvReply := RecvReply{}
	call("Coordinator.Recv", &recvArgs, &recvReply)
	fmt.Println(recvReply.Status)
}

func reduceWorker(args DispatchArgs, reply DispatchReply,
	reducef func(string, []string) string) {
	redNum, _ := strconv.Atoi(reply.FileName)
	fileList := getIntermediateFileList(redNum)

	intermediate := []KeyValue{}
	for _, fileName := range fileList {
		content := getFileContent(fileName)
		kva := getKeyValueList(string(content))
		intermediate = append(intermediate, kva...)
	}

	reduceOutput(intermediate, redNum, reducef)
	recvArgs := RecvArgs{
		reply.TaskType,
		reply.FileName,
		args.WorkID,
	}
	recvReply := RecvReply{}
	call("Coordinator.Recv", &recvArgs, &recvReply)
	fmt.Println(recvReply.Status)
}

func getFileContent(fileName string) []byte {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	return content
}

func reduceOutput(intermediate []KeyValue, redNum int,
	reducef func(string, []string) string) {
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(redNum)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		// detect consecutive same keys
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func getKeyValueList(content string) []KeyValue {
	res := []KeyValue{}
	pairString := strings.Split(content, "\n")
	for _, v := range pairString {
		if len(v) <= 1 {
			continue
		}
		value := strings.Split(v, " ")
		tmpKVA := KeyValue{value[0], value[1]}
		res = append(res, tmpKVA)
		// res[k].Key = value[0]
		// res[k].Value = value[1]
	}
	return res
}

func getIntermediateFileList(redNum int) []string {
	res := []string{}
	cnt := 0
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
			fileRedNum, _ := strconv.Atoi(strings.Split(fileName, "_")[3])
			if fileRedNum == redNum {
				res = append(res, fileName)
				cnt++
			}
		}
	}
	return res
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
