package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "encoding/json"
import "io/ioutil"
//
// Map functions return a slice of KeyValue.
//
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

func doMap(reply *MapReduceReply ,mapf func(string, string) []KeyValue ) {
	task := reply.Task
	filename := task.MapFile 

	intermediate := []KeyValue{}

	file, err := os.Open(filename)

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)

	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	file.Close()

	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...) // append one slice to another by three dots.

	files := []*os.File{} // declare a list of file pointers

	for i := 0; i < reply.NReduce ; i ++ {
		oname := fmt.Sprintf("mr-%d-%d", task.Index, i)
		ofile, _ := os.Create(oname)
		files = append(files, ofile)
		task.ReduceFiles = append(task.ReduceFiles, oname) // append the filenames  into reduce file list
	}

	for _, kv := range intermediate {
		filenum := ihash(kv.Key) % reply.NReduce
		enc := json.NewEncoder (files[filenum])
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Println("failed to save the intermediate file encode")
			fmt.Println(err)
			return
		}
	}

	argsFinish := MapReduceArgs{MessageType: FinishTask}


	res := call("Master.WorkerCallHandler", &argsFinish, &reply)

	if res == false {
		return 
	}
}

func doReduce(reply *MapReduceReply, reducef func(string, []string) string) {

}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for true {
		args := MapReduceArgs{MessageType: RequestTask}
		reply := MapReduceReply{}

		res := call("Master.WorkerCallHandler", &args, &reply)
		if !res {
			break;
		}

		switch reply.Task.Type {
			case "Map":
				doMap(&reply, mapf)
			case "Reduce":
				doReduce(&reply, reducef)
			case "Wait":
				time.Sleep(1 * time.Second)
		}
	}

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
