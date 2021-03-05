package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "encoding/json"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	
	args := MapArgs{}
	reply := MapReply{}

	call("Coordinator.MapHandler", &args, &reply)
	for reply.TaskID == -1 {
		// idle
		time.Sleep(1000)
		fmt.Printf("i am currently idle")
	}

	if reply.WorkerType == 0 { // Map worker
		filename := reply.Filename

		intermediate := []KeyValue{}

		fmt.Printf("worker: %d reading file:%s", reply.TaskID, filename)

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
		intermediate = append(intermediate, kva...)  // append one slice to another by three dots.

		for _, kv := range intermediate {
			oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, ihash(kv.Key) % reply.ReduceTaskNum) // 10 change to total reduce task number

			if _, err := os.Stat(oname); err == nil {

				if err != nil {
					fmt.Println("failed to save the intermediate file open")
					fmt.Println(err)
					return 
				}

				ofile, _ := os.OpenFile(oname,os.O_APPEND | os.O_WRONLY, os.ModeAppend)
				enc := json.NewEncoder(ofile)
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Println("failed to save the intermediate file encode")
					fmt.Println(err)
					return 
				}

			} else {
				ofile, _ := os.Create(oname)
				enc := json.NewEncoder(ofile)
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Println("failed to save the intermediate file create")
					fmt.Println(err)
					return 
				}
			}
		}
	} else { // Reduce worker

			// read all the content from different mr file into intermediate
			intermediate := []KeyValue{}
			for j:= 0; j < reply.MapTaskNum; j++ {
				oname := fmt.Sprintf("mr-%d-%d", j, reply.TaskID);
				ofile, _ := os.OpenFile(oname,os.O_APPEND | os.O_WRONLY, os.ModeAppend)
				dec := json.NewDecoder(ofile)
				for {
				  var kv KeyValue
				  if err := dec.Decode(&kv); err != nil {
					break
				  }
				  intermediate = append(intermediate, kv)
				}
				ofile.Close()
			}

			// sort it by key
			sort.Sort(ByKey(intermediate))


			res_name := fmt.Sprintf("mr-out-%d", reply.TaskID);
			res_file, _ := os.Create(res_name)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
		
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(res_file, "%v %v\n", intermediate[i].Key, output)
		
				i = j
			}
			res_file.Close()
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
