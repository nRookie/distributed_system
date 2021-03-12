package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "strings"
import "strconv"

type MapReduceTask struct {
	Type string // "Map", "Reduce", "Wait"
	Status int // "Unassigned", "Assigned", "Finished"
	Index  int // Index of the task
	Timestamp  time.Time   // Start time
	MapFile    string      // Files for map task
	ReduceFiles      [] string  // List of files for reduce task.
}

type Coordinator struct {
	// Your definitions here.
	files []string
	nReduce int
	
	mapTasks []MapReduceTask
	reduceTasks []MapReduceTask

	mapFinished bool
	reduceFinished bool

	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) WorkerCallHandler(args *MapReduceArgs, reply *MapReduceReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if (args.MessageType == RequestTask) {

		if (c.mapFinished == false) {

			for _, task := range c.mapTasks {
				if task.Status == 0 || (task.Status == 1 && time.Since(task.Timestamp).Seconds()  > 10) {
					task.Status = 1
					task.Type ="Map"
					task.Timestamp = time.Now()
					reply.Task = task
					reply.NReduce = c.nReduce
					return nil
				}
			}
			// map process is not finished we have to wait
			var task MapReduceTask
			task.Type = "Wait"
			reply.Task = task
			return nil
		} else if c.reduceFinished == false {
			for _, task := range c.reduceTasks {
				if task.Status == 0 || (task.Status == 1 && time.Since(task.Timestamp).Seconds()  > 10) {
					task.Status = 1
					task.Type ="Reduce"
					task.Timestamp = time.Now()
					reply.Task = task
					return nil
				}
			}
			var task MapReduceTask
			task.Type = "Wait"
			reply.Task = task
			return nil
		}
	} else { // Finished Task
		task := args.Task
		// check if the worker is the last worker who we assigned the task.
		if (task.Status == 1 && time.Since(task.Timestamp).Seconds() < 10) {
			task.Status = 2
			if task.Type == "Map" {
				// TODO: need to use temporary file name
				for _ , filename := range task.ReduceFiles {
					s := strings.Split(filename, "-")
					index, err := strconv.Atoi(s[2])
					if err != nil {
						log.Fatalf("failed to convert %s to integer ", s[2])
					}
					c.reduceTasks[index].ReduceFiles = append(c.reduceTasks[index].ReduceFiles, filename)
				}
			}
		}

		c.mapFinished = true
		for _, task := range c.mapTasks {
			if (task.Status != 2) {
				c.mapFinished = false 
			}
		}
		c.reduceFinished = true
		for _, task := range c.reduceTasks {
			if (task.Status != 2) {
				c.reduceFinished = false
			} 
		}
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
	ret := false

	// Your code here.
	if (c.mapFinished && c.reduceFinished) {
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

	// Your code here.

	for i, filename := range files {
		var task MapReduceTask
		c.files = append(c.files, filename)
		task.MapFile = filename
		task.Index = i
		task.Type = "Map"
		task.Status = 0
		c.mapTasks = append(c.mapTasks, task)
	}
	c.nReduce = nReduce

	for i := 0; i < nReduce; i++ {
		var task MapReduceTask
		task.Index = i
		task.Type = "Reduce"
		task.Status = 0
		c.reduceTasks = append(c.reduceTasks, task)

	}
	c.server()
	return &c
}
