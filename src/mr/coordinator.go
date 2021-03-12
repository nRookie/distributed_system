package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

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

	// Your code here.
	taskID := 0
	for _, filename := range files {
		var task MapReduceTask
		c.files = append(c.files, filename)
		task.MapFile = filename
		task.Index = taskID
		task.Type = "Map"
		task.Status = 0
		c.mapTasks = append(c.mapTasks, task)
		taskID ++
	}
	c.nReduce = nReduce

	for i := 0; i < nReduce; i++ {
		var task MapReduceTask
		task.Index = taskID
		task.Type = "Reduce"
		task.Status = 0
		taskID ++ 
	}
	c.server()
	return &c
}
