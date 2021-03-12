package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"


type Coordinator struct {
	// Your definitions here.
	MapTasks map[string]int   //// 0 idle, 1 in-progress, 2 completed.
	MapTaskNum  int
	ReduceTasks map[int]int
	ReduceTaskNum int
	MapTaskResult []int
	CurrentMapTaskNum int
	CurrentReduceTaskNum int
	CompletedMapTaskNum int
	Completed  bool
	mu   sync.Mutex
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

func (c *Coordinator) MapReduceHandler(args *MapArgs, reply *MapReply ) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.ReduceTaskNum = c.ReduceTaskNum
	reply.MapTaskNum = c.MapTaskNum
	if c.CurrentMapTaskNum < c.MapTaskNum {
		reply.TaskID = c.CurrentMapTaskNum
		c.CurrentMapTaskNum ++
		reply.WorkerType = 0
		for filename, status := range c.MapTasks {
			if status == 0 {
				c.MapTasks[filename] = 1
				fmt.Printf("serve %s filename status:%d", filename, c.MapTasks[filename])
				reply.Filename = filename
				return nil
			}
		}
	} else if c.CurrentReduceTaskNum < c.ReduceTaskNum {
		// call reduce worker.
		reply.TaskID = c.CurrentReduceTaskNum
		reply.WorkerType = 1
		c.CurrentReduceTaskNum ++
	} else {
		c.Completed = true
		reply.TaskID = -1 // -1 indicate does not allocate any worker to it.
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
	defer c.mu.Unlock()
	ret := c.Completed

	// Your code here.
	

	return ret
}

// reduce worker use this to poll if reduce worker can start
func (c *Coordinator) Poll(args *PollArgs, reply *PollReply ) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.CompletedMapTaskNum == c.MapTaskNum {
		reply.Finished = true
	} else {
		reply.Finished = false
	}
	return nil
}

// Map worker use this method to tell coordinator how many map task has finshied
func (c *Coordinator) Indicate(args *IndicateArgs, reply *IndicateReply ) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.CompletedMapTaskNum ++
	c.MapTasks[args.Filename] = 2
	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	
	// Your code here.
	c.MapTasks = make(map[string]int)
	// make every map task status as un assigned.
	for _, filename := range files {
		c.MapTasks[filename] = 0
	}
	for filename, status := range c.MapTasks {
		fmt.Printf("%s , %d \n", filename, status)
	}
	c.MapTaskNum = len(c.MapTasks)
	c.ReduceTaskNum = nReduce
	//make every reduce tasks as unassigned.
	for i := 0; i < nReduce; i ++ {
		c.ReduceTasks[i] = 0
	}
	c.Completed = false
	c.CurrentMapTaskNum = 0
	c.CurrentReduceTaskNum = 0
	c.server()
	return &c
}
