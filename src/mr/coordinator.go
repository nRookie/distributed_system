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
import "fmt"

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

			for i, task := range c.mapTasks {
				// fmt.Printf("timeout is %f \n", time.Since(task.Timestamp).Seconds())
				if (task.Status == 0 || (task.Status == 1 && (time.Since(task.Timestamp).Seconds()  > 10 ))) {
					c.mapTasks[i].Status = 1
					c.mapTasks[i].Type = "Map"
					c.mapTasks[i].Timestamp = time.Now()
					reply.Task = c.mapTasks[i]
					reply.NReduce = c.nReduce
					// fmt.Printf("%d %d", task.Status, c.mapTasks[task.Index].Status )
					return nil
				}
			}
			// map process is not finished we have to wait
			var task MapReduceTask
			task.Type = "Wait"
			reply.Task = task
			return nil
		} else if c.reduceFinished == false {
			for i, task := range c.reduceTasks {
				if task.Status == 0 || (task.Status == 1 && time.Since(task.Timestamp).Seconds()  > 10 ) {
					c.reduceTasks[i].Status = 1
					c.reduceTasks[i].Type ="Reduce"
					c.reduceTasks[i].Timestamp = time.Now()
					reply.Task = c.reduceTasks[i]
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
		fmt.Printf("task type is :%s\n", task.Type)
		if (task.Status == 1 && (time.Since(task.Timestamp).Seconds() < 10) ) {
			fmt.Printf("task type is :%s %s\n", task.Type, "Map")
			if task.Type == "Map" {
				c.mapTasks[task.Index].Status = 2
				// TODO: need to use temporary file name
				fmt.Printf("map task finished %d\n", task.Index );
				for _ , filename := range task.ReduceFiles {
					s := strings.Split(filename, "-")
					index, err := strconv.Atoi(s[2])
					if err != nil {
						log.Fatalf("failed to convert %s to integer ", s[2])
					}
					newName := fmt.Sprintf("%s-%s-%s", s[0], s[1], s[2]);
					os.Rename(filename, newName)
					c.reduceTasks[index].ReduceFiles = append(c.reduceTasks[index].ReduceFiles, newName)
				}
			} else if task.Type == "Reduce" {
				c.reduceTasks[task.Index].Status = 2
			}
		}

		c.mapFinished = true
		for _, task := range c.mapTasks {
			if (task.Status != 2) {
				c.mapFinished = false 
				fmt.Printf("FINISHEDTASK: %d unfinished %d\n",task.Index, task.Status  )
			}
		}
		c.reduceFinished = true
		for _, task := range c.reduceTasks {
			if (task.Status != 2) {
				c.reduceFinished = false
				fmt.Printf("FINISHEDTASK: reduce %d unfinished  %d\n",task.Index, task.Status   )
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
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
		fmt.Printf("initializing maptask: %d\n", task.Index)
		c.mapTasks = append(c.mapTasks, task)
	}
	c.nReduce = nReduce

	for i := 0; i < nReduce; i++ {
		var task MapReduceTask
		task.Index = i
		task.Type = "Reduce"
		task.Status = 0
		fmt.Printf("initializing reducetask: %d\n", task.Index)
		c.reduceTasks = append(c.reduceTasks, task)
	}
	c.server()
	return &c
}
