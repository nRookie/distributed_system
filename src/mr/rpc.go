package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	RequestTask  = iota // request task from the master
	FinishTask			// commit the finished task to the master for updaing task list.
)

type MapReduceArgs struct {
	MessageType int
	Task  MapReduceTask
}

type MapReduceReply struct {
	Task MapReduceTask
	NReduce int
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
