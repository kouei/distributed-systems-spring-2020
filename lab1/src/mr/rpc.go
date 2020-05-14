package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// ExampleArgs struct for RPC Call
type ExampleArgs struct {
	X int
}

// ExampleReply struct for RPC Call
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// GetWorkerIDArgs struct for RPC Call
type GetWorkerIDArgs struct {
}

// GetWorkerIDReply struct for RPC Call
type GetWorkerIDReply struct {
	WorkerID int
}

// GetTaskArgs struct for RPC Call
type GetTaskArgs struct {
	WorkerID int
}

// GetTaskReply struct for RPC Call
type GetTaskReply struct {
	TaskID    int
	TaskType  int
	Filenames []string
	MapID     int
	ReduceID  int
	NReduce   int
}

// NotifyTaskFinishedArgs struct for RPC Call
type NotifyTaskFinishedArgs struct {
	TaskID    int
	Filenames []string
}

// NotifyTaskFinishedReply struct for RPC Call
type NotifyTaskFinishedReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
