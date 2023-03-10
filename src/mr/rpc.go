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

// Add your RPC definitions here.

type TaskType int

const (
	// Request
	MAP_CONFIRM TaskType = iota+1
	REDUCE_CONFIRM
	PING
	WAITTING

	// Response
	TASK_CONFIRM
	TASK_FAIL
	PING_CONFIRM
	PING_TERMINATE
	MAP_TASK
	REDUCE_TASK
	EXIT
)

// CLIENT: MAP REDUCE PING WAIT
type Request struct {
	TaskType TaskType
	TaskId   int
	File     []string
}

// SERVER: MAP REDUCE WAIT CONFIRM
type Response struct {
	TaskType  TaskType
	TaskId    int
	File      []string
	NReduce   int
	ReduceIdx int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
