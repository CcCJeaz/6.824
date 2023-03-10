package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MapTask struct {
	file     string
	taskId   int
	pingTime time.Time
}

type ReduceTask struct {
	fileId   int //中间文件
	files    []string
	taskId   int
	pingTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mtx        sync.Mutex
	nextTaskId int
	timeout    time.Duration
	nReduce    int

	mapTasks      map[int]*list.Element
	mapDoingTasks *list.List
	mapFreeTasks  *list.List

	reduceTaskList   []*ReduceTask
	reduceTasks      map[int]*list.Element
	reduceDoingTasks *list.List
	reduceFreeTasks  *list.List
}

func (c *Coordinator) GetTask(args *Request, reply *Response) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	switch args.TaskType {
	case MAP_CONFIRM:
		c.confirmMapTask(args, reply)
	case REDUCE_CONFIRM:
		c.confirmReduceTask(args, reply)
	case PING:
		c.pingConfrm(args, reply)
	case WAITTING:
		if c.getMapTask(reply) {
			return nil
		}
		c.getReduceTask(reply)
	}
	return nil
}

func (c *Coordinator) pingConfrm(args *Request, reply *Response) {
	if mapTask:=c.mapTasks[args.TaskId]; mapTask != nil {
		mapTask.Value.(*MapTask).pingTime = time.Now()
		c.mapDoingTasks.Remove(mapTask)
		c.mapTasks[args.TaskId] = c.mapDoingTasks.PushBack(mapTask.Value)
	} else if reduceTask:=c.reduceTasks[args.TaskId]; reduceTask != nil {
		reduceTask.Value.(*ReduceTask).pingTime = time.Now()
		c.reduceDoingTasks.Remove(reduceTask)
		c.reduceTasks[args.TaskId] = c.reduceDoingTasks.PushBack(reduceTask.Value)
	}
	reply.TaskType = PING_CONFIRM
}

func (c *Coordinator) getMapTask(reply *Response) (isAssign bool) {
	if c.mapFreeTasks.Len() == 0 && c.mapDoingTasks.Len() == 0 {
		return false
	}
	// 查找可用的mapTask
	var elem *list.Element
	if c.mapFreeTasks.Len() != 0 {
		// 从free取
		elem = c.mapFreeTasks.Front()
		c.mapFreeTasks.Remove(elem)
	} else {
		// 从doing取
		elem = c.mapDoingTasks.Front()
		for ; elem != nil && time.Since(elem.Value.(*MapTask).pingTime) <= c.timeout; elem = elem.Next() {
		}
		if elem != nil {
			c.mapDoingTasks.Remove(elem)
		}
	}

	// 无可用任务, 但map还没完成
	if elem == nil {
		reply.TaskType = SLEEP
		return true
	}

	mapTask := elem.Value.(*MapTask)
	delete(c.mapTasks, mapTask.taskId)
	taskId := c.nextTaskId
	c.nextTaskId++

	// 设置mapTask信息
	mapTask.pingTime = time.Now()
	mapTask.taskId = taskId

	// 加入doing列表末尾
	// 记录taskId
	c.mapTasks[taskId] = c.mapDoingTasks.PushBack(mapTask)

	// 构造返回值
	reply.TaskType = MAP_TASK
	reply.TaskId = taskId
	reply.NReduce = c.nReduce
	reply.File = append(reply.File, mapTask.file)

	return true
}

func (c *Coordinator) confirmMapTask(args *Request, reply *Response) {
	elem := c.mapTasks[args.TaskId]
	if elem != nil && elem.Value.(*MapTask).taskId == args.TaskId {
		// 任务完成 移除任务
		c.mapDoingTasks.Remove(elem)

		delete(c.mapTasks, args.TaskId)
		// 记录reduce任务
		for _, file := range args.File {
			var reduceId, taskId int
			fmt.Sscanf(file, mapTemFileformat, &reduceId, &taskId)
			c.reduceTaskList[reduceId].files = append(c.reduceTaskList[reduceId].files, file)
		}
		reply.TaskType = TASK_CONFIRM
		return
	}
	reply.TaskType = TASK_FAIL
}

func (c *Coordinator) getReduceTask(reply *Response) {
	if c.reduceFreeTasks.Len() == 0 && c.reduceDoingTasks.Len() == 0 {
		reply.TaskType = EXIT
		return
	}

	var elem *list.Element
	if c.reduceFreeTasks.Len() != 0 {
		// 从free
		elem = c.reduceFreeTasks.Front()
		c.reduceFreeTasks.Remove(elem)
	} else {
		// 从doing取
		elem = c.reduceDoingTasks.Front()
		for ; elem != nil && time.Since(elem.Value.(*ReduceTask).pingTime) <= c.timeout; elem = elem.Next() {
		}
		if elem != nil {
			c.reduceDoingTasks.Remove(elem)
		}
	}

	// 无可用任务, 但reduce还没完成
	if elem == nil {
		reply.TaskType = SLEEP
		return
	}

	reduceTask := elem.Value.(*ReduceTask)
	delete(c.reduceTasks, reduceTask.taskId)
	taskId := c.nextTaskId
	c.nextTaskId++

	// 设置mapTask信息
	reduceTask.pingTime = time.Now()
	reduceTask.taskId = taskId

	// 加入doing列表末尾
	// 记录taskId
	c.reduceTasks[taskId] = c.reduceDoingTasks.PushBack(reduceTask)

	// 构造返回值
	reply.TaskType = REDUCE_TASK
	reply.TaskId = taskId
	reply.File = reduceTask.files
	reply.ReduceIdx = reduceTask.fileId
}

func (c *Coordinator) confirmReduceTask(args *Request, reply *Response) {
	elem := c.reduceTasks[args.TaskId]
	if elem != nil && elem.Value.(*ReduceTask).taskId == args.TaskId {
		// 任务完成 移除任务
		c.reduceDoingTasks.Remove(elem)
		delete(c.reduceTasks, args.TaskId)
		reply.TaskType = TASK_CONFIRM
		return
	}
	reply.TaskType = TASK_FAIL
}
// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	// ret := false

	// Your code here.
	return c.mapFreeTasks.Len()==0 &&
		c.mapDoingTasks.Len()==0 &&
		c.reduceFreeTasks.Len()==0 &&
		c.reduceDoingTasks.Len()==0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{}

	// Your code here.
	c.timeout = serverTimeout
	c.nReduce = nReduce

	c.mapTasks = map[int]*list.Element{}
	c.mapDoingTasks = list.New()
	c.mapFreeTasks = list.New()
	for _, file := range files {
		c.mapFreeTasks.PushBack(&MapTask{file: file, taskId: -2})
	}

	c.reduceTasks = map[int]*list.Element{}
	c.reduceDoingTasks = list.New()
	c.reduceFreeTasks = list.New()
	c.reduceTaskList = make([]*ReduceTask, nReduce)
	for i := range c.reduceTaskList {
		c.reduceTaskList[i] = &ReduceTask{fileId: i, taskId: -2}
		c.reduceFreeTasks.PushBack(c.reduceTaskList[i])
	}
	c.server()
	return c
}
