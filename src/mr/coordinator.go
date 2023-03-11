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
	file        string
	pingTime    time.Time
	workerTable []int
}

type ReduceTask struct {
	fileId      int //中间文件
	files       []string
	pingTime    time.Time
	workerTable []int
}

type Coordinator struct {
	// Your definitions here.
	mtx        sync.Mutex
	nextTaskId int
	timeout    time.Duration
	nReduce    int

	mapTasks       map[int]*list.Element
	mapTaskLRUList *list.List

	reduceTaskList    []*ReduceTask
	reduceTasks       map[int]*list.Element
	reduceTaskLRUList *list.List
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
	reply.TaskType = PING_TERMINATE
	if mapTask := c.mapTasks[args.TaskId]; mapTask != nil {
		m := mapTask.Value.(*MapTask)
		m.pingTime = time.Now()
		c.mapTaskLRUList.MoveToBack(mapTask)
		reply.TaskType = PING_CONFIRM
	}
	if reduceTask := c.reduceTasks[args.TaskId]; reduceTask != nil {
		r := reduceTask.Value.(*ReduceTask)
		r.pingTime = time.Now()
		c.reduceTaskLRUList.MoveToBack(reduceTask)
		reply.TaskType = PING_CONFIRM
	}
}

func (c *Coordinator) getMapTask(reply *Response) (isAssign bool) {
	if c.mapTaskLRUList.Len() == 0 {
		return false
	}
	// 查找可用的mapTask
	elem := c.mapTaskLRUList.Front()
	leastWorksElem := c.mapTaskLRUList.Front()
	for ; elem != nil && len(elem.Value.(*MapTask).workerTable) > 0 && time.Since(elem.Value.(*MapTask).pingTime) <= c.timeout; elem = elem.Next() {
		if len(elem.Value.(*MapTask).workerTable) < len(leastWorksElem.Value.(*MapTask).workerTable) {
			leastWorksElem = elem
		}
	}

	// 无可用任务, 但map还没完成
	if elem == nil {
		elem = leastWorksElem
	}

	// 设置mapTask信息
	mapTask := elem.Value.(*MapTask)
	taskId := c.nextTaskId
	c.nextTaskId++
	mapTask.workerTable = append(mapTask.workerTable, taskId)
	mapTask.pingTime = time.Now()

	// 移到链表末尾
	c.mapTaskLRUList.MoveToBack(elem)

	// 记录taskId
	c.mapTasks[taskId] = elem

	// 构造返回值
	reply.TaskType = MAP_TASK
	reply.TaskId = taskId
	reply.NReduce = c.nReduce
	reply.File = append(reply.File, mapTask.file)

	return true
}

func (c *Coordinator) confirmMapTask(args *Request, reply *Response) {
	elem := c.mapTasks[args.TaskId]
	if elem != nil {
		// 任务完成 移除任务
		c.mapTaskLRUList.Remove(elem)
		for _, taskId := range elem.Value.(*MapTask).workerTable {
			delete(c.mapTasks, taskId)
		}
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
	if c.reduceTaskLRUList.Len() == 0 {
		reply.TaskType = EXIT
		return
	}

	// 查找可用的mapTask
	elem := c.reduceTaskLRUList.Front()
	leastWorksElem := c.reduceTaskLRUList.Front()
	for ; elem != nil && len(elem.Value.(*ReduceTask).workerTable) > 0 && time.Since(elem.Value.(*ReduceTask).pingTime) <= c.timeout; elem = elem.Next() {
		if len(elem.Value.(*ReduceTask).workerTable) < len(leastWorksElem.Value.(*ReduceTask).workerTable) {
			leastWorksElem = elem
		}
	}

	// 无可用任务, 但map还没完成
	if elem == nil {
		elem = leastWorksElem
	}

	// 设置mapTask信息
	reduceTask := elem.Value.(*ReduceTask)
	taskId := c.nextTaskId
	c.nextTaskId++
	reduceTask.workerTable = append(reduceTask.workerTable, taskId)
	reduceTask.pingTime = time.Now()

	// 移到链表末尾
	c.reduceTaskLRUList.MoveToBack(elem)

	// 记录taskId
	c.reduceTasks[taskId] = elem

	// 构造返回值
	reply.TaskType = REDUCE_TASK
	reply.TaskId = taskId
	reply.File = reduceTask.files
	reply.ReduceIdx = reduceTask.fileId

	return
}

func (c *Coordinator) confirmReduceTask(args *Request, reply *Response) {
	elem := c.reduceTasks[args.TaskId]
	if elem != nil {
		// 任务完成 移除任务
		c.reduceTaskLRUList.Remove(elem)
		for _, taskId := range elem.Value.(*ReduceTask).workerTable {
			delete(c.reduceTasks, taskId)
		}
	}
	reply.TaskType = TASK_CONFIRM
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
	return c.mapTaskLRUList.Len() == 0 &&
		c.reduceTaskLRUList.Len() == 0
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
	c.mapTaskLRUList = list.New()
	for _, file := range files {
		c.mapTaskLRUList.PushBack(&MapTask{file: file})
	}

	c.reduceTasks = map[int]*list.Element{}
	c.reduceTaskList = make([]*ReduceTask, nReduce)
	c.reduceTaskLRUList = list.New()
	for i := range c.reduceTaskList {
		c.reduceTaskList[i] = &ReduceTask{fileId: i}
		c.reduceTaskLRUList.PushBack(c.reduceTaskList[i])
	}
	c.server()
	return c
}
