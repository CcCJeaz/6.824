# Lab1

## 了解MapReduce基本原理

> MapReduce的翻译论文: https://www.cnblogs.com/fuzhe1989/p/3413457.html

* MapReduce的目标: 用户只需要编写map和reduce函数, 连同输入文件交付给master, 便能利用多台机器的性能快速得到运行结果
* 主从架构: 一个master - 多个worker
* 具体工作原理: 论文第三节

## 通信协议

```go
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
	MAP_TASK
	REDUCE_TASK
	SLEEP
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
```

workder只通过一个rpc函数来获取指令, 所以各类请求交叉在一个结构中会有些混乱, 实际上可以看作以下结构

```go
// 向master报告,自己正在等待状态中可以分配任务,返回MapTaskResponse, ReduceTaskResponse, SleepResponse, ExitResponse之一
type WaittingRequest struct {}

// 发送心跳包, 告诉master自己正在执行的任务, 返回PingResponse
type PingRequest struct {
  TaskId int
}

// 告知master map任务完成, 返回TaskConfirmResponse, TaskFailResponse之一
type MapConfirmRequest struct {
  TaskId int
  File []string
}

// 告知master reduce任务完成, 返回TaskConfirmResponse, TaskFailResponse之一
type ReduceConfirmRequest struct {
  TaskId int
}
/******/

type TaskConfirmResponse struct {
}

type TaskFailResponse struct {
}

type PingResponse struct {
}

type MapTaskResponse struct {
  TaskId int
  File string
  NReduce int
}

type ReduceTaskResponse struct {
  TaskId int
  File []string
  ReduceIdx int
}

type SleepResponse struct {
}

type ExitResponse struct {
}
```

## Master实验思路

### 数据结构

* MapTask: 
  * file保存输入文件名
  * taskId任务号
  * pingTime保存上一次接收到执行task的worker心跳包的时间
* ReduceTask: 
  * fileId表示第fileId reduce任务([0, nReduce])
  * files保存reduce的输入文件列表, 也是map执行结果的文件列表
  * 其他同上
* Coordinator: 
  * timeout: 用于判断任务是否超时, time.Since(pingTime) <= c.timeout 认为未超时, 反之则超时
  * mapTasks: taskId 到 \*MapTask实例 的映射, 用于快速找到taskId对应的任务
  * mapDoingTask: lru链表, 冷数据链表头, 热数据在链节尾, 获取task时最差情况下会遍历全部节点
  * mapFreeTask: 从未被分配给worker的task, 服务启动时全部任务都在Free链表
  * reduceTaskList: len(reduceTaskList) == nReduce, 主要用于master确认map工作时快速的归类

```go
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
```

### 分配任务

整体思路: 找可用的map任务, map全部完成则再找可用的reduce任务 (**reduce必须在map全部完成后才能分配)**

具体操作都是现从free链表直接取, free为空才到doing链表中取超时的任务进行分配, 还是没有找到任务就让worker睡眠等待

### 确认任务

### 心跳包

## Worker实验思路

### 心跳包定时任务

### 执行map任务

### 执行reduce任务