package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

var curTaskId atomic.Int64

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapTask(mapf func(string, string) []KeyValue, r *Response) {
	// 获得文件内容
	content, err := os.ReadFile(r.File[0])
	if err != nil {
		fmt.Printf("doMapTask: can not open %s, %v\n", r.File[0], err)
		return
	}

	kvs := mapf(r.File[0], string(content))
	hashedKvs := make([][]KeyValue, r.NReduce)

	for _, kv := range kvs {
		hashedKvs[ihash(kv.Key)%r.NReduce] = append(hashedKvs[ihash(kv.Key)%r.NReduce], kv)
	}

	files := make([]string, 0, r.NReduce)
	for i := range hashedKvs {
		oname := fmt.Sprintf(mapTemFileformat, i, r.TaskId)
		ofile, _ := os.Create(oname)
		buf := bufio.NewWriter(ofile)
		for _, kv := range hashedKvs[i] {
			data, _ := json.Marshal(kv)
			buf.Write(data)
		}
		buf.Flush()
		ofile.Close()
		files = append(files, oname)
	}

	args := Request{}
	reply := Response{}
	args.TaskType = MAP_CONFIRM
	args.File = files
	args.TaskId = int(curTaskId.Load())

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok || reply.TaskType == TASK_FAIL {
		for _, file := range files {
			os.Remove(file)
		}
	}
}

func doReduceTask(reducef func(string, []string) string, r *Response) {
	reduceFileNum := r.ReduceIdx
	intermediate := shuffle(r.File)
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, reduceTemFileformat)
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	defer tempFile.Close()
	buf := bufio.NewWriter(tempFile)
	for i := 0; i < len(intermediate); {
		key := intermediate[i].Key
		var values []string
		for j:=i; i < len(intermediate) && intermediate[j].Key == intermediate[i].Key; i++ {
			values = append(values, intermediate[i].Value)
		}
		output := reducef(key, values)
		buf.WriteString(strings.Join([]string{key, " ", output, "\n"}, ""))
	}
	buf.Flush()

	// 在完全写入后进行重命名
	fn := fmt.Sprintf(resultFilenameFormat, reduceFileNum)
	os.Rename(tempFile.Name(), fn)

	args := Request{}
	reply := Response{}
	args.TaskType = REDUCE_CONFIRM
	args.TaskId = int(curTaskId.Load())

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok || reply.TaskType == TASK_FAIL {
		os.Remove(fn)
	}

	for _, file := range r.File {
		os.Remove(file)
	}
}

// 获取kv数组, 并返回有序的kv数组
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})
	return kva
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	curTaskId.Store(-1)

	// 定时ping server
	done := make(chan struct{})
	go func() {
		ticker := time.Tick(clienPingFrequency)
		for {
			select {
			case <-done:
				break
			case <-ticker:
				args := Request{}
				reply := Response{}
				args.TaskType = PING
				args.TaskId = int(curTaskId.Load())
				call("Coordinator.GetTask", &args, &reply)
			}
		}
	}()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := &Request{}
		reply := &Response{}
		args.TaskType = WAITTING

		ok := call("Coordinator.GetTask", args, reply)
		if !ok {
			close(done)
			break
		}

		switch reply.TaskType {
		case MAP_TASK:
			curTaskId.Store(int64(reply.TaskId))
			doMapTask(mapf, reply)
		case REDUCE_TASK:
			curTaskId.Store(int64(reply.TaskId))
			doReduceTask(reducef, reply)
		case SLEEP:
			time.Sleep(clientWaittingDuration)
		case EXIT:
			close(done)
			log.Println("complete task")
			return
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
