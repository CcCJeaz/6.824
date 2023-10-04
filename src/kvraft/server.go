package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	ExecuteTimeout = 1 // 1s
)

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big
	lastApplied  int

	// Your definitions here.
	stateMachine   KVStateMachine                // KV stateMachine
	lastOperations map[int64]OperationContext    // determine whether log is duplicated by recording the last commandId and response corresponding to the clientId

	notifyChans    map[int]chan *CommandResponse // notify client goroutine by applier goroutine to response
}

type OperationContext struct {
	CommandId int
	Response  *CommandResponse
}

type SnapshotState struct {
	StateMachine   []byte
	LastOperations map[int64]OperationContext
}

func (kv *KVServer) isDuplicateRequest(client int64, commandId int) bool {
	if _, ok := kv.lastOperations[client]; ok {
		return kv.lastOperations[client].CommandId == commandId
	} else {
		return false
	}
}

func (kv *KVServer) applyLogToStateMachine(request *CommandRequest) *CommandResponse {
	reply := &CommandResponse{}
	switch request.Kind {
	case GET:
		reply.Value, reply.Err = kv.stateMachine.Get(request.Key)
	case PUT:
		reply.Err = kv.stateMachine.Put(request.Key, request.Value)
	case APPEND:
		reply.Err = kv.stateMachine.Append(request.Key, request.Value)
	}
	return reply
}

func (kv *KVServer) removeOutdatedNotifyChan(index int) {
	kv.notifyChans[index] = nil
	delete(kv.notifyChans, index)
}

func (kv *KVServer) isNeedSnapshot() bool {
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate
}

func (kv *KVServer) takeSnapshot(lastIncludedIndex int) {
	snapshotState := SnapshotState{}
	snapshotState.StateMachine = kv.stateMachine.GenSnapshot()
	snapshotState.LastOperations = kv.lastOperations

	w := &bytes.Buffer{}
	encoder := labgob.NewEncoder(w)
	encoder.Encode(snapshotState)
	kv.rf.Snapshot(lastIncludedIndex, w.Bytes())
}

func (kv *KVServer) loadSnapshot() {
	snapshot := kv.persister.ReadSnapshot()
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewReader(snapshot)
	decoder := labgob.NewDecoder(r)

	snapshotState := &SnapshotState{}
	decoder.Decode(snapshotState)
	
	kv.lastOperations = snapshotState.LastOperations
	kv.stateMachine.LoadSnapshot(snapshotState.StateMachine)
}

func (kv *KVServer) Command(request *CommandRequest, response *CommandResponse) {
	defer DPrintf("[Node %v] processes CommandRequest %v with CommandResponse %v", kv.me, request, response)
	// return result directly without raft layer's participation if request is duplicated
	kv.mu.Lock()
	if request.Kind != GET && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastResponse := kv.lastOperations[request.ClientId].Response
		response.Value, response.Err = lastResponse.Value, lastResponse.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	// do not hold lock to improve throughput
	// when KVServer holds the lock to take snapshot, underlying raft can still commit raft logs
	index, _, isLeader := kv.rf.Start(request)
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.notifyChans[index] == nil {
		kv.notifyChans[index] = make(chan *CommandResponse, 1)
	}
	ch := kv.notifyChans[index]
	kv.mu.Unlock()

	select {
	case result := <-ch:
		response.Value, response.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout * time.Second):
		response.Err = ErrTimeout
	}
	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	go func() {
		kv.mu.Lock()
		kv.removeOutdatedNotifyChan(index)
		kv.mu.Unlock()
	}()
}

// a dedicated applier goroutine to apply committed entries to stateMachine, take snapshot and apply snapshot from raft
func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			DPrintf("[Node %v] tries to apply message %v", kv.me, message)

			if message.CommandValid {
				kv.mu.Lock()

				if message.CommandIndex <= kv.lastApplied {
					DPrintf("[Node %v] discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.me, message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var response *CommandResponse
				request := message.Command.(*CommandRequest)
				if request.Kind != GET && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
					DPrintf("[Node %v] doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.me, message, kv.lastOperations[request.ClientId], request.ClientId)
					response = kv.lastOperations[request.ClientId].Response
				} else {
					response = kv.applyLogToStateMachine(request)
					if request.Kind != GET {
						kv.lastOperations[request.ClientId] = OperationContext{request.CommandId, response}
					}
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					if ch := kv.notifyChans[message.CommandIndex]; ch != nil {
						ch <- response
					}
				}

				if kv.isNeedSnapshot() {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()

				kv.loadSnapshot()
				if message.SnapshotIndex > kv.lastApplied {
					kv.lastApplied = message.SnapshotIndex
				}

				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&CommandRequest{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	// You may need initialization code here.
	kv.stateMachine = NewMemoryKV()
	kv.lastOperations = map[int64]OperationContext{}
	kv.notifyChans = map[int]chan *CommandResponse{}

	kv.loadSnapshot()

	go kv.applier()

	return kv
}

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
	GenSnapshot() []byte
	LoadSnapshot(snapshot []byte) 
}

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{make(map[string]string)}
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := memoryKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}

func (memoryKV *MemoryKV) GenSnapshot() []byte {
	w := &bytes.Buffer{}
	encoder := labgob.NewEncoder(w)
	encoder.Encode(memoryKV.KV)
	return w.Bytes()
}

func (memoryKV *MemoryKV) LoadSnapshot(snapshot []byte) {
	r := bytes.NewReader(snapshot)
	decoder := labgob.NewDecoder(r)
	decoder.Decode(&memoryKV.KV)
}