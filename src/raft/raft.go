package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

type NodeState int

const (
	FOLLOWER NodeState = iota
	CANDIDATE
	LEADER
)

const HEATBEAT float64 = 150    // leader send heatbeat per 150ms
const TIMEOUT_LOW float64 = 500 // the timeout period randomize between 500ms - 1000ms
const TIMEOUT_HIGH float64 = 1000
const CHECK_PERIOD float64 = 100 // check timeout per 100ms
const REPLICATOR_PERIOD float64 = 10
const APPLIER_PERIOD float64 = 10

// const CHECK_APPLY_PERIOD float64 = 10   // check apply per 10ms
// const CHECK_APPEND_PERIOED float64 = 10 // check append per 10ms
// const CHECK_COMMIT_PERIOED float64 = 10 // check commit per 10ms

// log entry struct
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg

	currentTerm int
	votedFor    int
	log         []Entry // the first entry is a dummy entry which contains LastSnapshotTerm, LastSnapshotIndex and nil Command

	commitIndex       int // index of highest log entry known to be committed
	lastApplied       int // index of highest log entry applied to state machine
	state             NodeState
	electionTimestamp time.Time // last time receive the leader's heartbeat

	// volatile state on leaders
	nextIndex  []int // for each server, index of next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
}

func (rf *Raft) getFirstLog() *Entry {
	return &rf.log[0]
}

func (rf *Raft) getLastLog() *Entry {
	return &rf.log[len(rf.log)-1]
}

func (rf *Raft) followOther(currentTerm int) {
	rf.state = FOLLOWER
	rf.currentTerm = currentTerm
	rf.votedFor = -1
	rf.electionTimestamp = time.Now()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) genRaftstate() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	rf.persister.Save(rf.genRaftstate(), rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("[Raft %v] failed to call readPersist", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex {
		DPrintf("[Raft %v] rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	rf.log = shrinkEntriesArray(rf.log[index-snapshotIndex:])
	rf.log[0].Command = nil

	rf.persister.Save(rf.genRaftstate(), snapshot)
	DPrintf("[Raft %v]'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index, snapshotIndex)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the Raft instance is not the leader or has been killed, return gracefully
	if rf.state != LEADER || rf.killed() {
		return index, term, false
	}

	DPrintf("[term %d]: [Raft %v] start consensus", rf.currentTerm, rf.me)
	// append the entry to Raft's log
	index = rf.getLastLog().Index + 1
	term = rf.currentTerm
	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Index: index, Command: command})
	rf.persist()
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// a dedicated applier goroutine to guarantee that each log will be push into applyCh exactly once,
// ensuring that service's applying entries and raft's committing entries can be parallel
func (rf *Raft) applier() {
	for {
		time.Sleep(time.Millisecond * time.Duration(APPLIER_PERIOD))
		if rf.killed() {
			return
		}
		rf.mu.RLock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.RUnlock()
			continue
		}

		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.RUnlock()

		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}

		rf.mu.Lock()
		DPrintf("[Raft %v] applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		if commitIndex > rf.lastApplied {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		dead:              0,
		applyCh:           applyCh,
		currentTerm:       0,
		votedFor:          -1,
		log:               make([]Entry, 1),
		commitIndex:       0,
		lastApplied:       0,
		state:             FOLLOWER,
		electionTimestamp: time.Now(),
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = rf.getFirstLog().Index
	rf.lastApplied = rf.commitIndex

	for i := 0; i < len(peers); i++ {
		if i == rf.me {
			continue
		}
		rf.matchIndex[i], rf.nextIndex[i] = 0, rf.getLastLog().Index+1
		// start replicator goroutine
		go rf.replicator(i)
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()

	return rf
}
