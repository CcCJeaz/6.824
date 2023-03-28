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
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	NO_VOTED_FOR = -1
	NO_LEADER    = -1
)

const (
	ELECTION_TIMEOUT       = 500
	ELECTION_TIMEOUT_RANGE = 500
)

const (
	HEARTBEAT_INTERVAL = 100
)

// 当每个 Raft peer 意识到连续的日志条目被提交时，
// peer 应该通过传递给 Make() 的 applyCh 向同一服务器上的服务（或测试器）发送一个 ApplyMsg。
// 将 CommandValid 设置为 true 以指示 ApplyMsg 包含新提交的日志条目。
//
// 在 part 2D 中，您需要在 applyCh 上发送其他类型的消息（例如，快照），
// 所以应当将 CommandValid 设置为 false 以用于这些其他用途。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type BriefLogInfo struct {
	Term  int
	Index int
	Count int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         *Buffer[*LogEntry]

	// Volatile state on all servers
	commitIndex int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// 用于快速恢复
	briefLogList []BriefLogInfo

	leaderId int

	// 用于通知上层应用
	applyCh chan ApplyMsg

	// 用于停止计时器
	terminateTickerCh chan struct{}

	// 用于重置定时器
	resetTickerCh chan struct{}

	// 用于通知leader协程处理用户的请求
	noticeLeaderCh chan struct{}

	// 用于停止leader状态
	leaderCtxCancel context.CancelFunc

	// 用于重置心跳包发送计时器
	resetHeartbeatTickerChs []chan struct{}
}

func (rf *Raft) updateTermInfo(term int, votedFor int, leaderId int) {
	rf.currentTerm = term
	rf.votedFor = votedFor
	if rf.leaderId == rf.me {
		rf.leaderId = leaderId
		rf.leaderCtxCancel()
	}
	rf.leaderId = leaderId
}

func (rf *Raft) resetHeartbeatTicker(server int) {
	select {
	case rf.resetHeartbeatTickerChs[server] <- struct{}{}:
	default:
	}
}

func (rf *Raft) noticeLeader() {
	select {
	case rf.noticeLeaderCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) terminateTicker() {
	if rf.terminateTickerCh != nil {
		close(rf.terminateTickerCh)
		rf.terminateTickerCh = nil
	}
}

func (rf *Raft) resetTicker() {
	select {
	case rf.resetTickerCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) ticker(terminateTickerCh chan struct{}) {
	for !rf.killed() {
		ms := ELECTION_TIMEOUT + (rand.Int63() % ELECTION_TIMEOUT_RANGE)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			go rf.election()
		case <-rf.resetTickerCh:
		case <-terminateTickerCh:
			return
		}
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.leaderId == rf.me
}

// 使用 Raft 的服务（例如 k/v 服务器）希望就下一个要附加到 Raft 日志的命令达成协议。
// 如果此服务器不是领导者，则返回 false。否则启动协议并立即返回。
// 无法保证此命令将永远提交到 Raft 日志，因为领导者可能会失败或失去选举。
// 即使 Raft 实例已经被杀死，这个函数也应该优雅地返回。
//
// 第一个返回值是命令将在提交时出现的索引。
// 第二个返回值是当前任期
// 第三返回值, 当服务器任务自己是leader时为true
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.killed() {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.leaderId != rf.me {
		return -1, -1, false
	}

	rf.log.Push(&LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})

	rf.briefLogList[len(rf.briefLogList)-1].Count++

	rf.persist()

	DPrintf("(Submit Term %d Leader %d) Leader submit index %d\tcmd %v", rf.currentTerm, rf.me, rf.log.BackIndex(), command)

	rf.noticeLeader()

	return rf.log.BackIndex(), rf.currentTerm, true
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = NO_VOTED_FOR
	rf.log = NewBuffer[*LogEntry]()
	rf.log.Push(&LogEntry{nil, 0})

	rf.commitIndex = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.leaderId = NO_LEADER

	rf.applyCh = applyCh

	rf.resetTickerCh = make(chan struct{})

	rf.noticeLeaderCh = make(chan struct{})

	terminateTickerCh := make(chan struct{})
	rf.terminateTickerCh = terminateTickerCh

	rf.resetHeartbeatTickerChs = make([]chan struct{}, len(peers))
	for i := range rf.resetHeartbeatTickerChs {
		rf.resetHeartbeatTickerChs[i] = make(chan struct{})
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker(terminateTickerCh)

	return rf
}
