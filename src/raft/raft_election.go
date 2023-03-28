package raft

import (
	"context"
	"sync"
	"time"
)

func (rf *Raft) election() {
	rf.mu.Lock()

	if rf.killed() || rf.leaderId == rf.me {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderId = NO_LEADER

	nNode := len(rf.peers)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.BackIndex(),
		LastLogTerm:  rf.log.Back().Term,
	}

	rf.mu.Unlock()

	votes := make(chan bool, nNode-1)
	// 发送RequestVote请求
	for i := 0; i < nNode; i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.updateTermInfo(reply.Term, NO_VOTED_FOR, NO_LEADER)
			}
			// fmt.Printf("===============term-%d, candidate-%d --> server-%d, result: %v===============\n", args.Term, rf.me, server, ok && reply.VoteGranted)
			rf.mu.Unlock()
			votes <- ok && reply.VoteGranted
		}(i)
	}

	// 处理票数
	counter := 1
	for i := 0; i < nNode-1 && counter <= nNode/2; i++ {
		if <-votes {
			counter++
		}
	}
	// 票数不足
	if counter <= nNode/2 {
		return
	}

	go rf.startLeader(args.Term)
}

func (rf *Raft) startLeader(term int) {
	rf.mu.Lock()
	if rf.killed() || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	// 停止计时器
	rf.terminateTicker()

	nNode := len(rf.peers)

	// 选举成功, 更新信息
	rf.leaderId = rf.me

	rf.briefLogList = nil
	for i, end := rf.log.FrontIndex(), rf.log.BackIndex(); i <= end; {
		next := i
		for ; next <= end && rf.log.GetByIndex(next).Term == rf.log.GetByIndex(i).Term; next++ {
		}
		rf.briefLogList = append(rf.briefLogList, BriefLogInfo{rf.log.GetByIndex(i).Term, i, next - i})
		i = next
	}
	rf.briefLogList = append(rf.briefLogList, BriefLogInfo{rf.currentTerm, rf.log.BackIndex() + 1, 0})

	var ctx context.Context
	ctx, rf.leaderCtxCancel = context.WithCancel(context.Background())

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.BackIndex() + 1
		rf.matchIndex[i] = 0
	}

	rf.mu.Unlock()

	// 立即发送心跳通知所有节点自己赢得了选举
	for i := 0; i < nNode; i++ {
		if i == rf.me {
			continue
		}
		go rf.notifyHeartbeatPacket(i)
	}

	var wg sync.WaitGroup

	// 为每个节点单独的发送心跳
	for i := 0; i < nNode; i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			for !rf.killed() {
				ms := HEARTBEAT_INTERVAL
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(ms) * time.Millisecond):
					go rf.notifyHeartbeatPacket(server)
				case <-rf.resetHeartbeatTickerChs[server]:
				}
			}
		}(i)
	}
OUT:
	for {
		select {
		case <-ctx.Done():
			break OUT
		case <-rf.noticeLeaderCh:
			go rf.broadcastAppendEntriesRequest()
		}
	}

	wg.Wait()

	rf.mu.Lock()

	if rf.killed() {
		rf.mu.Unlock()
		return
	}

	if rf.leaderId != rf.me && rf.terminateTickerCh == nil {
		terminateTickerCh := make(chan struct{})
		rf.terminateTickerCh = terminateTickerCh
		// 重启计时器
		go rf.ticker(terminateTickerCh)
	}

	rf.mu.Unlock()

}
