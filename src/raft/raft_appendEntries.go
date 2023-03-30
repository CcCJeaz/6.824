package raft

import (
	"sort"
)

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term         int
	Success      bool
	BriefLogList []BriefLogInfo
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok && !rf.killed()
}

// AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Term = 0
		reply.Success = false
		return
	}

	// 对方任期小于自己, 拒绝
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 承认对方是领导者, 更新自己的信息
	rf.updateTermInfo(args.Term, args.LeaderId, args.LeaderId)

	rf.resetTicker()

	// 领导者发送的日志领先自己 或者 相同索引处日志不同, 返回缩略日志列表
	if args.PreLogIndex > rf.log.BackIndex() ||
		args.PreLogTerm != rf.log.Get(args.PreLogIndex).Term {
		reply.Success = false
		reply.Term = rf.currentTerm

		for i, end := rf.log.FrontIndex(), rf.log.BackIndex(); i <= end; {
			next := i
			for ; next <= end && rf.log.Get(i).Term == rf.log.Get(next).Term; next++ {
			}
			reply.BriefLogList = append(reply.BriefLogList, BriefLogInfo{rf.log.Get(i).Term, i, next - i})
			i = next
		}
		return
	}

	// 需要新添日志 或者 覆盖错误的日志
	if len(args.Entries) > 0 &&
		(args.PreLogIndex+len(args.Entries) > rf.log.BackIndex() ||
			rf.log.Get(args.PreLogIndex+len(args.Entries)).Term != args.Entries[len(args.Entries)-1].Term) {
		// 放入日志
		rf.log.Cut(rf.log.FrontIndex(), args.PreLogIndex+1)
		for i := range args.Entries {
			rf.log.Push(&args.Entries[i])
			DPrintf("(AppendLog Term %d Follower %d) Follower agree term %d index %d\tcmd %v\n",
				rf.currentTerm, rf.me, args.Entries[i].Term, rf.log.BackIndex(), args.Entries[i].Command)
		}
		rf.persist()
	}

	// 更新commitIndex, 向上层应用报告
	if rf.commitIndex < args.LeaderCommit {
		for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.Get(i).Command,
				CommandIndex: i,
			}
			rf.applyCh <- msg
		}
		rf.commitIndex = args.LeaderCommit
	}

	// 成功返回
	reply.Success = true
	reply.Term = rf.currentTerm

}

func (rf *Raft) notifyHeartbeatPacket(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.leaderId != rf.me {
		return
	}

	rf.resetHeartbeatTicker(server)

	// 构造参数
	preLogIndex, preLogTerm := rf.nextIndex[server]-1, rf.log.Get(rf.nextIndex[server]-1).Term
	entries := []LogEntry{}

	for i, end := preLogIndex+1, rf.log.BackIndex(); i <= end; i++ {
		entries = append(entries, *(rf.log.Get(i)))
	}

	args := &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PreLogIndex:  preLogIndex,
		PreLogTerm:   preLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesResponse{}

	rf.mu.Unlock()
	ok := rf.sendAppendEntries(server, args, reply)
	rf.mu.Lock()

	if !ok || rf.killed() || rf.leaderId != rf.me {
		return
	}

	// 当前任期已经落后了, 终止领导者状态
	if reply.Term > rf.currentTerm {
		rf.updateTermInfo(reply.Term, NO_VOTED_FOR, NO_LEADER)
		return
	}

	// 心跳包
	if len(entries) == 0 && reply.Success {
		rf.matchIndex[server] = max(rf.matchIndex[server], preLogIndex)
		return
	}

	if reply.Success {
		// 处理成功
		rf.matchIndex[server] = max(rf.matchIndex[server], preLogIndex+len(entries))
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		nNode := len(rf.peers)
		matchArray := make([]int, nNode)
		for i := 0; i < nNode; i++ {
			if i == rf.me {
				matchArray[i] = rf.log.BackIndex()
			} else {
				matchArray[i] = rf.matchIndex[i]
			}
		}
		sort.Ints(matchArray)

		// 更新commitIndex
		lastCommitIndex := matchArray[(nNode+1)/2-1]

		for i := rf.commitIndex + 1; i <= lastCommitIndex; i++ {
			DPrintf("(ApplyMsg Term %d Leader %d) Leader apply index %d\tcmd %v\n", rf.currentTerm, rf.leaderId, i, rf.log.Get(i).Command)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log.Get(i).Command,
				CommandIndex: i,
			}
		}

		rf.commitIndex = max(rf.commitIndex, lastCommitIndex)

	} else {
		l, r := 0, min(len(rf.briefLogList), len(reply.BriefLogList))-1

		for l < r {
			m := (l + r) / 2
			if rf.briefLogList[m] == reply.BriefLogList[m] {
				l = m + 1
			} else {
				r = m
			}
		}

		if rf.briefLogList[l].Term != reply.BriefLogList[l].Term {
			rf.nextIndex[server] = reply.BriefLogList[l].Index
		} else {
			rf.nextIndex[server] = reply.BriefLogList[l].Index + min(reply.BriefLogList[l].Count, rf.briefLogList[l].Count)
		}

		rf.matchIndex[server] = max(rf.matchIndex[server], rf.nextIndex[server]-1)
		rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
	}
}

func (rf *Raft) broadcastAppendEntriesRequest() {
	rf.mu.Lock()
	nNode := len(rf.peers)
	rf.mu.Unlock()

	for i := 0; i < nNode; i++ {
		if i == rf.me {
			continue
		}
		go rf.notifyHeartbeatPacket(i)
	}

}
