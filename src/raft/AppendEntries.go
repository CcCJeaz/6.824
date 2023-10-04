package raft

import (
	"sort"
	"time"
)

type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderId     int     // so follower canredirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of preLogIndex entry
	Entries      []Entry // log entries to store (empty for heart beat)
	LeaderCommit int     // leader's commitIndex
}

type AppendEntriesReply struct {
	Term        int  // currentTerm, for leader to update itself
	Success     bool // true if follower contained entry matching preLogIndex and prevLogTerm
	ExpectIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("[term %d]: [Raft %v] [state %d] receive AppendEntries [Raft %v] -- Entries %v", rf.currentTerm, rf.me, rf.state, args.LeaderId, args.Entries)

	// reply false immediately if sender's term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// reset timer
	rf.electionTimestamp = time.Now()

	// other server has higher term !
	if args.Term > rf.currentTerm || rf.state != FOLLOWER {
		rf.followOther(args.Term)
	}

	firstIndex := rf.getFirstLog().Index
	// prevLogIndex points to the compressed log
	if args.PrevLogIndex < firstIndex {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ExpectIndex = firstIndex + 1
		DPrintf("[Raft %v] receives unexpected AppendEntriesArgs %v from [Raft %v] because prevLogIndex %v < firstLogIndex %v", rf.me, args, args.LeaderId, args.PrevLogIndex, rf.getFirstLog().Index)
		return
	}

	// lack of logs between lastLog and prevLog sent by leader
	if lastLogIndex := rf.getLastLog().Index; lastLogIndex < args.PrevLogIndex {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ExpectIndex = lastLogIndex + 1
		DPrintf("[Raft %v] receives unexpected AppendEntriesArgs %v from [Raft %v] because lastLogIndex %v < args.PrevLogIndex %v", rf.me, args, args.LeaderId, lastLogIndex, args.PrevLogIndex)
		return
	}

	// log does not match, rollback according to term
	if rf.log[args.PrevLogIndex-firstIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex-firstIndex].Term
		index := args.PrevLogIndex - 1
		for index >= firstIndex && rf.log[index-firstIndex].Term == conflictTerm {
			index--
		}

		reply.Term, reply.Success = rf.currentTerm, false
		reply.ExpectIndex = index + 1
		DPrintf("[Raft %v] receives unexpected AppendEntriesArgs %v from [Raft %v] because rf.log[args.PrevLogIndex-firstIndex].Term %v < args.PrevLogTerm %v", rf.me, args, args.LeaderId, conflictTerm, args.PrevLogTerm)
		return
	}

	// delete conflicting entries and append new entries
	for index, entry := range args.Entries {
		if entry.Index-firstIndex >= len(rf.log) || rf.log[entry.Index-firstIndex].Term != entry.Term {
			rf.log = shrinkEntriesArray(append(rf.log[:entry.Index-firstIndex], args.Entries[index:]...))
			break
		}
	}

	// other server has higher commitIndex !
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLog().Index)
	}

	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) genAppendEntriesArgs(server int) *AppendEntriesArgs {
	term := rf.currentTerm
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex-rf.getFirstLog().Index].Term
	entries := make([]Entry, len(rf.log[prevLogIndex-rf.getFirstLog().Index+1:]))
	copy(entries, rf.log[prevLogIndex-rf.getFirstLog().Index+1:])
	commitIndex := rf.commitIndex
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	return args
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term != rf.currentTerm {
		return
	}

	// other server has higher term !
	if reply.Term > rf.currentTerm {
		rf.followOther(reply.Term)
		return
	}

	if reply.Success {
		if len(args.Entries) == 0 {
			rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex)
		} else {
			rf.matchIndex[server] = max(rf.matchIndex[server], args.Entries[len(args.Entries)-1].Index)
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// check commit
		rf.matchIndex[rf.me] = rf.getLastLog().Index
		matchIndex := make([]int, len(rf.peers))
		copy(matchIndex, rf.matchIndex)
		sort.Ints(matchIndex)
		rf.commitIndex = max(rf.commitIndex, matchIndex[(len(rf.peers)-1)/2])
	} else {
		rf.nextIndex[server] = max(rf.matchIndex[server]+1, reply.ExpectIndex)
	}
}

func (rf *Raft) genHeartbeatArgs(server int) *AppendEntriesArgs {
	term := rf.currentTerm
	prevLogIndex, prevLogTerm := rf.getLastLog().Index, rf.getLastLog().Term
	entries := []Entry{}
	commitIndex := rf.commitIndex
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	return args
}

func (rf *Raft) handleHeartbeatReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term != rf.currentTerm {
		return
	}
	// other server has higher term !
	if reply.Term > rf.currentTerm {
		rf.followOther(reply.Term)
		return
	}
	if !reply.Success {
		rf.nextIndex[server] = max(rf.matchIndex[server]+1, reply.ExpectIndex)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("[Raft %v]: Ask [Raft %v] AppendEntries Args %v and AppendEntries Reply %v", rf.me, server, args, reply)
	return ok
}

// send heartbeat to all other servers (leader only)
func (rf *Raft) leaderHeartbeat() {
	DPrintf("[term %d]:[Raft %v] [state %d] becomes leader !", rf.currentTerm, rf.me, rf.state)
	for !rf.killed() {
		rf.mu.RLock()
		// if the server is dead or is not the leader, just return
		if rf.state != LEADER {
			rf.mu.RUnlock()
			return
		}
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			args := rf.genHeartbeatArgs(server)
			go func(server int) {
				reply := &AppendEntriesReply{}
				if !rf.sendAppendEntries(server, args, reply) {
					return
				}
				rf.mu.Lock()
				rf.handleHeartbeatReply(server, args, reply)
				rf.mu.Unlock()
			}(server)
		}
		rf.mu.RUnlock()
		time.Sleep(time.Millisecond * time.Duration(HEATBEAT))
	}
}

func (rf *Raft) replicator(server int) {
	for {
		time.Sleep(time.Millisecond * time.Duration(REPLICATOR_PERIOD))
		if rf.killed() {
			return
		}
		rf.mu.RLock()
		if rf.state != LEADER || rf.matchIndex[server] >= rf.getLastLog().Index {
			rf.mu.RUnlock()
			continue
		}

		if prevLogIndex := rf.nextIndex[server] - 1; prevLogIndex < rf.getFirstLog().Index {
			// only snapshot can catch up
			args := rf.genInstallSnapshotArgs(server)
			rf.mu.RUnlock()
			go func() {
				reply := &InstallSnapshotReply{}
				if !rf.sendInstallSnapshot(server, args, reply) {
					return
				}
				rf.mu.Lock()
				rf.handleInstallSnapshotReply(server, args, reply)
				rf.mu.Unlock()
			}()
		} else {
			// just entries can catch up
			args := rf.genAppendEntriesArgs(server)
			rf.mu.RUnlock()
			go func() {
				reply := &AppendEntriesReply{}
				if !rf.sendAppendEntries(server, args, reply) {
					return
				}
				rf.mu.Lock()
				rf.handleAppendEntriesReply(server, args, reply)
				rf.mu.Unlock()
			}()
		}

	}
}
