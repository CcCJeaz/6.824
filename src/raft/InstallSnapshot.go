package raft

import "time"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// Offset            int
	// Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// other server has higher term !
	if args.Term > rf.currentTerm || rf.state != FOLLOWER {
		rf.followOther(args.Term)
	}

	rf.electionTimestamp = time.Now()

	reply.Term = rf.currentTerm

	// outdated snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	left := max(0, args.LastIncludedIndex-rf.getFirstLog().Index)
	left = min(left, len(rf.log)-1)
	rf.log[left].Index = args.LastIncludedIndex
	rf.log[left].Term = args.LastIncludedTerm
	rf.log[left].Command = nil

	rf.log = shrinkEntriesArray(rf.log[left:])

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = rf.commitIndex

	rf.persister.Save(rf.genRaftstate(), args.Data)

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

	DPrintf("[Raft %v]'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotArgs %v and reply InstallSnapshotReply %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args.LastIncludedIndex, reply)
}

func (rf *Raft) genInstallSnapshotArgs(server int) *InstallSnapshotArgs {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.getFirstLog().Index,
		LastIncludedTerm:  rf.getFirstLog().Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	return args
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) handleInstallSnapshotReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if args.Term != rf.currentTerm {
		return
	}
	// other server has higher term !
	if reply.Term > rf.currentTerm {
		rf.followOther(reply.Term)
		return
	}
	rf.matchIndex[server] = max(args.LastIncludedIndex, rf.matchIndex[server])
	rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
}
