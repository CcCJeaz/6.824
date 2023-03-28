package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// 向服务器发送 RequestVote RPC 的示例代码。
// 服务器是目标服务器在 rf.peers[] 中的索引。
// 需要 args 中的 RPC 参数。
// 将 RPC 回复填入 *reply，因此调用者应传递 &reply。
// 传递给 Call() 的参数和回复的类型必须是
// 与处理函数中声明的参数类型相同（包括它们是否为指针）。
//
// labrpc 包模拟有损网络，其中服务器可能无法访问，并且请求和回复可能会丢失。
// Call() 发送请求并等待回复。 如果回复在超时间隔内到达，则 Call() 返回 true； 否则 Call() 返回 false。
// 因此 Call() 可能暂时不会返回。
// 一个错误的返回可能是由死服务器、无法访问的活服务器、丢失的请求或丢失的回复引起的。
//
// 如果服务器端的处理函数不返回，则 Call() 保证返回（可能在延迟之后）*除外*。
// 因此无需围绕 Call() 实现您自己的超时。
//
// 查看 ../labrpc/labrpc.go 中的注释以获取更多详细信息。
//
// 如果您在使 RPC 工作时遇到问题，请检查您是否已将通过 RPC 传递的结构中的所有字段名称大写，
// 并且调用者使用 & 传递回复结构的地址，而不是结构本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok && !rf.killed()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Term = 0
		reply.VoteGranted = false
		return
	}

	checkTerm := args.Term > rf.currentTerm ||
		args.Term == rf.currentTerm && (rf.votedFor == NO_VOTED_FOR || rf.votedFor == args.CandidateId)
	checkLog := args.LastLogTerm > rf.log.Back().Term ||
		args.LastLogTerm == rf.log.Back().Term && args.LastLogIndex >= rf.log.BackIndex()

	if checkTerm && checkLog {
		// 重置计时器
		rf.resetTicker()

		// 候选者任期号大于等于当前任期, 并且对方日志至少和自己一样新
		rf.updateTermInfo(args.Term, args.CandidateId, NO_LEADER)

		reply.VoteGranted = true
		reply.Term = args.Term

	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		// 对方任期大, 更新自己的任期
		if args.Term > rf.currentTerm {
			rf.updateTermInfo(args.Term, NO_VOTED_FOR, NO_LEADER)
		}
	}

}
