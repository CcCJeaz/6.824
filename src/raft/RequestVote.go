package raft

import (
	"math/rand"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("[term %d]: [Raft %v] receive requestVote from [Raft %v]", rf.currentTerm, rf.me, args.CandidateId)

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.followOther(args.Term)
	}

	if lastLog := rf.getLastLog(); args.LastLogTerm < lastLog.Term || args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.votedFor = args.CandidateId
	reply.Term, reply.VoteGranted = rf.currentTerm, true

	rf.electionTimestamp = time.Now()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) StartElection(term int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.currentTerm != term {
		return
	}

	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.state = CANDIDATE

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.getLastLog().Term,
		LastLogIndex: rf.getLastLog().Index,
	}

	DPrintf("[Raft %v] starts election with RequestVoteArgs %v", rf.me, args)
	// use Closure
	grantedVotes := 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(peer, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentTerm != args.Term || rf.state != CANDIDATE {
				return
			}
			if reply.Term > rf.currentTerm {
				DPrintf("[Raft %v] finds a new leader [Raft %v] with term %v and steps down in term %v", rf.me, peer, reply.Term, rf.currentTerm)
				rf.followOther(reply.Term)
				return
			}
			DPrintf("[Raft %v] receives RequestVoteReply %v from [Raft %v] after sending RequestVoteArgs %v in term %v", rf.me, reply, peer, args, rf.currentTerm)
			if reply.VoteGranted {
				grantedVotes += 1
				if grantedVotes > len(rf.peers)/2 {
					DPrintf("[Raft %v] receives majority votes in term %v", rf.me, rf.currentTerm)
					rf.state = LEADER
					for server := range rf.peers {
						rf.nextIndex[server] = rf.getLastLog().Index + 1
					}
					go rf.leaderHeartbeat()
				}
			}

		}(peer)
	}
}

func (rf *Raft) ticker() {
	r := rand.New(rand.NewSource(int64(rf.me)))
	for !rf.killed() {
		timeout := int(r.Float64()*(TIMEOUT_HIGH-TIMEOUT_LOW) + TIMEOUT_LOW)
		rf.mu.RLock()
		// if timeout and the server is not a leader, start election
		if time.Since(rf.electionTimestamp) > time.Duration(timeout)*time.Millisecond && rf.state != LEADER {
			// start a new go routine to do the election. This is important
			// so that if you are a candidate (i.e., you are currently running an election),
			// but the election timer fires, you should start another election.
			go rf.StartElection(rf.currentTerm)
			rf.electionTimestamp = time.Now()
		}
		rf.mu.RUnlock()
		time.Sleep(time.Millisecond * time.Duration(CHECK_PERIOD))
	}
}
