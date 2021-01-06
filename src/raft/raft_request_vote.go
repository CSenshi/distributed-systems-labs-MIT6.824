package raft

// RequestVoteArgs struct for RequestVote RPC call Argument
type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// RequestVoteReply struct for RequestVote RPC call Reply
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC call on client Side
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// Rule for all servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		_, _ = DPrintf(vote("[T%v] %v: Received RequestVote from %v | Discarded Vote | Received Lower Term "), rf.currentTerm, rf.me, args.CandidateID, args.CandidateID)
		return
	}

	/* 2. If
	 *		1. votedFor is null or candidateId
	 *		2. candidate’s log is at least as up-to-date as receiver’s log
	 *	grant vote (§5.2, §5.4)
	 */

	// Check 1 vote: should be able to vote or voted for candidate
	voteCheck := rf.votedFor == noVote || rf.votedFor == args.CandidateID
	// Check 2 up-to-date = (same indices OR candidate's lastLogIndex > current peer's lastLogIndex)
	lastLogIndex, lastLogTerm := rf.lastLogEntryIndex(), rf.lastLogEntryTerm()
	logCheck := lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)

	// Both checks should be true to grant vote
	if voteCheck && logCheck {
		reply.VoteGranted = true
		_, _ = DPrintf(vote("[T%v] %v: Received RequestVote from %v | Vote Successful"), rf.currentTerm, rf.me, args.CandidateID)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
	} else if !voteCheck {
		_, _ = DPrintf(vote("[T%v] %v: Received RequestVote from %v | Vote Failure | Already voted for %v"), rf.currentTerm, rf.me, args.CandidateID, rf.votedFor)
	} else {
		_, _ = DPrintf(vote("[T%v] %v: Received RequestVote from %v | Vote Failure | No Up-To-Date Log | Received {LastLogTerm: %v, LastLogIndex: %v} | Current {LastLogTerm: %v, LastLogIndex: %v}"),
			rf.currentTerm, rf.me, args.CandidateID, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
	}
	rf.resetTTL()
}

// Create Request Vote RPC Argument
func (rf *Raft) createRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastLogEntryIndex(),
		LastLogTerm:  rf.lastLogEntryTerm(),
	}
}

// code to send a RequestVote RPC to a server.
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// RPC Send Request
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		rf.mu.Lock()
		_, _ = DPrintf(red("[T%v] %v: Network Error! RequestVote: No connection to Peer %v"), rf.currentTerm, rf.me, server)
		rf.mu.Unlock()
		return false
	}

	// Evaluate RPC Result
	rf.processRequestVoteReply(server, args, reply)
	return true
}

// Process Request Vote RPC Reply
func (rf *Raft) processRequestVoteReply(peerNum int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule for all servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		return
	}

	if reply.VoteGranted {
		rf.votesReceived++
		if rf.state == leader {
			return
		}
		// it wins the election
		if rf.votesReceived > len(rf.peers)/2 {
			_, _ = DPrintf(newLeader("[T%v] %v: New Leader! (%v/%v votes) (%v -> %v)"), rf.currentTerm, rf.me, rf.votesReceived, len(rf.peers), rf.state, leader)
			rf.state = leader

			// Initialize all nextIndex values to the index just after the last one in its log
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
			}

			// send heartbeat messages to all of the other servers to establish its authority (§5.2)
			go rf.sendPeriodicHeartBeats()
		}
	}
}
