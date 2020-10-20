package raft

import "time"

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

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		_, _ = DPrintf(vote("[T%v] %v: Discarded Vote: did not vote for %v"), rf.currentTerm, rf.me, args.CandidateID)
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		_, _ = DPrintf(vote("[T%v -> T%v] %v: Transition to new Term | Received Higher Term RequestVote"), rf.currentTerm, args.Term, rf.me)
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = follower
	}

	/* 2. If
	 *		1. votedFor is null or candidateId
	 *		2. candidate’s log is at least as up-to-date as receiver’s log
	 *	grant vote (§5.2, §5.4)
	 */

	// Check 1
	voteCheck := rf.votedFor == -1 || rf.votedFor == args.CandidateID
	// Check 2
	logCheck := (args.LastLogTerm == rf.log[len(rf.log)-1].Term) ||
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)
	if voteCheck && logCheck {
		reply.VoteGranted = true
		_, _ = DPrintf(vote("[T%v] %v: New Vote: Voted for %v"), rf.currentTerm, rf.me, args.CandidateID)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
	} else {
		_, _ = DPrintf(vote("[T%v] %v: Old Vote: Voted for %v"), rf.currentTerm, rf.me, rf.votedFor)
	}
	rf.resetTTL()
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// create a background goroutine that will kick off leader election periodically by sending out
// RequestVote RPCs when it hasn't heard from another peer for a while. This way a peer
// will learn who is the leader, if there is already a leader, or become the leader itself.
func (rf *Raft) leaderElection() {
	for {
		rf.mu.Lock()
		ttlElapsed := rf.electionStartTime.Before(time.Now().Add(-rf.electionTTL))

		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.state == leader {
			rf.mu.Unlock()
			time.Sleep(time.Duration(dummySleepNoElection) * time.Millisecond)
		} else if !ttlElapsed /* && (rf.state == followerState || rf.state == candidateState) */ {
			rf.mu.Unlock()
			time.Sleep(time.Duration(dummySleepNoElection) * time.Millisecond)
		} else /* (rf.state == followerState || rf.state == candidateState) && ttlElapsed */ {
			// Just Debug Prints
			if rf.state == follower {
				_, _ = DPrintf(newElection("[T%v -> T%v] %v: (%v -> %v) Heartbeat Timeout!"), rf.currentTerm, rf.currentTerm+1, rf.me, rf.state, candidate)
			} else if rf.state == candidate {
				_, _ = DPrintf(newElection("[T%v -> T%v] %v: (%v -> %v) Election Timeout!"), rf.currentTerm, rf.currentTerm+1, rf.me, rf.state, candidate)
			} else {
				_, _ = DPrintf(newElection("[T%v -> T%v] %v: (%v -> %v) WTF State?!"), rf.currentTerm, rf.currentTerm+1, rf.me, rf.state, candidate)
			}
			_, _ = DPrintf(vote("[T%v] %v: Voted for %v (Itself)"), rf.currentTerm+1, rf.me, rf.me)

			// Actual Work
			rf.currentTerm++     // 2. increments its current term
			rf.state = candidate // 1. transitions to candidate state
			rf.votedFor = rf.me  // 3. votes for itself
			rf.votesReceived = 1
			rf.resetTTL()

			// 4. issues RequestVote RPCs in parallel
			voteArg := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				// send initial empty AppendEntries RPCs (heartbeat) to each server;
				// repeat during idle periods to prevent election timeouts (§5.2)
				go rf.sendRequestAndProceed(i, voteArg)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendRequestAndProceed(peerNum int, voteArg *RequestVoteArgs) {
	voteReplay := RequestVoteReply{}
	ok := rf.sendRequestVote(peerNum, voteArg, &voteReplay)
	if !ok {
		_, _ = DPrintf(red("Network Error! No connection to Peer %v"), peerNum)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if voteReplay.Term > rf.currentTerm {
		rf.currentTerm = voteReplay.Term
		rf.state = follower
		rf.votedFor = -1
		rf.votesReceived = 0
		rf.resetTTL()
		return
	}

	if voteReplay.VoteGranted {
		rf.votesReceived++
		if rf.state == leader {
			return
		}
		// (a) it wins the election
		if rf.votesReceived >= (len(rf.peers)/2)+1 {
			_, _ = DPrintf(newLeader("[T%v] %v: New leaderState! (%v/%v votes) (%v -> %v)"), rf.currentTerm, rf.me, rf.votesReceived, len(rf.peers), rf.state, leader)
			rf.state = leader

			// Initialize all nextIndex values to the index just after the last one in its log
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
			}
			// send heartbeat messages to all of the other servers to establish its authority
			go rf.sendPeriodicHeartBeats()
		}
	}
}
