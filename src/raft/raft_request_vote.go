package raft

import (
	"time"
)

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

	// (All Servers): If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		if rf.state == follower {
			_, _ = DPrintf(vote("[T%v -> T%v] %v: Received RequestVote from %v | Transition to new Term | Received Higher Term"), rf.currentTerm, args.Term, rf.me, args.CandidateID)
		} else {
			_, _ = DPrintf(vote("[T%v -> T%v] %v: Received RequestVote from %v | Transition to new Term/State | %v -> %v | Received Higher Term"), rf.currentTerm, args.Term, rf.me, args.CandidateID, rf.state, follower)
		}
		rf.votedFor = noVote
		rf.currentTerm = args.Term
		rf.state = follower
		rf.persist()
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
	lastLogIndex, lastLogTerm := len(rf.log)-1, rf.log[len(rf.log)-1].Term
	logCheck := lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)

	// Both checks should be true to grant vote
	if voteCheck && logCheck {
		reply.VoteGranted = true
		_, _ = DPrintf(vote("[T%v] %v: Received RequestVote from %v | Vote Successful"), rf.currentTerm, rf.me, args.CandidateID)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.persist()
	} else if !voteCheck {
		_, _ = DPrintf(vote("[T%v] %v: Received RequestVote from %v | Vote Failure | Already voted for %v"), rf.currentTerm, rf.me, args.CandidateID, rf.votedFor)
	} else {
		_, _ = DPrintf(vote("[T%v] %v: Received RequestVote from %v | Vote Failure | No Up-To-Date Log | Received {LastLogTerm: %v, LastLogIndex: %v} | Current {LastLogTerm: %v, LastLogIndex: %v}"),
			rf.currentTerm, rf.me, args.CandidateID, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
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
		} else if !ttlElapsed /* && (rf.state == follower || rf.state == candidate) */ {
			rf.mu.Unlock()
			time.Sleep(time.Duration(dummySleepNoElection) * time.Millisecond)
		} else /* (rf.state == follower || rf.state == candidate) && ttlElapsed */ {
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
			rf.currentTerm++     // 1. increments its current term (§5.1)
			rf.state = candidate // 2. transitions to candidate state (§5.1)
			rf.votedFor = rf.me  // 3. votes for itself (§5.1)
			rf.votesReceived = 1
			rf.persist()
			rf.resetTTL()

			// 4. issues RequestVote RPCs in parallel (§5.1)

			// Prepare RPC Arg
			args := rf.createRequestVoteArgs()
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(peer int, args *RequestVoteArgs) {
					// Prepare RPC Reply
					reply := &RequestVoteReply{}

					// RPC Send Request
					ok := rf.sendRequestVote(peer, args, reply)
					if !ok {
						rf.mu.Lock()
						_, _ = DPrintf(red("[T%v] %v: Network Error! RequestVote: No connection to Peer %v"), rf.currentTerm, rf.me, peer)
						rf.mu.Unlock()
						return
					}

					// Evaluate RPC Result
					rf.processRequestVoteReply(peer, args, reply)
				}(i, args)
			}
			rf.mu.Unlock()
		}
	}
}

// Create Request Vote RPC Argument
func (rf *Raft) createRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
}

// Process Request Vote RPC Reply
func (rf *Raft) processRequestVoteReply(peerNum int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		if rf.currentTerm != reply.Term {
			_, _ = DPrintf(newFollower("[T%v -> T%v] %v: Change State: %v -> %v"), rf.currentTerm, reply.Term, rf.me, rf.state, follower)
		} else {
			_, _ = DPrintf(newFollower("[T%v] %v: Change State: %v -> %v"), rf.currentTerm, rf.me, rf.state, follower)
		}
		_, _ = DPrintf(newFollower("[T%v] %v: Change State: %v -> %v"), rf.currentTerm, rf.me, rf.state, follower)
		rf.currentTerm = reply.Term
		rf.state = follower
		rf.votedFor = noVote
		rf.votesReceived = 0
		rf.persist()
		rf.resetTTL()
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
