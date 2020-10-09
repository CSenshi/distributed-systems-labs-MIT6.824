package raft

import "time"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// 	  least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID /* ToDO: Check for log */ {
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		_, _ = DPrintf(Vote("Peer %v Voted for %v"), rf.me, args.CandidateID)
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
		ttlElapsed := rf.electionStartTime.Before(time.Now().Add(-rf.electionTTL))

		if rf.state == Leader {
			// ToDo: Solve for Leader
		} else if !ttlElapsed /* && (rf.state == Follower || rf.state == Candidate) */ {
			// ToDO: Solve for Follower/Candidate
		} else /* (rf.state == Follower || rf.state == Candidate) && ttlElapsed */ {
			_, _ = DPrintf(NewElection("Term: %v, Peer %v (%v -> %v)"), rf.currentTerm, rf.me, rf.state, Candidate)

			rf.state = Candidate // 1. transitions to candidate state
			rf.currentTerm += 1  // 2. increments its current term
			rf.votedFor = rf.me  // 3. votes for itself
			_, _ = DPrintf(Vote("Peer %v Voted for %v (Itself)"), rf.me, rf.me)
			rf.resetTTL()

			// 4. issues RequestVote RPCs in parallel
			voteArg := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: -1, // ToDo: Change
				LastLogTerm:  -1, // ToDo: Change
			}

			rf.votesReceived = 1
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.sendRequestAndProceed(i, voteArg)
			}
		}
	}
}

func (rf *Raft) sendRequestAndProceed(peerNum int, voteArg *RequestVoteArgs) {
	voteReplay := RequestVoteReply{}
	rf.sendRequestVote(peerNum, voteArg, &voteReplay)
	if voteReplay.VoteGranted {
		rf.votesReceived++
		if rf.state == Leader {
			return
		}
		// (a) it wins the election
		if rf.votesReceived >= (len(rf.peers)/2)+1 {
			_, _ = DPrintf(NewLeader("New Leader = %v (%v/%v votes)"), rf.me, rf.votesReceived, len(rf.peers))
			rf.state = Leader
			// send heartbeat messages to all of the other servers to establish its authority
			// ToDo: send HeartBeats
		}
	}
}
