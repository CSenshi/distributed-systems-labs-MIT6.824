package raft

import (
	"time"
)

// AppendEntriesArgs struct for AppendEntries RPC call Argument
type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry //log Entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

// AppendEntriesReply struct for AppendEntries RPC call Reply
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

// AppendEntries RPC call on client Side
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	// (All Servers): If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		if rf.state == follower {
			_, _ = DPrintf(appendEntryLog("[T%v -> T%v] %v: Received AppendEntry from %v | Transition to new Term"), rf.currentTerm, args.Term, rf.me, args.LeaderID)
		} else {
			_, _ = DPrintf(newFollower("[T%v -> T%v] %v: Received AppendEntry from %v | Transition to new Term/State |  %v -> %v"), rf.currentTerm, args.Term, rf.me, args.LeaderID, rf.state, follower)
			rf.state = follower
		}
		rf.currentTerm = args.Term
		rf.persist()
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		_, _ = DPrintf(appendEntryLog("[T%v] %v: Received AppendEntry from %v | Reply False: Request term (%v) < (%v) Current Term "), rf.currentTerm, rf.me, args.LeaderID, args.Term, rf.currentTerm)
		return
	}

	// 2. Reply false if log doesnt contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	lastLogIndex := len(rf.log) - 1
	if lastLogIndex < args.PrevLogIndex {
		_, _ = DPrintf(appendEntryLog("[T%v] %v: Received AppendEntry from %v | Reply False: Peer doesn't have log with index:%v  maxIndex:%v"), rf.currentTerm, rf.me, args.LeaderID, args.PrevLogIndex, len(rf.log)-1)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		_, _ = DPrintf(appendEntryLog("[T%v] %v: Received AppendEntry from %v | Reply False: Log's term (%v) < (%v) Current Term "), rf.currentTerm, rf.me, args.LeaderID, args.Term, rf.currentTerm)
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	rf.log = rf.log[:args.PrevLogIndex+1]

	// 4. Append any new entries not already in the log
	for _, entry := range args.Entries {
		rf.log = append(rf.log, entry)
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if len(rf.log)-1 < args.LeaderCommit {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		go rf.commitLogEntries()
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.persist()
	_, _ = DPrintf(appendEntryLog("[T%v] %v: Received AppendEntry from %v | Result: Success | Entries: %v"), rf.currentTerm, rf.me, args.LeaderID, args.Entries)
	rf.resetTTL()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendPeriodicHeartBeats() {
	for {
		// Check if server is killed
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// send AppendEntries RPCs (heartbeat) to each server
		success := rf.oneHeartBeatsCycle()
		if !success {
			return
		}

		// repeat during idle periods to prevent election timeouts (§5.2)
		time.Sleep(heartBeatInterval * time.Millisecond)
	}
}

func (rf *Raft) oneHeartBeatsCycle() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return false
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peer int) {
			// Prepare RPC Arg/Reply
			args := rf.createAppendEntriesArgs(peer)
			reply := &AppendEntriesReply{}

			// RPC Send Request
			ok := rf.sendAppendEntries(peer, args, reply)
			if !ok {
				rf.mu.Lock()
				_, _ = DPrintf(red("[T%v] %v: Network Error! AppendEntries: No connection to Peer %v"), rf.currentTerm, rf.me, peer)
				rf.mu.Unlock()
				return
			}

			// Evaluate RPC Result
			rf.processAppendEntriesReply(peer, *args, reply)
		}(i)
	}
	return true
}

// Create Append Entries RPC Argument
func (rf *Raft) createAppendEntriesArgs(i int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: 0,   // Fill Below
		PrevLogTerm:  0,   // Fill Below
		Entries:      nil, // Fill Below
		LeaderCommit: rf.commitIndex,
	}
	args.PrevLogIndex = rf.matchIndex[i]
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	args.Entries = rf.log[args.PrevLogIndex+1:]
	return args
}

// Process Append Entries RPC Reply
func (rf *Raft) processAppendEntriesReply(i int, args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		if rf.currentTerm != reply.Term {
			_, _ = DPrintf(newFollower("[T%v -> T%v] %v: Change State: %v -> %v"), rf.currentTerm, reply.Term, rf.me, rf.state, follower)
		} else {
			_, _ = DPrintf(newFollower("[T%v] %v: Change State: %v -> %v"), rf.currentTerm, rf.me, rf.state, follower)
		}
		rf.currentTerm = reply.Term
		rf.state = follower
		rf.votedFor = noVote
		rf.votesReceived = 0
	}

	// Term confusion
	if args.Term != rf.currentTerm || rf.state != leader {
		return
	}

	// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
	if !reply.Success {
		rf.nextIndex[i]--
		return
	}

	// If successful: update nextIndex and matchIndex for follower (§5.3)
	rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
	rf.nextIndex[i] = rf.matchIndex[i] + 1

	// 	If there exists an N such that
	//		1. N > commitIndex,
	//		2. a majority of matchIndex[i] ≥ N
	//  	3. log[N].term == currentTerm:
	// 	set commitIndex = N (§5.3, §5.4)
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- { // Iterate until commitIndex
		// Check for Term
		if rf.log[N].Term != rf.currentTerm {
			continue
		}

		// Check for  majority
		count := 1 // Self is already in the match index
		for j := range rf.peers {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] < N {
				continue
			}

			count++
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				go rf.commitLogEntries()
				break
			}
		}
	}
}
