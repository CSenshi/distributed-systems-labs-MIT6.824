package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry //log Entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	// 0. If RPC received from new leader: convert to follower
	if args.Term > rf.currentTerm {
		_, _ = DPrintf(AppendEntryLog("[T%v -> T%v] %v: Received AppendEntry from %v | Result: Request term > Current Term"), rf.currentTerm, args.Term, rf.me, args.LeaderID)
		rf.currentTerm = args.Term
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		_, _ = DPrintf(AppendEntryLog("[T%v] %v: Received AppendEntry from %v | Reply False: Request term (%v) < (%v) Current Term "), rf.currentTerm, rf.me, args.LeaderID, args.Term, rf.currentTerm)
		return
	}

	// 2. Reply false if log doesnt contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	lastLogIndex := len(rf.log) - 1
	if lastLogIndex < args.PrevLogIndex {
		_, _ = DPrintf(AppendEntryLog("[T%v] %v: Received AppendEntry from %v | Reply False: Peer doesn't have log with index:%v  maxIndex:%v"), rf.currentTerm, rf.me, args.LeaderID, args.PrevLogIndex, len(rf.log)-1)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		_, _ = DPrintf(AppendEntryLog("[T%v] %v: Received AppendEntry from %v | Reply False: Log's term (%v) < (%v) Current Term "), rf.currentTerm, rf.me, args.LeaderID, args.Term, rf.currentTerm)
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	rf.log = rf.log[:args.PrevLogIndex+1]

	// 4. Append any new entries not already in the log
	for i, entry := range args.Entries {
		_, _ = DPrintf(AppendEntryLog("[T%v] %v: ________ Appending log %v: %+v "), rf.currentTerm, rf.me, i, entry)
		rf.log = append(rf.log, entry)
	}

	// Here comes only followers
	if rf.state != Follower {
		_, _ = DPrintf(NewFollower("[T%v] %v: %v -> %v"), rf.currentTerm, rf.me, rf.state, Follower)
		rf.state = Follower
	}
	rf.votedFor = -1
	rf.votesReceived = 0

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
	_, _ = DPrintf(AppendEntryLog("[T%v] %v: Received AppendEntry from %v | Result: Success! "), rf.currentTerm, rf.me, args.LeaderID)
	rf.resetTTL()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendPeriodicHeartBeats() {
	for {
		oneHeartBeatsCycle := func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.killed() {
				return
			}
			if rf.state != Leader {
				return
			}
			go func() {
				for i := range rf.peers {
					if i == rf.me {
						continue
					}

					// Prepare RPC Arg/Reply
					args := rf.createAppendEntriesArgs(i)
					reply := &AppendEntriesReply{}

					// RPC Send Request
					ok := rf.sendAppendEntries(i, args, reply)
					if !ok {
						_, _ = DPrintf(Red("Network Error! No connection to Peer %v"), i)
						return
					}

					// Evaluate RPC Result
					rf.processAppendEntriesReply(i, args, reply)
				}
			}()
		}
		oneHeartBeatsCycle()
		time.Sleep(heartBeatInterval * time.Millisecond)
	}
}

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
	args.PrevLogIndex = rf.nextIndex[i] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	args.Entries = rf.log[rf.nextIndex[i]:]
	return args
}

func (rf *Raft) processAppendEntriesReply(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !reply.Success {
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		// ToDo: Can be optimised (Get index from followers)
		rf.nextIndex[i]--
	} else {
		// If successful: update nextIndex and matchIndex for follower
		rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[i] = rf.matchIndex[i] + 1
	}

	// 	If there exists an N such that
	//		1. N > commitIndex,
	//		2. a majority of matchIndex[i] ≥ N
	//  	3. log[N].term == currentTerm:
	// 	set commitIndex = N
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- { // Check 1: Iterate until commitIndex
		// Check 3: Term
		if rf.log[N].Term != rf.currentTerm {
			continue
		}

		// Check 2:  majority
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

func (rf *Raft) commitLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(rf.log) < 2 {
		return
	}

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		_, _ = DPrintf(NewLog("[T%v] %v: Committing Log #%v %v"), rf.currentTerm, rf.me, i, rf.log[i])
		rf.applyChan <- ApplyMsg{CommandIndex: i, CommandValid: true, Command: rf.log[i].Command}
	}
	rf.lastApplied = rf.commitIndex
}
