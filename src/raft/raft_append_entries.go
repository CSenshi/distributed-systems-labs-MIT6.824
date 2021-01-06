package raft

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

	// Rule for all servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		_, _ = DPrintf(appendEntryLog("[T%v] %v: Received AppendEntry from %v | Reply False: Request term (%v) < (%v) Current Term "), rf.currentTerm, rf.me, args.LeaderID, args.Term, rf.currentTerm)
		return
	}

	if args.PrevLogIndex <= rf.lastIncludedIndex {
		reply.Success = true

		if args.PrevLogIndex+len(args.Entries) <= rf.lastIncludedIndex {
			return
		} else if rf.lastIncludedIndex-args.PrevLogIndex >= len(args.Entries) {
			return
		}

		i := rf.lastIncludedIndex - args.PrevLogIndex
		for ; i < len(args.Entries); i++ {
			// check: if we reached the end and have nothing to commit
			if rf.toReal(i+args.PrevLogIndex+1) >= len(rf.log) {
				break
			}

			// conflict occures
			if args.Entries[i].Term != rf.log[rf.toReal(i+args.PrevLogIndex+1)].Term {
				// delete the existing entry and all that follow it
				rf.log = rf.log[:rf.toReal(i+args.PrevLogIndex+1)]
				break
			}
		}
		args.Entries = args.Entries[i:]

		// 4. Append any new entries not already in the log
		for _, entry := range args.Entries {
			rf.log = append(rf.log, entry)
		}
		return
	}

	// 2. Reply false if log doesnt contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// lastLogIndex := rf.getLastLogIndex()
	// if lastLogIndex < args.PrevLogIndex {
	// 	_, _ = DPrintf(appendEntryLog("[T%v] %v: Received AppendEntry from %v | Reply False: Peer doesn't have log with index:%v  maxIndex:%v"), rf.currentTerm, rf.me, args.LeaderID, args.PrevLogIndex, len(rf.log)-1)
	// 	return
	// }
	// if rf.getLastLogTerm() != args.PrevLogTerm {
	// 	_, _ = DPrintf(appendEntryLog("[T%v] %v: Received AppendEntry from %v | Reply False: Log's term (%v) < (%v) Current Term "), rf.currentTerm, rf.me, args.LeaderID, args.Term, rf.currentTerm)
	// 	return
	// }

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	i := 0
	for ; i < len(args.Entries); i++ {
		// check 1: needed when invalid index is given
		if rf.toReal(args.PrevLogIndex+1+i) < 0 {
			break
		}
		// check 2: if we reached the end and have nothing to commit
		if args.PrevLogIndex+1+i >= rf.lastLogEntryIndex()+1 {
			break
		}

		// conflict occures
		if rf.log[rf.toReal(args.PrevLogIndex+1+i)].Term != args.Entries[i].Term {
			// delete the existing entry and all that follow it
			rf.log = rf.log[:rf.toReal(args.PrevLogIndex+1+i)]
			break
		}
	}
	args.Entries = args.Entries[i:]

	// 4. Append any new entries not already in the log
	for _, entry := range args.Entries {
		rf.log = append(rf.log, entry)
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if rf.lastLogEntryIndex() < args.LeaderCommit {
			rf.commitIndex = rf.lastLogEntryIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		go rf.applyCommittedLogEntries()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	rf.persist()
	rf.resetTTL()
	_, _ = DPrintf(appendEntryLog("[T%v] %v: Received AppendEntry from %v | Result: Success | Entries: %v"), rf.currentTerm, rf.me, args.LeaderID, args.Entries)
}

// Create Append Entries RPC Argument
func (rf *Raft) createAppendEntriesArgs(i int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: 0,   // Fill Below
		PrevLogTerm:  0,   // Fill Below
		Entries:      nil, // Fill Below
		LeaderCommit: rf.commitIndex,
	}
	args.PrevLogIndex = rf.matchIndex[i]
	args.PrevLogTerm = rf.toTerm(args.PrevLogIndex)
	args.Entries = rf.log[rf.toReal(args.PrevLogIndex)+1:]
	return args
}

// code to send a AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		rf.mu.Lock()
		_, _ = DPrintf(red("[T%v] %v: Network Error! AppendEntries: No connection to Peer %v"), rf.currentTerm, rf.me, server)
		rf.mu.Unlock()
		return false
	}

	// Evaluate RPC Result
	rf.processAppendEntriesReply(server, *args, reply)
	return true
}

// Process Append Entries RPC Reply
func (rf *Raft) processAppendEntriesReply(i int, args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule for all servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
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

	rf.commitLogEntries()
}
