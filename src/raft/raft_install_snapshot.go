package raft

// InstallSnapshotArgs according to figure 13 but without 'done' and 'offset'
//
// Hint: Send the entire snapshot in a single InstallSnapshot RPC. Don't implement Figure
// 		 13's offset mechanism for splitting up the snapshot.
type InstallSnapshotArgs struct {
	Term              int    //leader’s term
	LeaderID          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

// InstallSnapshotReply according to figure 13
type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// InstallSnapshot Invoked by leader to send chunks of a snapshot to a follower. Leaders always send chunks in order.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule for all servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// Reply immediately if recepient is ahead
	rf.resetTTL()
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	lastEntryIndex := rf.toReal(args.LastIncludedIndex)
	prefixLogs := 0 <= lastEntryIndex && lastEntryIndex < len(rf.log) && rf.log[lastEntryIndex].Term == args.LastIncludedTerm
	if prefixLogs {
		// by retransmission or by mistake: received prefix log entries -> retain logs
		rf.log = rf.log[lastEntryIndex+1:]
	} else {
		// new information not already in the recipient’s log: discard the entire log
		rf.log = []LogEntry{}
	}

	// Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.lastIncludedTerm, rf.lastIncludedIndex = args.LastIncludedTerm, args.LastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.enocdeRaftState(), args.Data)
	rf.updateIndices(args.LastIncludedTerm, args.LastIncludedIndex)

	// Send snapshot back for kvraft server to decode it and update kv storage
	if rf.lastApplied <= rf.lastIncludedIndex {
		rf.applyChan <- ApplyMsg{CommandValid: false, Command: args.Data}
	}
}

// Create Append Entries RPC Argument
func (rf *Raft) createInstallSnapshotArgsArgs(i int) *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
}

// code to send a InstallSnapshot RPC to a server.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		rf.mu.Lock()
		_, _ = DPrintf(red("[T%v] %v: Network Error! InstallSnapshot: No connection to Peer %v"), rf.currentTerm, rf.me, server)
		rf.mu.Unlock()
		return false
	}

	// Evaluate RPC Result
	rf.processInstallSnapshotReply(server, *args, reply)
	return true
}

// Process Append Entries RPC Reply
func (rf *Raft) processInstallSnapshotReply(i int, args InstallSnapshotArgs, reply *InstallSnapshotReply) {
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

	// If successful: update nextIndex and matchIndex for follower (§5.3)
	if rf.matchIndex[i] < args.LastIncludedIndex {
		rf.matchIndex[i] = args.LastIncludedIndex
	}
	rf.nextIndex[i] = rf.matchIndex[i] + 1

	rf.commitLogEntries()
}
