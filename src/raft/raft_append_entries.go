package raft

import "time"

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
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
		_, _ = DPrintf(HeartBeat("[T%v] %v: Received heartBeat from %v | Result: Request term (%v) > (%v) Current Term "), rf.currentTerm, rf.me, args.LeaderId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		_, _ = DPrintf(HeartBeat("[T%v] %v: Received heartBeat from %v | Reply False: Request term (%v) < (%v) Current Term "), rf.currentTerm, rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)

	// 4. Append any new entries not already in the log
	if rf.state != Follower {
		_, _ = DPrintf(NewFollower("[T%v] %v:  %v -> %v"), rf.currentTerm, rf.me, rf.state, Follower)
		rf.state = Follower
	}

	rf.votedFor = -1
	rf.votesReceived = 0

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	reply.Success = true
	_, _ = DPrintf(HeartBeat("[T%v] %v: Received heartBeat from %v | Result: Success! "), rf.currentTerm, rf.me, args.LeaderId)
	rf.resetTTL()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeats() {
	for {
		rf.mu.Lock()
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: 0,
		}
		rf.mu.Unlock()
		if rf.state != Leader {
			return
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			reply := &AppendEntriesReply{}

			go rf.sendAppendEntries(i, args, reply)
		}
		time.Sleep(heartBeatInterval * time.Millisecond)
	}
}
