package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

//
// ApplyMsg - as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry Struct to contain each log that server received
type LogEntry struct {
	Command interface{} // State Machine Command
	Term    int         // Term when the entry was received by the leade
}

func (entry LogEntry) String() string {
	command := fmt.Sprintf("%v", entry.Command)
	if len(command) > 20 {
		command = command[:10] + "..." + command[len(command)-10:]
	}
	return fmt.Sprintf("{Command: %v, Term: %v}", command, entry.Term)
}

//
//Raft - A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state             State         // State of raft (leader, follower, candidate)
	electionTTL       time.Duration // ttl timer
	electionStartTime time.Time     // time when ttl reset happened
	votesReceived     int           // when current peer is candidate this int is tracking number of votes received
	applyChan         chan ApplyMsg // channel where all client's result should be returned

	/* Taken From Frame 2 https://raft.github.io/raft.pdf */
	// Persistent state on all servers (Updated on stable storage before responding to RPCs)
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateID that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term = rf.currentTerm
	var isLeader = rf.state == leader

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// Start - the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index, term, isLeader := len(rf.log), rf.currentTerm, rf.state == leader
	if isLeader {
		logEntry := LogEntry{Command: command, Term: rf.currentTerm}

		_, _ = DPrintf(newLog("[T%v] %v: Received new log! #%v %+v "), rf.currentTerm, rf.me, len(rf.log), logEntry)
		rf.log = append(rf.log, logEntry)
		rf.persist()
	} else {
		//_, _ = DPrintf(NewLog("[T%v] %v: Received new log! {%v}, but not leader! "), rf.currentTerm, rf.currentTerm+1, command)
	}

	return index, term, isLeader
}

// Commit entries into the apply channel
func (rf *Raft) commitLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// checking for 0th (nil command should not be aplied)
	if len(rf.log) < 2 {
		return
	}

	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		_, _ = DPrintf(newLog("[T%v] %v: Committing Log #%v %v"), rf.currentTerm, rf.me, i, rf.log[i])
		rf.applyChan <- ApplyMsg{CommandIndex: i, CommandValid: true, Command: rf.log[i].Command}
	}
	rf.lastApplied = rf.commitIndex
}

//
// Kill - the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	_, _ = DPrintf(red("[T%v] %v: Server Killed!!! "), rf.currentTerm, rf.me)
	atomic.StoreInt32(&rf.dead, 1)

	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetTTL() {
	rf.electionTTL = time.Millisecond * (time.Duration(electionMinTTL + rand.Intn(electionRangeTTL)))
	rf.electionStartTime = time.Now()
}

//
// Make - the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano())
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = follower // all raft servers should start as followers
	rf.electionTTL = time.Millisecond * (time.Duration(electionMinTTL + rand.Intn(electionRangeTTL)))
	rf.electionStartTime = time.Now()
	rf.applyChan = applyCh

	/* Persistent state on all servers */
	rf.currentTerm = 0
	rf.votedFor = noVote
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Command: nil, Term: 0}) // Append 0th nil command

	/* Volatile state on all servers */
	rf.commitIndex = 0
	rf.lastApplied = 0

	/* Volatile state on leaders */
	rf.nextIndex = nil
	rf.matchIndex = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	_, _ = DPrintf(newRaftServer("[T%v] %v: Initialized New Raft Server! Persistent Storage: {currentTerm: %v, votedFor: %v, len(log): %v}"), rf.currentTerm, rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	go rf.leaderElection()

	return rf
}
