package raft

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

var (
	vote           = teal
	newLeader      = green
	newElection    = red
	appendEntryLog = white
	newFollower    = yellow
	newLog         = purple
	newRaftServer  = green
)

var (
	black   = Color("\033[1;30m%s\033[0m")
	red     = Color("\033[1;31m%s\033[0m")
	green   = Color("\033[1;32m%s\033[0m")
	yellow  = Color("\033[1;33m%s\033[0m")
	purple  = Color("\033[1;34m%s\033[0m")
	magenta = Color("\033[1;35m%s\033[0m")
	teal    = Color("\033[1;36m%s\033[0m")
	white   = Color("\033[1;37m%s\033[0m")
)

const coloring = 0 // if coloring > 0 then: color output
const debug = 0    // if debug > 0 then: print debug logs

// Color function takes interface that sprintfs given string and colors
func Color(colorString string) func(...interface{}) string {
	if coloring > 0 {
		return func(args ...interface{}) string {
			return fmt.Sprintf(colorString,
				fmt.Sprint(args...))
		}
	}
	return func(args ...interface{}) string {
		return fmt.Sprint(args...)
	}
}

// DPrintf prints on stdout when Debug > 1
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// State of Raft can be one of 3 that are described below
type State int

const (
	leader    State = iota
	follower  State = iota
	candidate State = iota
	noVote          = -1 // Used for rf.VotedFor (when not yet voted)
)

func (e State) String() string {
	switch e {
	case leader:
		return "Leader"
	case follower:
		return "Follower"
	case candidate:
		return "Candidate"
	default:
		return fmt.Sprintf("Undefined State:%d", int(e))
	}
}

// Min and Max TTLs for election Default is (150, 300), for now we can use different TTLs
const (
	electionMinTTL   = 400
	electionRangeTTL = 200

	heartBeatInterval    = 100
	dummySleepNoElection = 100
)

func (rf *Raft) resetTTL() {
	rf.electionTTL = time.Millisecond * (time.Duration(electionMinTTL + rand.Intn(electionRangeTTL)))
	rf.electionStartTime = time.Now()
}

func (rf *Raft) convertToFollower(newTerm int) {
	if rf.state == follower {
		_, _ = DPrintf(newFollower("[T%v -> T%v] %v: Received Received Higher Term | Transition to new Term"), rf.currentTerm, newTerm, rf.me)
	} else {
		_, _ = DPrintf(newFollower("[T%v -> T%v] %v: Received Received Higher Term | Transition to new Term/State | %v -> %v"), rf.currentTerm, newTerm, rf.me, rf.state, follower)
	}
	rf.currentTerm = newTerm
	rf.votedFor = noVote
	rf.votesReceived = 0
	rf.state = follower
	rf.persist()
}

func (rf *Raft) updateIndices(index int, term int) {
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if term > rf.lastApplied {
		rf.lastApplied = term
	}
}

func (rf *Raft) toReal(trimmed int) int {
	return trimmed - rf.lastIncludedIndex - 1
}

func (rf *Raft) lastLogEntryTerm() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	}
	return rf.lastIncludedTerm
}

func (rf *Raft) lastLogEntryIndex() int {
	if len(rf.log) > 0 {
		return len(rf.log) + rf.lastIncludedIndex
	}
	return rf.lastIncludedIndex
}

func (rf *Raft) toTerm(trimmed int) int {
	if trimmed > rf.lastIncludedIndex {
		return rf.log[rf.toReal(trimmed)].Term
	}
	return rf.lastIncludedTerm
}
