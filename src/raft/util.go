package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// State of Raft
type State int

const (
	Leader    State = iota
	Follower  State = iota
	Candidate State = iota
)

func (e State) String() string {
	switch e {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return fmt.Sprintf("Undefined State:%d", int(e))
	}
}
