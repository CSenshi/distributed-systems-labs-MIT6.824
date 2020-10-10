package raft

import (
	"fmt"
	"log"
)

var (
	Vote        = Teal
	NewLeader   = Green
	NewElection = Red
	HeartBeat   = White
	NewFollower = Yellow
)

var (
	Black   = Color("\033[1;30m%s\033[0m")
	Red     = Color("\033[1;31m%s\033[0m")
	Green   = Color("\033[1;32m%s\033[0m")
	Yellow  = Color("\033[1;33m%s\033[0m")
	Purple  = Color("\033[1;34m%s\033[0m")
	Magenta = Color("\033[1;35m%s\033[0m")
	Teal    = Color("\033[1;36m%s\033[0m")
	White   = Color("\033[1;37m%s\033[0m")
)

// Turn off Coloring when writing into  file
const Coloring = 1

// Debugging
const Debug = 1

func Color(colorString string) func(...interface{}) string {
	if Coloring > 0 {
		return func(args ...interface{}) string {
			return fmt.Sprintf(colorString,
				fmt.Sprint(args...))
		}
	} else {
		return func(args ...interface{}) string {
			return fmt.Sprint(args...)
		}
	}
}

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

// Min and Max TTLs for election Default is (150, 300), for now we can use different TTLs
const (
	electionMinTTL   = 400
	electionRangeTTL = 200

	heartBeatInterval    = 200
	dummySleepNoElection = 50
)
