package raft

import (
	"fmt"
	"log"
)

var (
	vote           = teal
	newLeader      = green
	newElection    = red
	appendEntryLog = white
	newFollower    = yellow
	newLog         = purple
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

const coloring = 1 // if coloring > 0 then: color output
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
)

func (e State) String() string {
	switch e {
	case leader:
		return "leaderState"
	case follower:
		return "followerState"
	case candidate:
		return "candidateState"
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
