package mr

import (
	"fmt"
	"log"
	"time"
)

var (
	makeMasterRequest = green
	fail              = red
	newTask           = magenta
	newPhase          = green
)

var (
	red     = Color("\033[1;31m%s\033[0m")
	green   = Color("\033[1;32m%s\033[0m")
	magenta = Color("\033[1;35m%s\033[0m")
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

// State of Map/Reduce Tasks
type State int

const (
	idle       State = iota
	inProgress State = iota
	completed  State = iota
)

func (e State) String() string {
	switch e {
	case idle:
		return "Idle"
	case inProgress:
		return "In Progress"
	case completed:
		return "Completed"
	default:
		return fmt.Sprintf("Undefined State: %d", int(e))
	}
}

// TaskType of RequestTask Reply
type TaskType int

const (
	mapTask  TaskType = iota
	redTask  TaskType = iota
	waitTask TaskType = iota
	nop      TaskType = iota
)

func (e TaskType) String() string {
	switch e {
	case mapTask:
		return "Map Task"
	case redTask:
		return "Reduce Task"
	case waitTask:
		return "Wait Task"
	case nop:
		return "No Operation"
	default:
		return fmt.Sprintf("Undefined Task: %d", int(e))
	}
}

const requestTaskTTL = 50 * time.Millisecond
const workerTTL = 10 * time.Second
