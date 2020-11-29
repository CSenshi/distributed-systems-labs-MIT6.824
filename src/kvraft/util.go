package kvraft

import (
	"fmt"
	"log"
)

var (
	newServer  = green
	serverDied = red
	networkErr = red
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

func less(s string) string {
	if len(s) > 30 {
		s = s[:15] + " .... " + s[len(s)-15:]
	}
	return s
}
