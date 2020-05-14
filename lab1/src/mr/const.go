package mr

// This file defines some constants

// Task State
const (
	Idle int = iota
	Working
	Finished
)

// Task Types
const (
	Map int = iota
	Reduce
	NoTask
)
