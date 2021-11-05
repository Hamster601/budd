package metric

import "go.uber.org/atomic"

const (
	DestStateOK   = 1
	DestStateFail = 0

	LeaderState   = 1
	FollowerState = 0
)

var (
	leaderState  atomic.Bool
	destState    atomic.Bool
	delay        atomic.Uint32
	insertRecord map[string]*atomic.Uint64
	updateRecord map[string]*atomic.Uint64
	deleteRecord map[string]*atomic.Uint64
)