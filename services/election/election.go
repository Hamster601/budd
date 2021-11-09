package election

type Service interface {
	Elect() error
	IsLeader() bool
	Leader() string
	Nodes() []string
}

func NewElection(_informCh chan bool) Service {
	return newEtcdElection(_informCh,"")
}
