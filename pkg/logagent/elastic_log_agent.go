package logagent

import "github.com/Hamster601/Budd/pkg/logs"

type ElsLoggerAgent struct {
}

func NewElsLoggerAgent() *ElsLoggerAgent {
	return &ElsLoggerAgent{}
}

func (s *ElsLoggerAgent) Printf(format string, v ...interface{}) {
	logs.Infof(format, v)
}

