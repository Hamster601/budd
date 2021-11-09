package services

import (
	"github.com/Hamster601/Budd/config"
	"github.com/Hamster601/Budd/metric"
	"github.com/siddontang/go-log/log"
)

type ClusterService struct {
	electionSignal chan bool //选举信号
}

func (s *ClusterService) boot() error {
	log.Println("start master election")
	err := _electionService.Elect()
	if err != nil {
		return err
	}

	s.startElectListener()

	return nil
}

func (s *ClusterService) startElectListener() {
	go func() {
		for {
			select {
			case selected := <-s.electionSignal:
				config.SetLeaderNode(_electionService.Leader())
				config.SetLeaderFlag(selected)
				if selected {
					metric.SetLeaderState(metric.LeaderState,true)
					_transferService.StartUp()
				} else {
					metric.SetLeaderState(metric.FollowerState,true)
					_transferService.stopDump()
				}
			}
		}

	}()
}

func (s *ClusterService) Nodes() []string {
	return _electionService.Nodes()
}
