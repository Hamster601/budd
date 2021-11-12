package election

import (
	"context"
	"fmt"
	"github.com/Hamster601/Budd/config"
	"github.com/Hamster601/Budd/pkg/etcd"
	"github.com/Hamster601/Budd/pkg/logs"
	"github.com/Hamster601/Budd/pkg/storage"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"
	"log"
	"sync"
	"time"
)

const electionNodeTTL = 2 //秒

type etcdElection struct {
	once sync.Once

	informCh chan bool

	selected atomic.Bool
	ensured  atomic.Bool
	leader   atomic.String
	filePath string
}

func newEtcdElection(_informCh chan bool,filepath string) *etcdElection {
	return &etcdElection{
		informCh: _informCh,
		filePath: filepath,  // key 的根路径
	}
}

func (s *etcdElection) Elect() error {
	s.doElect()
	s.ensureFollower()
	return nil
}

func (s *etcdElection) doElect() {
	go func() {

		for {
			session, err := concurrency.NewSession(storage.EtcdConn(), concurrency.WithTTL(electionNodeTTL))
			if err != nil {
				logs.Error(err.Error())
				return
			}

			elc := concurrency.NewElection(session, s.filePath)
			ctx := context.Background()
			if err = elc.Campaign(ctx, config.CurrentNode()); err != nil {
				logs.Error(err.Error())
				session.Close()
				s.beFollower("")
				continue
			}

			select {
			case <-session.Done():
				s.beFollower("")
				continue
			default:
				s.beLeader()
				err = etcd.UpdateOrCreate(s.filePath, elc.Key(), storage.EtcdOps())
				if err != nil {
					logs.Error(err.Error())
					return
				}
			}

			shouldBreak := false
			for !shouldBreak {
				select {
				case <-session.Done():
					logs.Warn("etcd session has done")
					shouldBreak = true
					s.beFollower("")
					break
				case <-ctx.Done():
					ctxTmp, _ := context.WithTimeout(context.Background(), time.Second*electionNodeTTL)
					elc.Resign(ctxTmp)
					session.Close()
					s.beFollower("")
					return
				}
			}
		}
	}()
}

func (s *etcdElection) IsLeader() bool {
	return s.selected.Load()
}

func (s *etcdElection) Leader() string {
	return s.leader.Load()
}

func (s *etcdElection) ensureFollower() {
	go func() {
		for {
			if s.selected.Load() {
				break
			}

			k, _, err := etcd.Get(s.filePath, storage.EtcdOps())
			if err != nil {
				logs.Error(err.Error())
				continue
			}

			var l []byte
			l, _, err = etcd.Get(string(k), storage.EtcdOps())
			if err != nil {
				logs.Error(err.Error())
				continue
			}

			s.ensured.Store(true)
			s.beFollower(string(l))
			break
		}
	}()
}

func (s *etcdElection) Nodes() []string {
	var nodes []string
	ls, err := etcd.List("/transfer/myTransfer/election", storage.EtcdOps())
	if err == nil {
		for _, v := range ls {
			nodes = append(nodes, string(v.Value))
		}
	}
	return nodes
}

func (s *etcdElection) beLeader() {
	s.selected.Store(true)
	s.leader.Store(config.CurrentNode())
	s.informCh <- s.selected.Load()
	log.Println("the current node is the master")
}

func (s *etcdElection) beFollower(leader string) {
	s.selected.Store(false)
	s.informCh <- s.selected.Load()
	s.leader.Store(leader)
	log.Println(fmt.Sprintf("The current node is the follower, master node is : %s", s.leader.Load()))
}
