package pkg

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/Hamster601/Budd/config"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/pkg/transport"
)

type EtcdCli struct {
	serverId    uint32
	lockPath    string
	endPoints   []string
	client      *clientv3.Client
	session     *concurrency.Session
	mutex       *concurrency.Mutex
	certPath    string
	keyPath     string
	caPath      string
	dialTimeout int
}

func NewEtcd(c *config.Config) (*EtcdCli, error) {
	e := new(EtcdCli)
	e.serverId = c.MysqlSlave.ServerID
	e.lockPath = fmt.Sprintf("%s-%d", c.Etcd.LockPath, e.serverId)
	e.endPoints = c.Etcd.Endpoints
	e.certPath = c.Etcd.CertPath
	e.keyPath = c.Etcd.KeyPath
	e.caPath = c.Etcd.CaPath
	e.dialTimeout = c.Etcd.DialTimeout

	var err error
	var tlsConfig *tls.Config
	if c.Etcd.EnableTLS {
		tlsInfo := transport.TLSInfo{
			CertFile:      e.certPath,
			KeyFile:       e.keyPath,
			TrustedCAFile: e.caPath,
		}

		tlsConfig, err = tlsInfo.ClientConfig()
		if err != nil {
			log.Println("etcd init tls config failed, err:", err)
			return nil, err
		}
	}

	if e.client, err = clientv3.New(clientv3.Config{
		Endpoints:   e.endPoints,
		DialTimeout: time.Duration(e.dialTimeout) * time.Second,
		TLS:         tlsConfig,
	}); err != nil {
		log.Println("etcd create client failed, err:", err)
		return nil, err
	}

	if e.session, err = concurrency.NewSession(e.client); err != nil {
		log.Println("etcd create session failed, err:", err)
		return nil, err
	}

	e.mutex = concurrency.NewMutex(e.session, e.lockPath)

	return e, nil
}

func (e *EtcdCli) Lock() error {
	if err := e.mutex.Lock(context.TODO()); err != nil {
		log.Printf("etcd mutex failed, err:%s", err.Error())
		return err
	}

	return nil
}

func (e *EtcdCli) UnLock() error {
	if err := e.mutex.Unlock(context.TODO()); err != nil {
		log.Printf("etcd unlock failed, err:%s", err.Error())
		return err
	}

	return nil
}

func (e *EtcdCli) Close() {
	e.session.Close()
	e.client.Close()
}
