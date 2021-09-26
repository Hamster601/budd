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

func newEtcdCli(config2 *config.Config) *EtcdCli {
	lockKey := fmt.Sprintf("%s-%d",config2.Etcd.LockPath,config2.MysqlSlave.ServerID)
	return &EtcdCli{
		serverId:    config2.MysqlSlave.ServerID,
		lockPath:    lockKey,
		endPoints:   config2.Etcd.Endpoints,
		certPath:    config2.Etcd.CertPath,
		keyPath:     config2.Etcd.KeyPath,
		caPath:      config2.Etcd.CaPath,
		dialTimeout: config2.Etcd.DialTimeout,
	}
}

func NewEtcd(c *config.Config) (etcdCli *EtcdCli, err error) {
	etcdCli = newEtcdCli(c)

	var tlsConfig *tls.Config

	if c.Etcd.EnableTLS {
		tlsInfo := transport.TLSInfo{
			CertFile:      etcdCli.certPath,
			KeyFile:       etcdCli.keyPath,
			TrustedCAFile: etcdCli.caPath,
		}
		tlsConfig, err = tlsInfo.ClientConfig()
		if err != nil {
			log.Println("etcd init tls config failed, err:", err)
			return nil, err
		}
	}

	clientV3Cfg := clientv3.Config{
		Endpoints:   etcdCli.endPoints,
		DialTimeout: time.Duration(etcdCli.dialTimeout) * time.Second,
		TLS:         tlsConfig,
	}

	if etcdCli.client, err = clientv3.New(clientV3Cfg); err != nil {
		log.Println("etcd create client failed, err:", err)
		return nil, err
	}

	if etcdCli.session, err = concurrency.NewSession(etcdCli.client); err != nil {
		log.Println("etcd create session failed, err:", err)
		return nil, err
	}

	etcdCli.mutex = concurrency.NewMutex(etcdCli.session, etcdCli.lockPath)

	return etcdCli, nil
}

func (e *EtcdCli) Lock() error {
    return e.mutex.Lock(context.TODO())
}

func (e *EtcdCli) UnLock() error {
	return e.mutex.Unlock(context.TODO())
}

func (e *EtcdCli) Close() {
	e.session.Close()
	e.client.Close()
}
