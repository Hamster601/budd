package storage

import (
	"errors"
	"fmt"
	"github.com/Hamster601/Budd/config"
	"github.com/Hamster601/Budd/pkg/byteutil"
	"github.com/Hamster601/Budd/pkg/file"
	"github.com/Hamster601/Budd/pkg/logagent"
	"go.etcd.io/bbolt"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.etcd.io/etcd/client/v3"
	"path/filepath"
	"strings"
	"time"
)

const (
	boltFilePath = "db"
	boltFileName = "data.db"
	boltFileMode = 0600
)

var (
	positionBucket = []byte("Position")
	fixPositionId  = byteutil.Uint64ToBytes(uint64(1))

	bolt *bbolt.DB

	etcdConn *clientv3.Client
	etcdOps  clientv3.KV
)

func Initialize(configfile string) error {
	if err := initBolt(configfile); err != nil {
		return err
	}

	config, err := config.NewConfig(configfile)
	if err != nil {
		return err
	}
	if config.IsEtcd() {
		if err := initEtcd(configfile); err != nil {
			return err
		}
	}

	return nil
}

func initBolt(configfile string) error {
	dataDir, err := config.NewConfig(configfile)
	if err != nil {
		return err
	}
	blotStorePath := filepath.Join((*dataDir).DataDir, boltFilePath)
	if err1 := file.MkdirIfNecessary(blotStorePath); err != nil {
		return errors.New(fmt.Sprintf("create boltdb store : %s", err1.Error()))
	}

	boltFilePath := filepath.Join(blotStorePath, boltFileName)
	bolt, err := bbolt.Open(boltFilePath, boltFileMode, bbolt.DefaultOptions)
	if err != nil {
		return errors.New(fmt.Sprintf("open boltdb: %s", err.Error()))
	}

	err = bolt.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(positionBucket)
		return nil
	})

	bolt = bolt

	return err
}

func initEtcd(configfile string) error {
	logutil.DefaultZapLoggerConfig = logagent.EtcdZapLoggerConfig()

	cfg, err := config.NewConfig(configfile)
	list := strings.Split((*cfg).EtcdConfig.EtcdAddrs, ",")
	config := clientv3.Config{
		Endpoints:   list,
		Username:    (*cfg).EtcdConfig.EtcdUser,
		Password:    (*cfg).EtcdConfig.EtcdPassword,
		DialTimeout: 1 * time.Second,
	}

	client, err := clientv3.New(config)
	if err != nil {
		return err
	}
	etcdConn = client
	etcdOps = clientv3.NewKV(etcdConn)

	return nil
}

func EtcdConn() *clientv3.Client {
	return etcdConn
}

func EtcdOps() clientv3.KV {
	return etcdOps
}

func Close() {
	if bolt != nil {
		bolt.Close()
	}
	if etcdConn != nil {
		etcdConn.Close()
	}
}
