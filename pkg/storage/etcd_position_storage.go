package storage

import (
	"encoding/json"
	"github.com/Hamster601/Budd/config"
	"github.com/Hamster601/Budd/pkg/etcd"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type etcdPositionStorage struct {
}

func NewetcdPositionStorage( string) *etcdPositionStorage {
	return &etcdPositionStorage{}
}

func (s *etcdPositionStorage) Initialize() error {
	data, err := json.Marshal(mysql.Position{})
	if err != nil {
		return err
	}

	err = etcd.CreateIfNecessary(config.InitConfig.EtcdConfig.EtcdFilePath, string(data), nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *etcdPositionStorage) Save(pos mysql.Position) error {
	data, err := json.Marshal(pos)
	if err != nil {
		return err
	}

	return etcd.Save(config.InitConfig.EtcdConfig.EtcdFilePath, string(data), nil)
}

func (s *etcdPositionStorage) Get() (mysql.Position, error) {
	var entity mysql.Position

	data, _, err := etcd.Get(config.InitConfig.EtcdConfig.EtcdFilePath, nil)
	if err != nil {
		return entity, err
	}

	err = json.Unmarshal(data, &entity)

	return entity, err
}
