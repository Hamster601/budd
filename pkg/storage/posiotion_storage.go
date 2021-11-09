package storage

import (
	"github.com/Hamster601/Budd/config"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type PositionStorage interface {
	Initialize() error
	Save(pos mysql.Position) error
	Get() (mysql.Position, error)
}

func NewPositionStorage() PositionStorage {
	if config.InitConfig.IsCluster() {
		if config.InitConfig.IsEtcd() {
			return &etcdPositionStorage{}
		}
	}

	return &boltPositionStorage{}
}