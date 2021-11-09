package storage

import "github.com/Hamster601/Budd/config"

type ElectionStorage interface {
	Elect() error
}

func NewElectionStorage(conf *config.Config) PositionStorage {

	if conf.IsEtcd() {

	}

	return nil
}
