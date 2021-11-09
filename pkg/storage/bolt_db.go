package storage

import (
	"errors"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/vmihailenco/msgpack"
	"go.etcd.io/bbolt"
)

type boltPositionStorage struct {
	Name string
	Pos  uint32
}

func (s *boltPositionStorage) Initialize() error {
	return bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(positionBucket)
		data := bt.Get(fixPositionId)
		if data != nil {
			return nil
		}

		bytes, err := msgpack.Marshal(mysql.Position{})
		if err != nil {
			return err
		}
		return bt.Put(fixPositionId, bytes)
	})
}

func (s *boltPositionStorage) Save(pos mysql.Position) error {
	return bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(positionBucket)
		data, err := msgpack.Marshal(pos)
		if err != nil {
			return err
		}
		return bt.Put(fixPositionId, data)
	})
}

func (s *boltPositionStorage) Get() (mysql.Position, error) {
	var entity mysql.Position
	err := bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(positionBucket)
		data := bt.Get(fixPositionId)
		if data == nil {
			return errors.New("Not found PositionStorage")
		}
		return msgpack.Unmarshal(data, &entity)
	})

	return entity, err
}
