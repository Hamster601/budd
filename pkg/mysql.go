package pkg

import (
	"database/sql"
	"sync"
)

type dbInfo struct {
	sync.RWMutex

	db       *sql.DB
	serverId uint32
	schema   string
	table    string
	name     string
	pos      uint32
}

type reflectFunc struct {
	b *Bin2es
}
