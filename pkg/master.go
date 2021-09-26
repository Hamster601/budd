package pkg

import (
	//系统
	"fmt"
	"log"
	"github.com/Hamster601/Budd/config"

	//第三方
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/mysql"
)

func loadMasterInfo(serverId uint32, masterInfo config.MasterInfo) (*dbInfo, error) {
	var m dbInfo

	schema := masterInfo.Schema
	table := masterInfo.Table

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=%s", masterInfo.User, masterInfo.Pwd,
		masterInfo.Addr, masterInfo.Port, masterInfo.Charset)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("sql Open error: %s", err)
		return nil, err
	}
	if err = db.Ping(); err != nil {
		log.Printf("db ping error: %s", err)
		return nil, err
	}

	//若没有schema:bin2es, 则创建
	dbSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", schema)
	if _, err := db.Exec(dbSQL); err != nil {
		log.Printf("create database %s failed, err:%s", schema, err)
		return nil, err
	}

	//若没有table:master_info, 则创建
	tblSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			server_id int NOT NULL,
			bin_name  varchar(256) NOT NULL DEFAULT '',
			bin_pos   bigint NOT NULL  DEFAULT 0,
			PRIMARY KEY (server_id)
		)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='binlog位置表'
	`, schema, table)
	if _, err := db.Exec(tblSQL); err != nil {
		log.Printf("create table %s failed, err:%s", table, err)
		return nil, err
	}

	//读取master_info的`bin_name` `bin_pos`
	var name string
	var pos uint32
	querySQL := fmt.Sprintf("SELECT bin_name, bin_pos FROM %s.%s WHERE server_id = %d", schema, table, serverId)
	rows, err := db.Query(querySQL)
	if err != nil {
		log.Printf("query %s.%s failed, err:%s", schema, table, err)
		return nil, err
	}
	defer rows.Close()

	var num = 0
	for rows.Next() {
		num += 1
		err = rows.Scan(&name, &pos)
		if err != nil {
			log.Printf("iteration %s.%s failed, err:%s", schema, table, err)
			return nil, err
		}
		log.Printf("bin_name:%s bin_pos:%d", name, pos)
	}
	err = rows.Err()
	if err != nil {
		log.Printf("rows.Err:%s", err)
		return nil, err
	}

	if num == 0 {
		insertSQL := fmt.Sprintf("INSERT INTO %s.%s (server_id, bin_name, bin_pos) VALUES (%d, %s, %d)", schema, table, serverId, "''", 0)
		log.Println(insertSQL)
		if _, err := db.Exec(insertSQL); err != nil {
			log.Printf("insert %s.%s failed, err:%s", schema, table, err)
			return nil, err
		}
	}

	m.db = db
	m.name = name
	m.pos = pos
	m.schema = schema
	m.table = table
	m.serverId = serverId

	return &m, err
}

func (m *dbInfo) Save(pos mysql.Position) error {
	log.Printf("save position %s", pos)

	m.Lock()
	defer m.Unlock()

	m.name = pos.Name
	m.pos = pos.Pos

	//写db
	var err error
	updateSQL := fmt.Sprintf("UPDATE %s.%s SET bin_name = '%s', bin_pos = %d WHERE server_id = %d", m.schema, m.table, m.name, m.pos, m.serverId)
	if _, err = m.db.Exec(updateSQL); err != nil {
		log.Printf("update %s.%s failed, err:%s", m.schema, m.table, err)
	}

	return err
}

func (m *dbInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		Name: m.name,
		Pos:  m.pos,
	}
}

func (m *dbInfo) SetPosition(p mysql.Position) {
	m.RLock()
	defer m.RUnlock()

	m.name = p.Name
	m.pos = p.Pos
}

func (m *dbInfo) Close() error {
	log.Println("----- closing master -----")

	pos := m.Position()

	return m.Save(pos)
}
