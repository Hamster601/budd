package pkg

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	constvar "github.com/Hamster601/Budd/pkg/const"
	es7 "github.com/olivere/elastic/v7"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/Hamster601/Budd/config"
	"github.com/siddontang/go-mysql/canal"
)

type Empty struct{}
type Event2Pipe map[string]Bin2esConfig
type RefFuncMap map[string]reflect.Value
type SQLPool map[string]*sql.DB
type Set map[string]Empty

type Pos2Field map[string]int
type Field map[string]string
type Replace map[string]string
type Pipeline map[string]interface{}
type ROWS []map[string]interface{}

type Bin2esConfig []struct {
	Schema    string     `json:"schema"`
	Tables    []string   `json:"tables"`
	Actions   []string   `json:"actions"`
	Pipelines []Pipeline `json:"pipelines"`
	Dest      Dest       `json:"dest"`
}
type Dest struct {
	Index string `json:"index"`
}

func NewBin2esConfig(path string, config *Bin2esConfig) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	//读取的数据为json格式，需要进行解码
	err = json.Unmarshal(data, config)
	if err != nil {
		return err
	}

	return nil
}

type Bin2es struct {
	c          *config.Config
	ctx        context.Context
	cancel     context.CancelFunc
	canal      *canal.Canal
	wg         sync.WaitGroup
	master     *dbInfo
	esCli      *MyES
	syncCh     chan interface{}
	refFuncMap RefFuncMap
	event2Pipe Event2Pipe
	bin2esConf Bin2esConfig
	sqlPool    SQLPool
	tblFilter  Set
	finish     chan bool
}

func NewBin2es(c *config.Config) (*Bin2es, error) {
	b := new(Bin2es)
	b.c = c
	b.finish = make(chan bool)
	b.syncCh = make(chan interface{}, c.Bin2Es.SyncChLen)
	b.sqlPool = make(SQLPool)
	b.tblFilter = make(Set)
	b.refFuncMap = make(RefFuncMap)
	b.event2Pipe = make(Event2Pipe)
	b.ctx, b.cancel = context.WithCancel(context.Background())

	var err error
	if b.master, err = loadMasterInfo(c.MysqlSlave.ServerID, c.MasterInfo); err != nil {
		return nil, err
	}

	if err = b.initBin2esConf(); err != nil {
		return nil, err
	}

	if err = b.newCanal(); err != nil {
		return nil, err
	}

	if err = b.newES(); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *Bin2es) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", b.c.MysqlSlave.Addr, b.c.MysqlSlave.Port)
	cfg.User = b.c.MysqlSlave.User
	cfg.Password = b.c.MysqlSlave.Pwd
	cfg.Charset = b.c.MysqlSlave.Charset
	cfg.Flavor = "mysql"
	cfg.SemiSyncEnabled = true

	cfg.ServerID = b.c.MysqlSlave.ServerID
	cfg.Dump.ExecutionPath = "mysqldump"
	cfg.Dump.DiscardErr = false
	cfg.Dump.SkipMasterData = false

	var err error
	if b.canal, err = canal.NewCanal(cfg); err != nil {
		return err
	}

	b.canal.SetEventHandler(&eventHandler{b})

	//init tblFilter
	for _, source := range b.c.Sources {
		schema := source.Schema
		for _, table := range source.Tables {
			key := schema + "." + table
			b.tblFilter[key] = Empty{}
		}
	}

	//prepare canal
	for _, source := range b.c.Sources {
		b.canal.AddDumpTables(source.Schema, source.DumpTable)
	}

	// We must use binlog full row image
	if err = b.canal.CheckBinlogRowImage(constvar.CanalROWImageFull); err != nil {
		return err
	}

	return nil
}

func (b *Bin2es) isInTblFilter(key string) bool {
	_, ok := b.tblFilter[key]
	return ok
}

func (b *Bin2es) initBin2esConf() error {
	//read json config
	err := NewBin2esConfig("./config/binlog2es.json", &b.bin2esConf)
	if err != nil {
		log.Printf("Failed to create ES Processor, err:%s", err.Error())
		b.cancel()
		return err
	}

	//initialize event2Pipe
	set := make(map[string]Empty)
	for _, conf := range b.bin2esConf {
		schema := conf.Schema
		set[schema] = Empty{}
		for _, table := range conf.Tables {
			for _, action := range conf.Actions {
				key := fmt.Sprintf("%s_%s_%s", schema, table, action)
				b.event2Pipe[key] = append(b.event2Pipe[key], conf)
			}
		}
	}

	//initialize db connection
	for schema, _ := range set {
		user := b.c.MysqlSlave.User
		pwd := b.c.MysqlSlave.Pwd
		addr := b.c.MysqlSlave.Addr
		port := b.c.MysqlSlave.Port
		charset := b.c.MysqlSlave.Charset
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", user, pwd, addr, port, schema, charset)

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Printf("sql Open error: %s", err.Error())
			b.cancel()
			return err
		}
		if err = db.Ping(); err != nil {
			log.Printf("db ping error: %s", err.Error())
			b.cancel()
			return err
		}
		b.sqlPool[schema] = db

		log.Printf("connect to mysql successfully, dsn:%s", []string{dsn})
	}

	//initialize refFuncMap
	value := reflect.ValueOf(reflectFunc{b})
	vType := value.Type()
	for i := 0; i < value.NumMethod(); i++ {
		key := vType.Method(i)
		b.refFuncMap[key.Name] = value.Method(i)
	}

	return nil
}

func (b *Bin2es) newES() error {
	var err error
	b.esCli = new(MyES)
	b.esCli.ctx = b.Ctx()

	httpClient := http.DefaultClient

	var funcs []es7.ClientOptionFunc
	funcs = append(funcs, es7.SetURL(b.c.Es.Nodes...))
	funcs = append(funcs, es7.SetHealthcheckInterval(5*time.Second))
	funcs = append(funcs, es7.SetGzip(true))
	funcs = append(funcs, es7.SetRetrier(NewMyRetrier()))
	funcs = append(funcs, es7.SetSniff(false))

	if b.c.Es.EnableAuthentication {
		httpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: b.c.Es.InsecureSkipVerify},
			},
		}

		funcs = append(funcs, es7.SetBasicAuth(b.c.Es.User, b.c.Es.Passwd))
	}

	funcs = append(funcs, es7.SetHttpClient(httpClient))

	b.esCli.client, err = es7.NewClient(funcs...)

	if err != nil {
		log.Printf("Failed to create ES client, err:%s", err.Error())
		b.cancel()
		return err
	}

	b.esCli.bulkService = b.esCli.client.Bulk()

	log.Printf("connect to es successfully, addr:%v", b.c.Es.Nodes)

	return nil
}

func (b *Bin2es) Ctx() context.Context {
	return b.ctx
}

func (b *Bin2es) Run() error {
	defer log.Print("----- Canal closed -----")

	b.wg.Add(1)
	go b.syncES()

	pos := b.master.Position()
	if err := b.canal.RunFrom(pos); err != nil {
		log.Printf("start canal err %v", err)
		b.cancel()
		return err
	}

	return nil
}

func (b *Bin2es) CloseDB() {
	defer log.Print("----- DB Closed -----")

	for _, db := range b.sqlPool {
		db.Close()
	}
}

func (b *Bin2es) Close() {
	defer log.Print("----- Bin2es Closed -----")

	log.Print("closing bin2es")

	b.cancel()

	b.canal.Close()

	b.master.Close()

	<-b.finish

	b.esCli.Close()

	b.CloseDB()

	//消耗完剩余syncCh里的消息, 不然会有一定概率阻塞Canal组件的关闭
	for {
		select {
		case <-b.syncCh:
		default:
			goto END
		}
	}

END:
	close(b.syncCh)
	log.Print("----- close sync channel -----")

	b.wg.Wait()
}
