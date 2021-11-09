package services

import (
	"errors"
	"fmt"
	"github.com/Hamster601/Budd/config"
	"github.com/Hamster601/Budd/metric"
	"github.com/Hamster601/Budd/pkg/logs"
	"github.com/Hamster601/Budd/pkg/storage"
	"github.com/Hamster601/Budd/services/endpoint"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"go.uber.org/atomic"
	"regexp"
	"sync"
	"time"
)

const _transferLoopInterval = 1

type TransferService struct {
	canal        *canal.Canal
	canalCfg     *canal.Config
	canalHandler *handler
	canalEnable  atomic.Bool
	lockOfCanal  sync.Mutex
	firstsStart  atomic.Bool

	wg             sync.WaitGroup
	endpoint       endpoint.Endpoint
	endpointEnable atomic.Bool
	positionDao    storage.PositionStorage
	loopStopSignal chan struct{}
}

func (s *TransferService) initialize() error {
	s.canalCfg = canal.NewDefaultConfig()
	s.canalCfg.Addr = config.InitConfig.Addr
	s.canalCfg.User = config.InitConfig.User
	s.canalCfg.Password = config.InitConfig.Password
	s.canalCfg.Charset = config.InitConfig.Charset
	s.canalCfg.Flavor = config.InitConfig.Flavor
	s.canalCfg.ServerID = config.InitConfig.SlaveID
	s.canalCfg.Dump.ExecutionPath = config.InitConfig.DumpExec
	s.canalCfg.Dump.DiscardErr = false
	s.canalCfg.Dump.SkipMasterData = config.InitConfig.SkipMasterData

	if err := s.createCanal(); err != nil {
		return err
	}

	if err := s.completeRules(); err != nil {
		return err
	}

	s.addDumpDatabaseOrTable()

	positionDao := storage.NewPositionStorage()
	if err := positionDao.Initialize(); err != nil {
		return err
	}
	s.positionDao = positionDao

	// endpoint
	endpoint := endpoint.NewEndpoint(s.canal)
	if err := endpoint.Connect(); err != nil {
		return err
	}
	// 异步，必须要ping下才能确定连接成功

	s.endpoint = endpoint
	s.endpointEnable.Store(true)
	metric.SetDestState(metric.DestStateOK,config.InitConfig.EnableExporter)

	s.firstsStart.Store(true)
	s.startLoop()

	return nil
}

func (s *TransferService) run() error {
	current, err := s.positionDao.Get()
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go func(p mysql.Position) {
		s.canalEnable.Store(true)
		log.Println(fmt.Sprintf("transfer run from position(%s %d)", p.Name, p.Pos))
		if err := s.canal.RunFrom(p); err != nil {
			log.Println(fmt.Sprintf("start transfer : %v", err))
			logs.Errorf("canal : %v", err.Error())
			if s.canalHandler != nil {
				s.canalHandler.stopListener()
			}
			s.canalEnable.Store(false)
		}

		logs.Info("Canal is Closed")
		s.canalEnable.Store(false)
		s.canal = nil
		s.wg.Done()
	}(current)

	// canal未提供回调，停留一秒，确保RunFrom启动成功
	time.Sleep(time.Second)
	return nil
}

func (s *TransferService) StartUp() {
	s.lockOfCanal.Lock()
	defer s.lockOfCanal.Unlock()

	if s.firstsStart.Load() {
		s.canalHandler = newHandler()
		s.canal.SetEventHandler(s.canalHandler)
		s.canalHandler.startListener()
		s.firstsStart.Store(false)
		s.run()
	} else {
		s.restart()
	}
}

func (s *TransferService) restart() {
	if s.canal != nil {
		s.canal.Close()
		s.wg.Wait()
	}

	s.createCanal()
	s.addDumpDatabaseOrTable()
	s.canalHandler = newHandler()
	s.canal.SetEventHandler(s.canalHandler)
	s.canalHandler.startListener()
	s.run()
}

func (s *TransferService) stopDump() {
	s.lockOfCanal.Lock()
	defer s.lockOfCanal.Unlock()

	if s.canal == nil {
		return
	}

	if !s.canalEnable.Load() {
		return
	}

	if s.canalHandler != nil {
		s.canalHandler.stopListener()
		s.canalHandler = nil
	}

	s.canal.Close()
	s.wg.Wait()

	log.Println("dumper stopped")
}

func (s *TransferService) Close() {
	s.stopDump()
	s.loopStopSignal <- struct{}{}
}

func (s *TransferService) Position() (mysql.Position, error) {
	return s.positionDao.Get()
}

func (s *TransferService) createCanal() error {
	for _, rc := range config.InitConfig.RuleConfig {
		s.canalCfg.IncludeTableRegex = append(s.canalCfg.IncludeTableRegex, rc.Schema+"\\."+rc.Table)
	}
	var err error
	s.canal, err = canal.NewCanal(s.canalCfg)
	return err
}

func (s *TransferService) completeRules() error {
	wildcards := make(map[string]bool)
	for _, rc := range config.InitConfig.RuleConfig {
		if rc.Table == "*" {
			return errors.New("wildcard * is not allowed for table name")
		}

		if regexp.QuoteMeta(rc.Table) != rc.Table { //通配符
			if _, ok := wildcards[config.RuleKey(rc.Schema, rc.Schema)]; ok {
				return errors.New(fmt.Sprintf("duplicate wildcard table defined for %s.%s", rc.Schema, rc.Table))
			}

			tableName := rc.Table
			if rc.Table == "*" {
				tableName = "." + rc.Table
			}
			sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
					table_name RLIKE "%s" AND table_schema = "%s";`, tableName, rc.Schema)
			res, err := s.canal.Execute(sql)
			if err != nil {
				return err
			}
			for i := 0; i < res.Resultset.RowNumber(); i++ {
				tableName, _ := res.GetString(i, 0)
				newRule, err := config.RuleDeepClone(&rc)
				if err != nil {
					return err
				}
				newRule.Table = tableName
				ruleKey := config.RuleKey(rc.Schema, tableName)
				config.AddRuleIns(ruleKey, newRule)
			}
		} else {
			newRule, err := config.RuleDeepClone(&rc)
			if err != nil {
				return err
			}
			ruleKey := config.RuleKey(rc.Schema, rc.Table)
			config.AddRuleIns(ruleKey, newRule)
		}
	}

	for _, rule := range config.RuleInsList() {
		tableMata, err := s.canal.GetTable(rule.Schema, rule.Table)
		if err != nil {
			return err
		}
		if len(tableMata.PKColumns) == 0 {
			if !config.InitConfig.SkipNoPkTable {
				return errors.New(fmt.Sprintf("%s.%s must have a PK for a column", rule.Schema, rule.Table))
			}
		}
		if len(tableMata.PKColumns) > 1 {
			rule.IsCompositeKey = true // 组合主键
		}
		rule.TableInfo = tableMata
		rule.TableColumnSize = len(tableMata.Columns)

		if err := rule.Initialize(); err != nil {
			return err
		}

		if rule.LuaEnable() {
			if err := rule.CompileLuaScript(config.InitConfig.DataDir); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *TransferService) addDumpDatabaseOrTable() {
	var schema string
	schemas := make(map[string]int)
	tables := make([]string, 0, config.RuleInsTotal())
	for _, rule := range config.RuleInsList() {
		schema = rule.Table
		schemas[rule.Schema] = 1
		tables = append(tables, rule.Table)
	}
	if len(schemas) == 1 {
		s.canal.AddDumpTables(schema, tables...)
	} else {
		keys := make([]string, 0, len(schemas))
		for key := range schemas {
			keys = append(keys, key)
		}
		s.canal.AddDumpDatabases(keys...)
	}
}

func (s *TransferService) updateRule(schema, table string) error {
	rule, ok := config.RuleIns(config.RuleKey(schema, table))
	if ok {
		tableInfo, err := s.canal.GetTable(schema, table)
		if err != nil {
			return err
		}

		if len(tableInfo.PKColumns) == 0 {
			if !config.InitConfig.SkipNoPkTable {
				return errors.New(fmt.Sprintf("%s.%s must have a PK for a column", rule.Schema, rule.Table))
			}
		}

		if len(tableInfo.PKColumns) > 1 {
			rule.IsCompositeKey = true
		}

		rule.TableInfo = tableInfo
		rule.TableColumnSize = len(tableInfo.Columns)

		err = rule.AfterUpdateTableInfo()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *TransferService) startLoop() {
	go func() {
		ticker := time.NewTicker(_transferLoopInterval * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !s.endpointEnable.Load() {
					err := s.endpoint.Ping()
					if err != nil {
						log.Println("destination not available,see the log file for details")
						logs.Error(err.Error())
					} else {
						s.endpointEnable.Store(true)

						s.StartUp()
						metric.SetDestState(metric.DestStateOK,config.InitConfig.EnableExporter)
					}
				}
			case <-s.loopStopSignal:
				return
			}
		}
	}()
}