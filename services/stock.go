package services

import (
	"errors"
	"fmt"
	"github.com/Hamster601/Budd/config"
	"github.com/Hamster601/Budd/pkg/datefortmat"
	"github.com/Hamster601/Budd/pkg/logs"
	"github.com/Hamster601/Budd/pkg/model"
	"github.com/Hamster601/Budd/services/endpoint"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/siddontang/go-log/log"
	"go.uber.org/atomic"
	"regexp"
	"strings"
	"sync"
)

// StockService 存量数据
type StockService struct {
	canal    *canal.Canal
	endpoint endpoint.Endpoint

	queueCh       chan []*model.RowRequest
	counter       map[string]int64
	lockOfCounter sync.Mutex
	totalRows     map[string]int64
	wg            sync.WaitGroup
	shutoff       *atomic.Bool
}

func NewStockService() *StockService {
	return &StockService{
		queueCh:   make(chan []*model.RowRequest, config.InitConfig.Maxprocs),
		counter:   make(map[string]int64),
		totalRows: make(map[string]int64),
		shutoff:   atomic.NewBool(false),
	}
}

func (s *StockService) Run() error {
	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = config.InitConfig.Addr
	canalCfg.User = config.InitConfig.User
	canalCfg.Password = config.InitConfig.Password
	canalCfg.Charset = config.InitConfig.Charset
	canalCfg.Flavor = config.InitConfig.Flavor
	canalCfg.ServerID = config.InitConfig.SlaveID
	canalCfg.Dump.ExecutionPath = config.InitConfig.DumpExec
	canalCfg.Dump.DiscardErr = false
	canalCfg.Dump.SkipMasterData = config.InitConfig.SkipMasterData

	if c, err := canal.NewCanal(canalCfg); err != nil {
		return err
	} else {
		s.canal = c
	}

	if err := s.completeRules(); err != nil {
		return err
	}
	s.addDumpDatabaseOrTable()

	endpoint := endpoint.NewEndpoint(s.canal)
	if err := endpoint.Connect(); err != nil {
		log.Println(err.Error())
		return err
	}
	s.endpoint = endpoint

	startTime := datefortmat.NowMillisecond()
	log.Println(fmt.Sprintf("bulk size: %d", config.InitConfig.BulkSize))
	for _, rule := range config.RuleInsList() {
		if rule.OrderByColumn == "" {
			return errors.New("empty order_by_column not allowed")
		}

		exportColumns := s.exportColumns(rule)
		fullName := fmt.Sprintf("%s.%s", rule.Schema, rule.Table)
		log.Println(fmt.Sprintf("开始导出 %s", fullName))

		res, err := s.canal.Execute(fmt.Sprintf("select count(1) from %s", fullName))
		if err != nil {
			return err
		}
		totalRow, err := res.GetInt(0, 0)
		s.totalRows[fullName] = totalRow
		log.Println(fmt.Sprintf("%s 共 %d 条数据", fullName, totalRow))

		s.counter[fullName] = 0

		var batch int64
		size := config.InitConfig.BulkSize
		if batch%size == 0 {
			batch = totalRow / size
		} else {
			batch = (totalRow / size) + 1
		}

		var processed atomic.Int64
		for i := 0; i < config.InitConfig.Maxprocs; i++ {
			s.wg.Add(1)
			go func(_fullName, _columns string, _rule *config.Details) {
				for {
					processed.Inc()
					requests, err := s.export(_fullName, _columns, processed.Load(), _rule)
					if err != nil {
						logs.Error(err.Error())
						s.shutoff.Store(true)
						break
					}

					s.imports(_fullName, requests)
					if processed.Load() > batch {
						break
					}
				}
				s.wg.Done()
			}(fullName, exportColumns, rule)
		}
	}

	s.wg.Wait()

	fmt.Println(fmt.Sprintf("共耗时 ：%d（毫秒）", datefortmat.NowMillisecond()-startTime))

	for k, v := range s.totalRows {
		vv, ok := s.counter[k]
		if ok {
			fmt.Println(fmt.Sprintf("表： %s，共：%d 条数据，成功导入：%d 条", k, v, vv))
			if v > vv {
				fmt.Println("存在导入错误的数据，具体请至日志查看")
			}
		}
	}

	s.endpoint.Close() // 关闭客户端

	return nil
}

func (s *StockService) export(fullName, columns string, batch int64, rule *config.Details) ([]*model.RowRequest, error) {
	if s.shutoff.Load() {
		return nil, errors.New("shutoff")
	}

	offset := s.offset(batch)
	sql := s.buildSql(fullName, columns, offset, rule)
	logs.Infof("export sql : %s", sql)
	resultSet, err := s.canal.Execute(sql)
	if err != nil {
		logs.Errorf("数据导出错误: %s - %s", sql, err.Error())
		return nil, err
	}
	rowNumber := resultSet.RowNumber()
	requests := make([]*model.RowRequest, 0, rowNumber)
	for i := 0; i < rowNumber; i++ {
		rowValues := make([]interface{}, 0, len(rule.TableInfo.Columns))
		request := new(model.RowRequest)
		for j := 0; j < len(rule.TableInfo.Columns); j++ {
			val, err := resultSet.GetValue(i, j)
			if err != nil {
				logs.Errorf("数据导出错误: %s - %s", sql, err.Error())
				break
			}
			rowValues = append(rowValues, val)
			request.Action = canal.InsertAction
			request.RuleKey = config.RuleKey(rule.Schema, rule.Table)
			request.Row = rowValues
		}
		requests = append(requests, request)
	}

	return requests, nil
}

// 构造SQL
func (s *StockService) buildSql(fullName, columns string, offset int64, rule *config.Details) string {
	size := config.InitConfig.BulkSize
	if len(rule.TableInfo.PKColumns) == 0 {
		return fmt.Sprintf("select %s from %s order by %s limit %d,%d", columns, fullName, rule.OrderByColumn, offset, size)
	}

	i := rule.TableInfo.PKColumns[0]
	n := rule.TableInfo.GetPKColumn(i).Name
	t := "select b.* from (select %s from %s order by %s limit %d,%d) a left join %s b on a.%s=b.%s"
	sql := fmt.Sprintf(t, n, fullName, rule.OrderByColumn, offset, size, fullName, n, n)
	return sql
}

func (s *StockService) imports(fullName string, requests []*model.RowRequest) {
	if s.shutoff.Load() {
		return
	}

	succeeds := s.endpoint.Stock(requests)
	count := s.incCounter(fullName, succeeds)
	log.Println(fmt.Sprintf("%s 导入数据 %d 条", fullName, count))
}

func (s *StockService) exportColumns(rule *config.Details) string {
	if rule.IncludeColumnConfig != "" {
		var columns string
		includes := strings.Split(rule.IncludeColumnConfig, ",")
		for _, c := range rule.TableInfo.Columns {
			for _, e := range includes {
				var column string
				if strings.ToUpper(e) == strings.ToUpper(c.Name) {
					column = c.Name
				} else {
					column = "null as " + c.Name
				}

				if columns != "" {
					columns = columns + ","
				}
				columns = columns + column
			}
		}
		return columns
	}

	if rule.ExcludeColumnConfig != "" {
		var columns string
		excludes := strings.Split(rule.ExcludeColumnConfig, ",")
		for _, c := range rule.TableInfo.Columns {
			for _, e := range excludes {
				var column string
				if strings.ToUpper(e) == strings.ToUpper(c.Name) {
					column = "null as " + c.Name
				} else {
					column = c.Name
				}

				if columns != "" {
					columns = columns + ","
				}
				columns = columns + column
			}
		}
		return columns
	}

	return "*"
}

func (s *StockService) offset(currentPage int64) int64 {
	var offset int64

	if currentPage > 0 {
		offset = (currentPage - 1) * config.InitConfig.BulkSize
	}

	return offset
}

func (s *StockService) Close() {
	s.canal.Close()
}

func (s *StockService) incCounter(name string, n int64) int64 {
	s.lockOfCounter.Lock()
	defer s.lockOfCounter.Unlock()

	c, ok := s.counter[name]
	if ok {
		c = c + n
		s.counter[name] = c
	}

	return c
}

func (s *StockService) completeRules() error {
	wildcards := make(map[string]bool)
	for _, rc := range config.InitConfig.RuleConfig{
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

func (s *StockService) addDumpDatabaseOrTable() {
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
