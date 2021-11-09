package endpoint

import (
	"context"
	"errors"
	"fmt"
	"github.com/Hamster601/Budd/config"
	"github.com/Hamster601/Budd/metric"
	"github.com/Hamster601/Budd/pkg/logagent"
	"github.com/Hamster601/Budd/pkg/logs"
	"github.com/Hamster601/Budd/pkg/model"
	"github.com/Hamster601/Budd/pkg/stringutil"
	"github.com/Hamster601/Budd/services/luaengine"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/olivere/elastic/v7"
	"github.com/siddontang/go-log/log"
	"sync"
)

type Elastic7Endpoint struct {
	first  string
	hosts  []string
	client *elastic.Client
	retryLock sync.Mutex
}

func newElastic7Endpoint() *Elastic7Endpoint {

	hosts := elsHosts(config.InitConfig.ESConfig.ElsAddr)
	r := &Elastic7Endpoint{}
	r.hosts = hosts
	r.first = hosts[0]
	return r
}

func (s *Elastic7Endpoint) Connect() error {
	var options []elastic.ClientOptionFunc
	options = append(options, elastic.SetErrorLog(logagent.NewElsLoggerAgent()))
	options = append(options, elastic.SetURL(s.hosts...))
	if config.InitConfig.ESConfig.ElsUser != "" && config.InitConfig.ESConfig.ElsPassword!= "" {
		options = append(options, elastic.SetBasicAuth(config.InitConfig.ESConfig.ElsUser, config.InitConfig.ESConfig.ElsPassword))
	}

	client, err := elastic.NewClient(options...)
	if err != nil {
		return err
	}

	s.client = client
	return s.indexMapping()
}

func (s *Elastic7Endpoint) indexMapping() error {
	for _, rule := range config.RuleInsList() {
		exists, err := s.client.IndexExists(rule.ElsIndex).Do(context.Background())
		if err != nil {
			return err
		}
		if exists {
			err = s.updateIndexMapping(rule)
		} else {
			err = s.insertIndexMapping(rule)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Elastic7Endpoint) insertIndexMapping(rule *config.Details) error {
	var properties map[string]interface{}
	if rule.LuaEnable() {
		properties = buildPropertiesByMappings(rule)
	} else {
		properties = buildPropertiesByRule(rule)
	}

	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": properties,
		},
	}
	body := stringutil.ToJsonString(mapping)

	ret, err := s.client.CreateIndex(rule.ElsIndex).Body(body).Do(context.Background())
	if err != nil {
		return err
	}
	if !ret.Acknowledged {
		return errors.New(fmt.Sprintf("create index %s err", rule.ElsIndex))
	}

	logs.Infof("create index: %s ,mappings: %s", rule.ElsIndex, body)

	return nil
}

func (s *Elastic7Endpoint) updateIndexMapping(rule *config.Details) error {
	ret, err := s.client.GetMapping().Index(rule.ElsIndex).Do(context.Background())
	if err != nil {
		return err
	}

	if ret[rule.ElsIndex]==nil{
		return nil
	}
	retIndex := ret[rule.ElsIndex].(map[string]interface{})

	if retIndex["mappings"] == nil {
		return nil
	}
	retMaps := retIndex["mappings"].(map[string]interface{})

	if retMaps["properties"] == nil {
		return nil
	}
	retPros := retMaps["properties"].(map[string]interface{})

	var currents map[string]interface{}
	if rule.LuaEnable() {
		currents = buildPropertiesByMappings(rule)
	} else {
		currents = buildPropertiesByRule(rule)
	}

	if len(retPros) < len(currents) {
		properties := make(map[string]interface{})
		mapping := map[string]interface{}{
			"properties": properties,
		}
		for field, current := range currents {
			if _, exist := retPros[field]; !exist {
				properties[field] = current
			}
		}

		doc := stringutil.ToJsonString(mapping)
		ret, err := s.client.PutMapping().Index(rule.ElsIndex).BodyString(doc).Do(context.Background())
		if err != nil {
			return err
		}
		if !ret.Acknowledged {
			return errors.New(fmt.Sprintf("update index %s err", rule.ElsIndex))
		}

		logs.Infof("update index: %s ,properties: %s", rule.ElsIndex, doc)
	}

	return nil
}

func (s *Elastic7Endpoint) Ping() error {
	if _, _, err := s.client.Ping(s.first).Do(context.Background()); err == nil {
		return nil
	}

	for _, host := range s.hosts {
		if _, _, err := s.client.Ping(host).Do(context.Background()); err == nil {
			return nil
		}
	}

	return errors.New("ssx")
}

func (s *Elastic7Endpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	bulk := s.client.Bulk()
	for _, row := range rows {
		rule, _ := config.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		metric.UpdateActionNum(row.Action, row.RuleKey,config.InitConfig.EnableExporter)

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoESOps(kvm, row.Action, rule)
			if err != nil {
				log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
				return errors.New(fmt.Sprintf("lua 脚本执行失败 : %s ", err.Error()))
			}
			for _, resp := range ls {
				logs.Infof("action: %s, Index: %s , Id:%s, value: %v", resp.Action, resp.Index, resp.Id, resp.Date)
				s.prepareBulk(resp.Action, resp.Index, resp.Id, resp.Date, bulk)
			}
		} else {
			kvm := rowMap(row, rule, false)
			id := primaryKey(row, rule)
			body := encodeValue(rule, kvm)
			logs.Infof("action: %s, Index: %s , Id:%s, value: %v", row.Action, rule.ElsIndex, id, body)
			s.prepareBulk(row.Action, rule.ElsIndex, stringutil.ToString(id), body, bulk)
		}
	}

	if bulk.NumberOfActions() == 0 {
		return nil
	}

	r, err := bulk.Do(context.Background())
	if err != nil {
		return err
	}

	if len(r.Failed()) > 0 {
		for _, f := range r.Failed() {
			reason := f.Index + " " + f.Type + " " + f.Result
			if f.Error == nil && "not_found" == f.Result {
				return nil
			}

			if f.Error != nil {
				reason = f.Error.Reason
			}
			log.Println(reason)
			return errors.New(reason)
		}
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *Elastic7Endpoint) Stock(rows []*model.RowRequest) int64 {
	if len(rows) == 0 {
		return 0
	}

	bulk := s.client.Bulk()
	for _, row := range rows {
		rule, _ := config.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoESOps(kvm, row.Action, rule)
			if err != nil {
				logs.Errorf("lua 脚本执行失败 : %s ", err.Error())
				break
			}
			for _, resp := range ls {
				s.prepareBulk(resp.Action, resp.Index, resp.Id, resp.Date, bulk)
			}
		} else {
			kvm := rowMap(row, rule, false)
			id := primaryKey(row, rule)
			body := encodeValue(rule, kvm)
			s.prepareBulk(row.Action, rule.ElsIndex, stringutil.ToString(id), body, bulk)
		}
	}

	r, err := bulk.Do(context.Background())
	if err != nil {
		logs.Error(err.Error())
		return 0
	}

	if len(r.Failed()) > 0 {
		for _, f := range r.Failed() {
			logs.Error(f.Error.Reason)
		}
	}

	return int64(len(r.Succeeded()))
}

func (s *Elastic7Endpoint) prepareBulk(action, index, id, doc string, bulk *elastic.BulkService) {
	switch action {
	case canal.InsertAction:
		req := elastic.NewBulkIndexRequest().Index(index).Id(id).Doc(doc)
		bulk.Add(req)
	case canal.UpdateAction:
		req := elastic.NewBulkUpdateRequest().Index(index).Id(id).Doc(doc)
		bulk.Add(req)
	case canal.DeleteAction:
		req := elastic.NewBulkDeleteRequest().Index(index).Id(id)
		bulk.Add(req)
	}

	logs.Infof("index: %s, doc: %s", index, doc)
}

func (s *Elastic7Endpoint) Close() {
	if s.client != nil {
		s.client.Stop()
	}
}
