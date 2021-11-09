package endpoint

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/Hamster601/Budd/metric"

	"github.com/go-redis/redis"

	"github.com/Hamster601/Budd/config"
	"github.com/Hamster601/Budd/pkg/logs"
	"github.com/Hamster601/Budd/pkg/model"
	"github.com/Hamster601/Budd/pkg/stringutil"
	"github.com/Hamster601/Budd/services/luaengine"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"strings"
	"sync"
)

type RedisEndpoint struct {
	isCluster bool
	client    *redis.Client
	cluster   *redis.ClusterClient
	retryLock sync.Mutex
}

func newRedisEndpoint(filename string) *RedisEndpoint {
	cfg ,_:=config.NewConfig(filename)
	r := &RedisEndpoint{}

	list := strings.Split(cfg.RedisConfig.RedisAddr, ",")
	if len(list) == 1 {
		r.client = redis.NewClient(&redis.Options{
			Addr:     cfg.RedisConfig.RedisAddr,
			Password: cfg.RedisConfig.RedisPass,
			DB:       cfg.RedisConfig.RedisDatabase,
		})
	} else {
		if cfg.RedisConfig.RedisGroupType == config.RedisGroupTypeSentinel {
			r.client = redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    cfg.RedisConfig.RedisMasterName,
				SentinelAddrs: list,
				Password:      cfg.RedisConfig.RedisPass,
				DB:            cfg.RedisConfig.RedisDatabase,
			})
		}
		if cfg.RedisConfig.RedisGroupType == config.RedisGroupTypeCluster {
			r.isCluster = true
			r.cluster = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    list,
				Password: cfg.RedisConfig.RedisPass,
			})
		}
	}

	return r
}

func (s *RedisEndpoint) Connect() error {
	return s.Ping()
}

func (s *RedisEndpoint) Ping() error {
	var err error
	if s.isCluster {
		_, err = s.cluster.Ping().Result()
	} else {
		_, err = s.client.Ping().Result()
	}
	return err
}

func (s *RedisEndpoint) pipe() redis.Pipeliner {
	var pipe redis.Pipeliner
	if s.isCluster {
		pipe = s.cluster.Pipeline()
	} else {
		pipe = s.client.Pipeline()
	}
	return pipe
}

func (s *RedisEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	pipe := s.pipe()
	for _, row := range rows {
		rule, _ := config.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		metric.UpdateActionNum(row.Action, row.RuleKey,true) // 后续重构

		if rule.LuaEnable() {
			var err error
			var ls []*model.RedisRespond
			kvm := rowMap(row, rule, true)
			if row.Action == canal.UpdateAction {
				previous := oldRowMap(row, rule, true)
				ls, err = luaengine.DoRedisOps(kvm, previous, row.Action, rule)
			} else {
				ls, err = luaengine.DoRedisOps(kvm, nil, row.Action, rule)
			}
			if err != nil {
				log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
				return errors.New(fmt.Sprintf("Lua 脚本执行失败 : %s ", err.Error()))
			}
			for _, resp := range ls {
				s.preparePipe(resp, pipe)
				logs.Infof("action: %s, structure: %s ,key: %s ,field: %s, value: %v", resp.Action, resp.Structure, resp.Key, resp.Field, resp.Val)
			}
			kvm = nil
		} else {
			resp := s.ruleRespond(row, rule)
			s.preparePipe(resp, pipe)
			logs.Infof("action: %s, structure: %s ,key: %s ,field: %s, value: %v", resp.Action, resp.Structure, resp.Key, resp.Field, resp.Val)
		}
	}

	_, err := pipe.Exec()
	if err != nil {
		return err
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *RedisEndpoint) Stock(rows []*model.RowRequest) int64 {
	pipe := s.pipe()
	for _, row := range rows {
		rule, _ := config.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoRedisOps(kvm, nil, row.Action, rule)
			if err != nil {
				logs.Errorf("lua 脚本执行失败 : %s ", err.Error())
				break
			}
			for _, resp := range ls {
				s.preparePipe(resp, pipe)
			}
		} else {
			resp := s.ruleRespond(row, rule)
			resp.Action = row.Action
			resp.Structure = rule.Redis.RedisStructure
			s.preparePipe(resp, pipe)
		}
	}

	var counter int64
	res, err := pipe.Exec()
	if err != nil {
		logs.Error(err.Error())
	}

	for _, re := range res {
		if re.Err() == nil {
			counter++
		}
	}

	return counter
}

func (s *RedisEndpoint) ruleRespond(row *model.RowRequest, rule *config.Details) *model.RedisRespond {
	resp := new(model.RedisRespond)
	resp.Action = row.Action
	resp.Structure = rule.Redis.RedisStructure

	kvm := rowMap(row, rule, false)
	resp.Key = s.encodeKey(row, rule)
	if resp.Structure == config.RedisStructureHash {
		resp.Field = s.encodeHashField(row, rule)
	}
	if resp.Structure == config.RedisStructureSortedSet {
		resp.Score = s.encodeSortedSetScoreField(row, rule)
	}

	if resp.Action == canal.InsertAction {
		resp.Val = encodeValue(rule, kvm)
	} else if resp.Action == canal.UpdateAction {
		if rule.Redis.RedisStructure == config.RedisStructureList ||
			rule.Redis.RedisStructure == config.RedisStructureSet ||
			rule.Redis.RedisStructure == config.RedisStructureSortedSet {
			oldKvm := oldRowMap(row, rule, false)
			resp.OldVal = encodeValue(rule, oldKvm)
		}
		resp.Val = encodeValue(rule, kvm)
	} else {
		if rule.Redis.RedisStructure == config.RedisStructureList ||
			rule.Redis.RedisStructure == config.RedisStructureSet ||
			rule.Redis.RedisStructure == config.RedisStructureSortedSet {
			resp.Val = encodeValue(rule, kvm)
		}
	}

	return resp
}

func (s *RedisEndpoint) preparePipe(resp *model.RedisRespond, pipe redis.Cmdable) {
	switch resp.Structure {
	case config.RedisStructureString:
		if resp.Action == canal.DeleteAction {
			pipe.Del(resp.Key)
		} else {
			pipe.Set(resp.Key, resp.Val, 0)
		}
	case config.RedisStructureHash:
		if resp.Action == canal.DeleteAction {
			pipe.HDel(resp.Key, resp.Field)
		} else {
			pipe.HSet(resp.Key, resp.Field, resp.Val)
		}
	case config.RedisStructureList:
		if resp.Action == canal.DeleteAction {
			pipe.LRem(resp.Key, 0, resp.Val)
		} else if resp.Action == canal.UpdateAction {
			pipe.LRem(resp.Key, 0, resp.OldVal)
			pipe.RPush(resp.Key, resp.Val)
		} else {
			pipe.RPush(resp.Key, resp.Val)
		}
	case config.RedisStructureSet:
		if resp.Action == canal.DeleteAction {
			pipe.SRem(resp.Key, resp.Val)
		} else if resp.Action == canal.UpdateAction {
			pipe.SRem(resp.Key, 0, resp.OldVal)
			pipe.SAdd(resp.Key, resp.Val)
		} else {
			pipe.SAdd(resp.Key, resp.Val)
		}
	case config.RedisStructureSortedSet:
		if resp.Action == canal.DeleteAction {
			pipe.ZRem(resp.Key, resp.Val)
		} else if resp.Action == canal.UpdateAction {
			pipe.ZRem(resp.Key, 0, resp.OldVal)
			val := redis.Z{Score: resp.Score, Member: resp.Val}
			pipe.ZAdd(resp.Key, val)
		} else {
			val := redis.Z{Score: resp.Score, Member: resp.Val}
			pipe.ZAdd(resp.Key, val)
		}
	}
}

func (s *RedisEndpoint) encodeKey(req *model.RowRequest, rule *config.Details) string {
	if rule.Redis.RedisKeyValue != "" {
		return rule.Redis.RedisKeyValue
	}

	if rule.Redis.RedisKeyFormatter != "" {
		kv := rowMap(req, rule, true)
		var tmplBytes bytes.Buffer
		err := rule.Redis.RedisKeyTmpl.Execute(&tmplBytes, kv)
		if err != nil {
			return ""
		}
		return tmplBytes.String()
	}

	var key string
	if rule.Redis.RedisKeyColumnIndex < 0 {
		for _, v := range rule.Redis.RedisKeyColumnIndexs {
			key += stringutil.ToString(req.Row[v])
		}
	} else {
		key = stringutil.ToString(req.Row[rule.Redis.RedisKeyColumnIndex])
	}
	if rule.Redis.RedisKeyPrefix != "" {
		key = rule.Redis.RedisKeyPrefix + key
	}

	return key
}

func (s *RedisEndpoint) encodeHashField(req *model.RowRequest, rule *config.Details) string {
	var field string

	if rule.Redis.RedisHashFieldColumnIndex < 0 {
		for _, v := range rule.Redis.RedisHashFieldColumnIndexs {
			field += stringutil.ToString(req.Row[v])
		}
	} else {
		field = stringutil.ToString(req.Row[rule.Redis.RedisHashFieldColumnIndex])
	}

	if rule.Redis.RedisHashFieldPrefix != "" {
		field = rule.Redis.RedisHashFieldPrefix + field
	}

	return field
}

func (s *RedisEndpoint) encodeSortedSetScoreField(req *model.RowRequest, rule *config.Details) float64 {
	obj := req.Row[rule.Redis.RedisHashFieldColumnIndex]
	if obj == nil {
		return 0
	}

	str := stringutil.ToString(obj)
	return stringutil.ToFloat64Safe(str)
}

func (s *RedisEndpoint) Close() {
	if s.client != nil {
		s.client.Close()
	}
}
