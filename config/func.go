package config

import (
	"errors"
	"github.com/Hamster601/Budd/pkg/datefortmat"
	"github.com/Hamster601/Budd/pkg/file"
	"github.com/Hamster601/Budd/pkg/model"
	"github.com/Hamster601/Budd/pkg/stringutil"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/vmihailenco/msgpack"
	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"
	"html/template"
	"io/ioutil"
	"path/filepath"
	"strings"
)

func (c *Config) IsEtcd() bool {
	if c.EtcdConfig == nil {
		return false
	}
	if c.EtcdConfig.EtcdAddrs == "" {
		return false
	}
	return true
}

func (c *Config) IsCluster() bool {
	if !c.IsEtcd() {
		return false
	}

	return true
}

func (c *Config) Destination() string {
	var des string
	switch strings.ToUpper(c.Target) {
	case _targetRedis:
		des += "redis("
		des += c.RedisConfig.RedisAddr
		des += ")"
	case _targetKafka:
		des += "kafka("
		des += c.KafkaConfig.KafkaAddr
		des += ")"
	case _targetElasticsearch:
		des += "elasticsearch("
		des += c.ESConfig.ElsAddr
		des += ")"
	case _targetScript:
		des += "Lua Script"
	}
	return des
}


func RuleDeepClone(res *Details) (*Details, error) {
	data, err := msgpack.Marshal(res)
	if err != nil {
		return nil, err
	}

	var r Details
	err = msgpack.Unmarshal(data, &r)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func RuleKey(schema string, table string) string {
	return strings.ToLower(schema + ":" + table)
}

func AddRuleIns(ruleKey string, r *Details) {
	_lockOfRuleInsMap.Lock()
	defer _lockOfRuleInsMap.Unlock()

	_ruleInsMap[ruleKey] = r
}

func RuleIns(ruleKey string) (*Details, bool) {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	r, ok := _ruleInsMap[ruleKey]

	return r, ok
}

func RuleInsExist(ruleKey string) bool {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	_, ok := _ruleInsMap[ruleKey]

	return ok
}

func RuleInsTotal() int {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	return len(_ruleInsMap)
}

func RuleInsList() []*Details {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	list := make([]*Details, 0, len(_ruleInsMap))
	for _, rule := range _ruleInsMap {
		list = append(list, rule)
	}

	return list
}

func RuleKeyList() []string {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	list := make([]string, 0, len(_ruleInsMap))
	for k, _ := range _ruleInsMap {
		list = append(list, k)
	}

	return list
}

func (s *Details) Initialize() error {
	if err := s.buildPaddingMap(); err != nil {
		return err
	}

	if s.ValueEncoder == "" {
		s.ValueEncoder = ValEncoderJson
	}

	if s.ValueFormatter != "" {
		tmpl, err := template.New(s.TableInfo.Name).Parse(s.ValueFormatter)
		if err != nil {
			return err
		}
		s.ValueTmpl = tmpl
		s.ValueEncoder = ""
	}

	if s.DefaultColumnValueConfig != "" {
		dm := make(map[string]string)
		for _, t := range strings.Split(s.DefaultColumnValueConfig, ",") {
			tt := strings.Split(t, "=")
			if len(tt) != 2 {
				return errors.New("default_field_value format error in rule")
			}
			field := tt[0]
			value := tt[1]
			dm[field] = value
		}
		s.DefaultColumnValueMap = dm
	}

	if s.DateFormatter != "" {
		s.DateFormatter = datefortmat.ConvertGoFormat(s.DateFormatter)
	}

	if s.DateFormatter != "" {
		s.DateFormatter = datefortmat.ConvertGoFormat(s.DateFormatter)
	}

	if _config.IsRedis() {
		if err := s.initRedisConfig(); err != nil {
			return err
		}
	}

	if _config.IsKafka() {
		if err := s.initKafkaConfig(); err != nil {
			return err
		}
	}

	if _config.IsEls() {
		if err := s.initElsConfig(); err != nil {
			return err
		}
	}

	if _config.IsScript() {
		if s.LuaScript == "" && s.LuaFilePath == "" {
			return errors.New("empty lua script not allowed")
		}
	}

	return nil
}

func (s *Details) AfterUpdateTableInfo() error {
	if err := s.buildPaddingMap(); err != nil {
		return err
	}

	if _config.IsRedis() {
		if err := s.initRedisConfig(); err != nil {
			return err
		}
	}

	if _config.IsKafka() {
		if err := s.initKafkaConfig(); err != nil {
			return err
		}
	}

	if _config.IsEls() {
		if err := s.initElsConfig(); err != nil {
			return err
		}
	}

	if _config.IsScript() {
		if s.LuaScript == "" || s.LuaFilePath == "" {
			return errors.New("empty lua script not allowed")
		}
	}

	return nil
}

func (s *Details) buildPaddingMap() error {
	paddingMap := make(map[string]*model.Padding)
	mappings := make(map[string]string)

	if s.ColumnMappingConfigs != "" {
		ls := strings.Split(s.ColumnMappingConfigs, ",")
		for _, t := range ls {
			cmc := strings.Split(t, "=")
			if len(cmc) != 2 {
				return errors.New("column_mappings format error in rule")
			}
			column := cmc[0]
			mapped := cmc[1]
			_, index := s.TableColumn(column)
			if index < 0 {
				return errors.New("column_mappings must be table column")
			}
			mappings[strings.ToUpper(column)] = mapped
		}
	}

	if len(s.EsMappings) > 0 {
		for _, mapping := range s.EsMappings {
			mappings[strings.ToUpper(mapping.Column)] = mapping.Field
		}
	}

	var includes []string
	var excludes []string

	if s.IncludeColumnConfig != "" {
		includes = strings.Split(s.IncludeColumnConfig, ",")
	}
	if s.ExcludeColumnConfig != "" {
		excludes = strings.Split(s.ExcludeColumnConfig, ",")
	}

	if len(includes) > 0 {
		for _, c := range includes {
			_, index := s.TableColumn(c)
			if index < 0 {
				return errors.New("include_field must be table column")
			}
			paddingMap[c] = s.newPadding(mappings, c)
		}
	} else {
		for _, column := range s.TableInfo.Columns {
			include := true
			for _, exclude := range excludes {
				if column.Name == exclude {
					include = false
				}
			}
			if include {
				paddingMap[column.Name] = s.newPadding(mappings, column.Name)
			}
		}
	}

	s.PaddingMap = paddingMap

	return nil
}

func (s *Details) newPadding(mappings map[string]string, columnName string) *model.Padding {
	column, index := s.TableColumn(columnName)

	wrapName := s.WrapName(column.Name)
	mapped, exist := mappings[strings.ToUpper(column.Name)]
	if exist {
		wrapName = mapped
	}

	return &model.Padding{
		WrapName: wrapName,

		ColumnIndex:    index,
		ColumnName:     column.Name,
		ColumnType:     column.Type,
		ColumnMetadata: column,
	}
}

func (s *Details) TableColumn(field string) (*schema.TableColumn, int) {
	for index, c := range s.TableInfo.Columns {
		if strings.ToUpper(c.Name) == strings.ToUpper(field) {
			return &c, index
		}
	}
	return nil, -1
}

func (s *Details) WrapName(fieldName string) string {
	if s.ColumnUnderscoreToCamel {
		return stringutil.Case2Camel(strings.ToLower(fieldName))
	}
	if s.ColumnLowerCase {
		return strings.ToLower(fieldName)
	}
	if s.ColumnUpperCase {
		return strings.ToUpper(fieldName)
	}
	return fieldName
}

func (s *Details) LuaEnable() bool {
	if s.LuaScript == "" && s.LuaFilePath == "" {
		return false
	}

	return true
}

func (s *Details) initRedisConfig() error {
	if s.LuaEnable() {
		return nil
	}

	if s.Redis.RedisStructure == "" {
		return errors.New("empty redis_structure not allowed in rule")
	}

	switch strings.ToUpper(s.Redis.RedisStructure) {
	case "STRING":
		s.Redis.RedisStructure = RedisStructureString
		if s.Redis.RedisKeyColumn == "" && s.Redis.RedisKeyFormatter == "" {
			if s.Redis.IsCompositeKey {
				for _, v := range s.TableInfo.PKColumns {
					s.Redis.RedisKeyColumnIndexs = append(s.Redis.RedisKeyColumnIndexs, v)
				}
				s.Redis.RedisKeyColumnIndex = -1
			} else {
				s.Redis.RedisKeyColumnIndex = s.TableInfo.PKColumns[0]
			}
		}
	case "HASH":
		s.Redis.RedisStructure = RedisStructureHash
		if s.Redis.RedisKeyValue == "" {
			return errors.New("empty redis_key_value not allowed")
		}
		// init hash field
		if s.Redis.RedisHashFieldColumn == "" {
			if s.IsCompositeKey {
				for _, v := range s.TableInfo.PKColumns {
					s.Redis.RedisHashFieldColumnIndexs = append(s.Redis.RedisHashFieldColumnIndexs, v)
				}
				s.Redis.RedisHashFieldColumnIndex = -1
			} else {
				s.Redis.RedisHashFieldColumnIndex = s.TableInfo.PKColumns[0]
			}
		} else {
			_, index := s.TableColumn(s.Redis.RedisHashFieldColumn)
			if index < 0 {
				return errors.New("redis_hash_field_column must be table column")
			}
			s.Redis.RedisHashFieldColumnIndex = index
		}
	case "LIST":
		s.Redis.RedisStructure = RedisStructureList
		if s.Redis.RedisKeyValue == "" {
			return errors.New("empty redis_key_value not allowed in rule")
		}
	case "SET":
		s.Redis.RedisStructure = RedisStructureSet
		if s.Redis.RedisKeyValue == "" {
			return errors.New("empty redis_key_value not allowed in rule")
		}
	case "SORTEDSET":
		s.Redis.RedisStructure = RedisStructureSortedSet
		if s.Redis.RedisKeyValue == "" {
			return errors.New("empty redis_key_value not allowed in rule")
		}
		if s.Redis.RedisSortedSetScoreColumn == "" {
			return errors.New("empty redis_sorted_set_score_column not allowed in rule")
		}
		_, index := s.TableColumn(s.Redis.RedisSortedSetScoreColumn)
		if index < 0 {
			return errors.New("redis_sorted_set_score_column must be table column")
		}
		s.Redis.RedisHashFieldColumnIndex = index
	default:
		return errors.New("redis_structure must be string or hash or list or set")
	}

	if s.Redis.RedisKeyColumn != "" {
		_, index := s.TableColumn(s.Redis.RedisKeyColumn)
		if index < 0 {
			return errors.New("redis_key_column must be table column")
		}
		s.Redis.RedisKeyColumnIndex = index
		s.Redis.RedisKeyFormatter = ""
	}

	if s.Redis.RedisKeyFormatter != "" {
		tmpl, err := template.New(s.TableInfo.Name).Parse(s.Redis.RedisKeyFormatter)
		if err != nil {
			return err
		}
		s.Redis.RedisKeyTmpl = tmpl
		s.Redis.RedisKeyColumnIndex = -1
	}

	return nil
}

func (s *Details) initElsConfig() error {
	if s.ElsIndex == "" {
		s.ElsIndex = s.Table
	}

	if s.ElsType == "" {
		s.ElsType = "_doc"
	}

	if len(s.EsMappings) > 0 {
		for _, m := range s.EsMappings {
			if m.Field == "" {
				return errors.New("empty field not allowed in es_mappings")
			}
			if m.Type == "" {
				return errors.New("empty type not allowed in es_mappings")
			}
			if m.Column == "" && !s.LuaEnable() {
				return errors.New("empty column not allowed in es_mappings")
			}
		}
	}

	return nil
}

func (s *Details) initKafkaConfig() error {
	if !s.LuaEnable() {
		if s.KafkaTopic == "" {
			s.KafkaTopic = s.Table
		}
	}

	return nil
}

// 编译Lua
func (s *Details) CompileLuaScript(dataDir string) error {
	script := s.LuaScript
	if s.LuaFilePath != "" {
		var filePath string
		if file.IsExist(s.LuaFilePath) {
			filePath = s.LuaFilePath
		} else {
			filePath = filepath.Join(dataDir, s.LuaFilePath)
		}
		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			return err
		}
		script = string(data)
	}

	if script == "" {
		return errors.New("empty lua script not allowed")
	}

	s.LuaScript = script

	if _config.IsRedis() {
		if !strings.Contains(script, `require("redisOps")`) {
			return errors.New("lua script incorrect format")
		}

		if !(strings.Contains(script, `SET(`) ||
			strings.Contains(script, `HSET(`) ||
			strings.Contains(script, `RPUSH(`) ||
			strings.Contains(script, `SADD(`) ||
			strings.Contains(script, `ZADD(`) ||
			strings.Contains(script, `DEL(`) ||
			strings.Contains(script, `HDEL(`) ||
			strings.Contains(script, `LREM(`) ||
			strings.Contains(script, `ZREM(`) ||
			strings.Contains(script, `SREM(`)) {

			return errors.New("lua script incorrect format")
		}
	}

	if _config.IsEls() {
		if !strings.Contains(script, `require("esOps")`) {
			return errors.New("lua script incorrect format")
		}
	}

	reader := strings.NewReader(script)
	chunk, err := parse.Parse(reader, script)
	if err != nil {
		return err
	}

	var proto *lua.FunctionProto
	proto, err = lua.Compile(chunk, script)
	if err != nil {
		return err
	}

	s.LuaProto = proto

	return nil
}


func (c *Config) IsRedis() bool {
	return strings.ToUpper(c.Target) == _targetRedis
}

func (c *Config) IsKafka() bool {
	return strings.ToUpper(c.Target) == _targetKafka
}

func (c *Config) IsEls() bool {
	return strings.ToUpper(c.Target) == _targetElasticsearch
}

func (c *Config) IsScript() bool {
	return strings.ToUpper(c.Target) == _targetScript
}

func (c *Config) IsExporterEnable() bool {
	return c.EnableExporter
}


func (c *Config) IsMQ() bool {
	return c.isMQ
}
