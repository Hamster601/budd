package config

import (
	"github.com/Hamster601/Budd/pkg/model"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/yuin/gopher-lua"
	"html/template"
)

type EsMappingConfig struct {
	Column   string `yaml:"column"`   // 数据库列名称
	Field    string `yaml:"field"`    // 映射后的ES字段名称
	Type     string `yaml:"type"`     // ES字段类型
	Analyzer string `yaml:"analyzer"` // ES分词器
	Format   string `yaml:"format"`   // 日期格式
}

type Details struct {
	Schema                   string `yaml:"schema"`
	Table                    string `yaml:"table"`
	OrderByColumn            string `yaml:"order_by_column"`
	ColumnLowerCase          bool   `yaml:"column_lower_case"`          // 列名称转为小写
	ColumnUpperCase          bool   `yaml:"column_upper_case"`          // 列名称转为大写
	ColumnUnderscoreToCamel  bool   `yaml:"column_underscore_to_camel"` // 列名称下划线转驼峰
	IncludeColumnConfig      string `yaml:"include_columns"`            // 包含的列
	ExcludeColumnConfig      string `yaml:"exclude_columns"`            // 排除掉的列
	ColumnMappingConfigs     string `yaml:"column_mappings"`            // 列名称映射
	DefaultColumnValueConfig string `yaml:"default_column_values"`      // 默认的字段和值
	ValueEncoder             string `yaml:"value_encoder"`              // #值编码，支持json、kv-commas、v-commas；默认为json；json形如：{"id":123,"name":"wangjie"} 、kv-commas形如：id=123,name="wangjie"、v-commas形如：123,wangjie
	ValueFormatter           string `yaml:"value_formatter"`            //格式化定义key,{id}表示字段id的值、{name}表示字段name的值

	LuaScript                string `yaml:"lua_script"`                 //lua 脚本
	LuaFilePath              string `yaml:"lua_file_path"`              //lua 文件地址
	DateFormatter            string `yaml:"date_formatter"`             //date类型格式化， 不填写默认2006-01-02

	// ------------------- Kafka -----------------
	KafkaTopic string `yaml:"kafka_topic"` //TOPIC名称,可以为空，默认使用表(Table)名称
	ReserveRawData bool `yaml:"reserve_raw_data"` // 保留update之前的数据，针对KAFKA、RABBITMQ、ROCKETMQ有效

	// ------------------- ES -----------------
	ElsIndex   string             `yaml:"es_index"`    //Elasticsearch Index,可以为空，默认使用表(Table)名称
	ElsType    string             `yaml:"es_type"`     //es6.x以后一个Index只能拥有一个Type,可以为空，默认使用_doc; es7.x版本此属性无效
	EsMappings []*EsMappingConfig `yaml:"es_mappings"` //Elasticsearch mappings映射关系,可以为空，为空时根据数据类型自己推导
	Redis      RedisValue         `yaml:"redis"`

	TableInfo             *schema.Table
	TableColumnSize       int
	IsCompositeKey        bool //是否联合主键
	DefaultColumnValueMap map[string]string
	PaddingMap            map[string]*model.Padding

	LuaProto    *lua.FunctionProto
	LuaFunction *lua.LFunction
	ValueTmpl   *template.Template
}

type RedisValue struct {
	//对应redis的5种数据类型 String、Hash(字典) 、List(列表) 、Set(集合)、Sorted Set(有序集合)
	RedisStructure string `yaml:"redis_structure"`
	RedisKeyPrefix string `yaml:"redis_key_prefix"` //key的前缀
	RedisKeyColumn string `yaml:"redis_key_column"` //使用哪个列的值作为key，不填写默认使用主键
	// 格式化定义key,如{id}-{name}；{id}表示字段id的值、{name}表示字段name的值
	RedisKeyFormatter string `yaml:"redis_key_formatter"`
	RedisKeyValue     string `yaml:"redis_key_value"` // key的值，固定值
	// hash的field前缀，仅redis_structure为hash时起作用
	RedisHashFieldPrefix string `yaml:"redis_hash_field_prefix"`
	// 使用哪个列的值作为hash的field，仅redis_structure为hash时起作用
	RedisHashFieldColumn string `yaml:"redis_hash_field_column"`
	// Sorted Set(有序集合)的Score
	RedisSortedSetScoreColumn      string `yaml:"redis_sorted_set_score_column"`
	RedisKeyColumnIndex            int
	RedisKeyColumnIndexs           []int
	RedisHashFieldColumnIndex      int
	RedisHashFieldColumnIndexs     []int
	RedisSortedSetScoreColumnIndex int
	RedisKeyTmpl                   *template.Template
	IsCompositeKey                 bool
}
