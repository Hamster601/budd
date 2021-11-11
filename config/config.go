package config

import (
	err "github.com/Hamster601/Budd/errors"
	"github.com/Hamster601/Budd/pkg/logs"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"runtime"
)

// 应用相关配置
type Config struct {
	Target string `yaml:"target"`
	// 数据库地址
	Addr         string       `yaml:"addr"`
	User         string       `yaml:"user"`
	Password     string       `yaml:"pass"`
	Charset      string       `yaml:"charset"`
	Flavor       string       `yaml:"flavor"`
	LoggerConfig *logs.Config `yaml:"logger"` // 日志配置
	SlaveID      uint32       `yaml:"slave_id"`
	DumpExec     string       `yaml:"dump_exec"`

	EnableExporter bool `yaml:"enable_exporter"` // 启用prometheus exporter，默认false
	ExporterPort   int  `yaml:"exporter_addr"`   // prometheus exporter端口

	Maxprocs int `yaml:"maxprocs"` // 最大协程数，默认CPU核心数*2

	DataDir    string    `yaml:"data_dir"`
	RuleConfig []Details `yaml:"rule_config"`

	// target
	RedisConfig       *Redis `yaml:"redis"`
	KafkaConfig       *Kafka `yaml:"kafka"`
	ESConfig          *ES    `yaml:"ES"`
	EtcdConfig        *Etcd  `yaml:"etcd"`
	isMQ              bool   `yaml:"is_mq"`
	isReserveRawData  bool   //保留原始数据
	FlushBulkInterval int    `yaml:"flush_bulk_interval"`
	BulkSize          int64  `yaml:"bulk_size"`
	SkipMasterData    bool   `yaml:"skip_master_data"`
	SkipNoPkTable     bool   `yaml:"skip_no_pk_table"`
}

// kafka配置
type Kafka struct {
	KafkaAddr         string `yaml:"kafka_addrs"`         //kafka连接地址，多个用逗号分隔
	KafkaSASLUser     string `yaml:"kafka_sasl_user"`     //kafka SASL_PLAINTEXT认证模式 用户名
	KafkaSASLPassword string `yaml:"kafka_sasl_password"` //kafka SASL_PLAINTEXT认证模式 密码
}

// ES配置
type ES struct {
	Name        string `yaml:"name"`
	ElsAddr     string `yaml:"es_addrs"`    //Elasticsearch连接地址，多个用逗号分隔
	ElsUser     string `yaml:"es_user"`     //Elasticsearch用户名
	ElsPassword string `yaml:"es_password"` //Elasticsearch密码
	ElsVersion  int    `yaml:"es_version"`  //Elasticsearch版本，支持6和7、默认为7
}

type Etcd struct {
	BindIP       string `yaml:"bind_id"`
	EtcdAddrs    string `yaml:"etcd_addrs"`
	EtcdUser     string `yaml:"etcd_user"`
	EtcdPassword string `yaml:"etcd_password"`

	EtcdFilePath string `yaml:"etcd_file_path"`
}

type Redis struct {
	// ------------------- REDIS -----------------
	RedisAddr       string `yaml:"redis_addrs"`       //redis地址
	RedisGroupType  string `yaml:"redis_group_type"`  //集群类型 sentinel或者cluster
	RedisMasterName string `yaml:"redis_master_name"` //Master节点名称
	RedisPass       string `yaml:"redis_pass"`        //redis密码
	RedisDatabase   int    `yaml:"redis_database"`    //redis数据库
}

// NewConfig初始化配置
func NewConfig(fileName string) (*Config, error) {
	if fileName == "" {
		return nil, err.NoFileError
	}

	data, err1 := ioutil.ReadFile(fileName)
	if err1 != nil {
		return nil, err1
	}

	var c Config

	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	defaultConfig := DefaultConfig(&c)
	InitConfig = defaultConfig
	return defaultConfig, nil
}

func DefaultConfig(c *Config) *Config {
	if c.ExporterPort == 0 {
		c.ExporterPort = 9595
	}
	if c.Maxprocs <= 0 {
		c.Maxprocs = runtime.NumCPU() * 2
	}
	return c
}
