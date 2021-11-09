package config

import "sync"

const (
	_targetRedis         = "REDIS"
	_targetMongodb       = "MONGODB"
	_targetRocketmq      = "ROCKETMQ"
	_targetRabbitmq      = "RABBITMQ"
	_targetKafka         = "KAFKA"
	_targetElasticsearch = "ELASTICSEARCH"
	_targetScript        = "SCRIPT"

	RedisGroupTypeSentinel = "sentinel"
	RedisGroupTypeCluster  = "cluster"

	_dataDir = "store"

	_zkRootDir = "/transfer" // ZooKeeper and Etcd root

	_flushBulkInterval = 200
	_flushBulkSize     = 100

	// update or insert
	UpsertAction = "upsert"

    // redis数据类型
	RedisStructureString    = "String"
	RedisStructureHash      = "Hash"
	RedisStructureList      = "List"
	RedisStructureSet       = "Set"
	RedisStructureSortedSet = "SortedSet"

	ValEncoderJson     = "json"
	ValEncoderKVCommas = "kv-commas"
	ValEncoderVCommas  = "v-commas"
)


var (
	_ruleInsMap       = make(map[string]*Details)
	_lockOfRuleInsMap sync.RWMutex
     _config *Config
	InitConfig *Config
)


