package endpoint

import (
	"errors"
	"fmt"
	"github.com/Hamster601/Budd/config"
	"github.com/Hamster601/Budd/metric"
	"github.com/Shopify/sarama"

	"github.com/Hamster601/Budd/pkg/logs"
	"github.com/Hamster601/Budd/pkg/model"
	"github.com/Hamster601/Budd/services/luaengine"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"strings"
	"sync"
)

type KafkaEndpoint struct {
	client   sarama.Client
	producer sarama.AsyncProducer

	retryLock sync.Mutex
}

func newKafkaEndpoint() *KafkaEndpoint {
	r := &KafkaEndpoint{}
	return r
}

func (s *KafkaEndpoint) Connect() error {
	cfg := sarama.NewConfig()
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner

	if config.InitConfig.KafkaConfig.KafkaSASLUser != "" && config.InitConfig.KafkaConfig.KafkaSASLPassword != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = config.InitConfig.KafkaConfig.KafkaSASLUser
		cfg.Net.SASL.Password = config.InitConfig.KafkaConfig.KafkaSASLPassword
	}

	var err error
	var client sarama.Client
	ls := strings.Split(config.InitConfig.KafkaConfig.KafkaSASLUser, ",")
	client, err = sarama.NewClient(ls, cfg)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to create kafka client: %s", err.Error()))
	}

	var producer sarama.AsyncProducer
	producer, err = sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to create kafka producer: %s", err.Error()))
	}

	s.producer = producer
	s.client = client

	return nil
}

func (s *KafkaEndpoint) Ping() error {
	return s.client.RefreshMetadata()
}

func (s *KafkaEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	var ms []*sarama.ProducerMessage
	for _, row := range rows {
		rule, _ := config.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		metric.UpdateActionNum(row.Action, row.RuleKey, config.InitConfig.EnableExporter)

		if rule.LuaEnable() {
			ls, err := s.buildMessages(row, rule)
			if err != nil {
				log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
				return errors.New(fmt.Sprintf("lua 脚本执行失败 : %s ", err.Error()))
			}
			ms = append(ms, ls...)
		} else {
			m, err := s.buildMessage(row, rule)
			if err != nil {
				return errors.New(fmt.Sprintf("创建消息失败，错误：%s", err.Error()))
			}
			ms = append(ms, m)
		}
	}

	for _, m := range ms {
		s.producer.Input() <- m
		select {
		case err := <-s.producer.Errors():
			return err
		default:
		}
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *KafkaEndpoint) Stock(rows []*model.RowRequest) int64 {
	expect := true
	for _, row := range rows {
		rule, _ := config.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaEnable() {
			ls, err := s.buildMessages(row, rule)
			if err != nil {
				logs.Errorf(err.Error())
				expect = false
				break
			}
			for _, m := range ls {
				s.producer.Input() <- m
				select {
				case err := <-s.producer.Errors():
					logs.Error(err.Error())
					expect = false
					break
				default:
				}
			}
			if !expect {
				break
			}
		} else {
			m, err := s.buildMessage(row, rule)
			if err != nil {
				logs.Errorf(err.Error())
				expect = false
				break
			}
			s.producer.Input() <- m
			select {
			case err := <-s.producer.Errors():
				logs.Error(err.Error())
				expect = false
				break
			default:

			}
		}
	}

	if !expect {
		return 0
	}

	return int64(len(rows))
}

func (s *KafkaEndpoint) buildMessages(row *model.RowRequest, rule *config.Details) ([]*sarama.ProducerMessage, error) {
	kvm := rowMap(row, rule, true)
	ls, err := luaengine.DoMQOps(kvm, row.Action, rule)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("lua 脚本执行失败 : %s ", err.Error()))
	}

	var ms []*sarama.ProducerMessage
	for _, resp := range ls {
		m := &sarama.ProducerMessage{
			Topic: resp.Topic,
			Value: sarama.ByteEncoder(resp.ByteArray),
		}
		logs.Infof("topic: %s, message: %s", resp.Topic, string(resp.ByteArray))
		ms = append(ms, m)
	}

	return ms, nil
}

func (s *KafkaEndpoint) buildMessage(row *model.RowRequest, rule *config.Details) (*sarama.ProducerMessage, error) {
	kvm := rowMap(row, rule, false)
	resp := new(model.MQRespond)
	resp.Action = row.Action
	resp.Timestamp = row.Timestamp
	if rule.ValueEncoder == config.ValEncoderJson {
		resp.Date = kvm
	} else {
		resp.Date = encodeValue(rule, kvm)
	}

	if rule.ReserveRawData && canal.UpdateAction == row.Action {
		resp.Raw = oldRowMap(row, rule, false)
	}

	body, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}
	m := &sarama.ProducerMessage{
		Topic: rule.KafkaTopic,
		Value: sarama.ByteEncoder(body),
	}
	logs.Infof("topic: %s, message: %s", rule.KafkaTopic, string(body))
	return m, nil
}

func (s *KafkaEndpoint) Close() {
	if s.producer != nil {
		s.producer.Close()
	}
	if s.client != nil {
		s.client.Close()
	}
}
