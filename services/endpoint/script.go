package endpoint

import (
	"errors"
	"fmt"
	"github.com/Hamster601/Budd/config"
	"github.com/Hamster601/Budd/metric"
	"github.com/Hamster601/Budd/pkg/logs"
	"github.com/Hamster601/Budd/pkg/model"
	"github.com/Hamster601/Budd/services/luaengine"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
)

type ScriptEndpoint struct {
}

func newScriptEndpoint() *ScriptEndpoint {
	return &ScriptEndpoint{}
}

func (s *ScriptEndpoint) Connect() error {
	return nil
}

func (s *ScriptEndpoint) Ping() error {
	return nil
}

func (s *ScriptEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	for _, row := range rows {
		rule, _ := config.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		metric.UpdateActionNum(row.Action, row.RuleKey,true)
		kvm := rowMap(row, rule, true)
		err := luaengine.DoScript(kvm, row.Action, rule)
		if err != nil {
			log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
			return errors.New(fmt.Sprintf("Lua 脚本执行失败 : %s ", err.Error()))
		}
		kvm = nil
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *ScriptEndpoint) Stock(rows []*model.RowRequest) int64 {
	var counter int64
	for _, row := range rows {
		rule, _ := config.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		kvm := rowMap(row, rule, true)
		err := luaengine.DoScript(kvm, row.Action, rule)
		if err != nil {
			logs.Errorf(fmt.Sprintf("lua 脚本执行失败 : %s ", err.Error()))
			break
		}
		counter++
	}

	return counter
}

func (s *ScriptEndpoint) Close() {

}
