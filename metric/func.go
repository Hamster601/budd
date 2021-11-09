package metric

import (
	"github.com/go-mysql-org/go-mysql/canal"
)


func SetLeaderState(state int,isExport bool) {
	if isExport {
		LeaderStateGauge.Set(float64(state))
	}
}

func SetDestState(state int,isExport bool) {
	if isExport {
		DestStateGauge.Set(float64(state))
	}

}

func DestState() bool {
	return destState.Load()
}

func SetTransferDelay(d uint32,isExport bool) {
	if isExport {
		DelayGauge.Set(float64(d))
	}
}

func UpdateActionNum(action, lab string,isExport bool) {
	if isExport {
		switch action {
		case canal.InsertAction:
			InsertCounter.WithLabelValues(lab).Inc()
		case canal.UpdateAction:
			UpdateCounter.WithLabelValues(lab).Inc()
		case canal.DeleteAction:
			DeleteCounter.WithLabelValues(lab).Inc()
		}
	}
}

func InsertAmount() uint64 {
	var amount uint64
	for _, v := range insertRecord {
		amount += v.Load()
	}
	return amount
}

func UpdateAmount() uint64 {
	var amount uint64
	for _, v := range updateRecord {
		amount += v.Load()
	}
	return amount
}

func DeleteAmount() uint64 {
	var amount uint64
	for _, v := range deleteRecord {
		amount += v.Load()
	}
	return amount
}

func LabInsertAmount(lab string) uint64 {
	var nn uint64
	n, ok := insertRecord[lab]
	if ok {
		nn = n.Load()
	}
	return nn
}

func LabUpdateRecord(lab string) uint64 {
	var nn uint64
	n, ok := updateRecord[lab]
	if ok {
		nn = n.Load()
	}
	return nn
}

func LabDeleteRecord(lab string) uint64 {
	var nn uint64
	n, ok := deleteRecord[lab]
	if ok {
		nn = n.Load()
	}
	return nn
}

func LeaderFlag() bool {
	return leaderState.Load()
}
