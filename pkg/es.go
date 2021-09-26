package pkg

import (
	"context"
	"errors"
	"log"
	"net/http"
	"syscall"
	"time"

	es7 "github.com/olivere/elastic/v7"
)

type MyES struct {
	client      *es7.Client
	bulkService *es7.BulkService
	ctx         context.Context
}

type MyRetrier struct {
	backoff es7.Backoff
}

func NewMyRetrier() *MyRetrier {
	return &MyRetrier{
		backoff: es7.NewConstantBackoff(time.Second),
	}
}

func (r *MyRetrier) Retry(ctx context.Context, retry int, req *http.Request, resp *http.Response, err error) (time.Duration, bool, error) {
	// 在一个特定的error上退出
	if err == syscall.ECONNREFUSED {
		return 0, false, errors.New("Elasticsearch or network down")
	}

	// 让 backoff 策略决定等待多久, 何时停止
	wait, stop := r.backoff.Next(retry)

	log.Printf("request es failed, retrying wait:%d  stop:%t", wait, stop)

	return wait, stop, nil
}

func (e *MyES) Close() {
	defer log.Print("----- ES closed -----")

	e.client.Stop()
}