package main

import (
	"flag"
	"github.com/Hamster601/Budd/config"
	"github.com/Hamster601/Budd/pkg"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

var conf_path = flag.String("config", "./config/config.toml", "config file path")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse() //解析命令行参数

	quitSignal := make(chan os.Signal)
	signal.Notify(quitSignal, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	cfg, err := config.NewConfigWithFile(*conf_path)
	if err != nil {
		panic(err)
	}

	var isHA bool
	var h pkg.HA

	if cfg.Etcd.Enable {
		h, err = pkg.NewEtcd(cfg)
		if err != nil {
			log.Println("初始化etcd失败:", err)
			return
		}
		isHA = true
	}

	if isHA {
		if err = h.Lock(); err != nil {
			log.Printf(err.Error())
			return
		}
		defer func() {
			defer log.Print("----- close ha -----")
			h.UnLock()
			h.Close()
		}()
	}

	// 初始化budd
	budd, err := pkg.NewBin2es(cfg)
	if err != nil {
		panic(err)
	}

	finish := make(chan struct{})
	// 运行bin2es实例
	go func() {
		budd.Run()
		finish <- struct{}{}
	}()

	select {
	case n := <-quitSignal:
		log.Printf("receive signal:%v, quiting", n)
	case <-budd.Ctx().Done():
		log.Printf("context done, err:%v", budd.Ctx().Err())
	}

	budd.Close()
	<-finish
}
