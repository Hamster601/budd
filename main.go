package main

import (
	"flag"
	"fmt"
	"github.com/Hamster601/Budd/config"
	"github.com/Hamster601/Budd/metric"
	"github.com/Hamster601/Budd/pkg/storage"
	"github.com/Hamster601/Budd/pkg/stringutil"
	"github.com/Hamster601/Budd/services"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"os"
	"os/signal"
	"regexp"
	"syscall"
)

var (
	helpFlag     bool
	cfgPath      string
	stockFlag    bool
	positionFlag bool
	statusFlag   bool
)

func init() {
	flag.BoolVar(&helpFlag, "help", false, "this help")
	flag.StringVar(&cfgPath, "config", "app.yml", "application config file")
	flag.BoolVar(&stockFlag, "stock", false, "stock data import")
	flag.BoolVar(&positionFlag, "position", false, "set dump position")
	flag.BoolVar(&statusFlag, "status", false, "display application status")
	flag.Usage = usage
}

func main() {

	flag.Parse()
	if helpFlag {
		flag.Usage()
		return
	}

	// 初始化global
	err := config.Initialize(cfgPath)
	if err != nil {
		log.Info(err.Error())
		return
	}

	if stockFlag {
		doStock()
		return
	}

	// 初始化Storage
	err = storage.Initialize(cfgPath)
	if err != nil {
		log.Info(err.Error())
		return
	}

	if statusFlag {
		doStatus()
		return
	}

	if positionFlag {
		doPosition()
		return
	}

	err = services.Initialize()
	if err != nil {
		println(err.Error())
		return
	}
	if err := metric.Initialize(); err != nil {
		println(err.Error())
		return
	}


	services.StartUp() // start application

	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Kill, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	sin := <-s
	log.Printf("application stoped，signal: %s \n", sin.String())

	services.Close()
	storage.Close()
}

func doStock() {
	stock := services.NewStockService()
	if err := stock.Run(); err != nil {
		println(err.Error())
	}
	stock.Close()
}

func doStatus() {
	ps := storage.NewPositionStorage()
	pos, _ := ps.Get()
	fmt.Printf("The current dump position is : %s %d \n", pos.Name, pos.Pos)
}

func doPosition() {
	others := flag.Args()
	if len(others) != 2 {
		println("error: please input the binlog's File and Position")
		return
	}
	f := others[0]
	p := others[1]

	matched, _ := regexp.MatchString(".+\\.\\d+$", f)
	if !matched {
		println("error: The parameter File must be like: mysql-bin.000001")
		return
	}

	pp, err := stringutil.ToUint32(p)
	if nil != err {
		println("error: The parameter Position must be number")
		return
	}
	ps := storage.NewPositionStorage()
	pos := mysql.Position{
		Name: f,
		Pos:  pp,
	}
	ps.Save(pos)
	fmt.Printf("The current dump position is : %s %d \n", f, pp)
}

func usage() {
	fmt.Fprintf(os.Stderr, `version: 0.0.1
	Usage: transfer [-c filename] [-s stock]
	Options:
	`)
	flag.PrintDefaults()
}
