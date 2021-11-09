package config

import (
	"fmt"
	"github.com/Hamster601/Budd/pkg/logs"
	sidlog "github.com/siddontang/go-log/log"
	"log"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

var (
	pid         int
	coordinator int
	_leaderFlag bool
	_leaderNode string
	currentNode string
	_bootTime   time.Time
)

func SetLeaderFlag(flag bool) {
	_leaderFlag = flag
}

func IsLeader() bool {
	return _leaderFlag
}

func SetLeaderNode(leader string) {
	_leaderNode = leader
}

func LeaderNode() string {
	return _leaderNode
}

func CurrentNode() string {
	return currentNode
}

func IsFollower() bool {
	return !_leaderFlag
}

func BootTime() time.Time {
	return _bootTime
}

func Initialize(configPath string) error {
	config, err := NewConfig(configPath)
	if err != nil {
		return err
	}
	runtime.GOMAXPROCS(config.Maxprocs)

	// 初始化global logger
	if err := logs.Initialize(config.LoggerConfig); err != nil {
		return err
	}

	streamHandler, err := sidlog.NewStreamHandler(logs.Writer())
	if err != nil {
		return err
	}
	agent := sidlog.New(streamHandler, sidlog.Ltime|sidlog.Lfile|sidlog.Llevel)
	sidlog.SetDefaultLogger(agent)

	_bootTime = time.Now()
	pid = syscall.Getpid()

	if config.IsCluster() {
		currentNode = config.EtcdConfig.BindIP + ":" + strconv.Itoa(pid)
	}

	log.Println(fmt.Sprintf("process id: %d", pid))
	log.Println(fmt.Sprintf("GOMAXPROCS :%d", config.Maxprocs))
	log.Println(fmt.Sprintf("source  %s(%s)", config.Flavor, config.Addr))
	log.Println(fmt.Sprintf("destination %s", config.Destination()))

	return nil
}
