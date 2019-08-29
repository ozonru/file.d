package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kingpin"
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/automaxprocs/maxprocs"

	_ "gitlab.ozon.ru/sre/filed/plugin/action/discard"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/k8s"
	_ "gitlab.ozon.ru/sre/filed/plugin/input/fake"
	_ "gitlab.ozon.ru/sre/filed/plugin/input/file"
	_ "gitlab.ozon.ru/sre/filed/plugin/output/devnull"
)

var (
	fd      *filed.Filed
	done    = make(chan bool)
	version = "v0.0.1"

	config = kingpin.Flag("config", `config file name`).Required().ExistingFile()
	http   = kingpin.Flag("http", `http listen addr eg. ":9000", "off" to disable`).Default(":9000").String()
)

func main() {
	kingpin.Version(version)
	kingpin.Parse()

	logger.Infof("hi!")

	_, _ = maxprocs.Set(maxprocs.Logger(logger.Debugf))

	go listenSignals()
	go start()

	<-done
}

func start() () {
	fd := filed.New(filed.NewConfigFromFile(*config), *http)
	fd.Start()
}

func listenSignals() {
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGTERM)

	for {
		s := <-signalChan

		switch s {
		case syscall.SIGHUP:
			logger.Infof("signal SIGHUP received")
			reload()
		case syscall.SIGTERM:
			logger.Infof("signal SIGTERM received")
			stop()
			<-done
		}
	}
}

func reload() {
	stop()
	start()
}

func stop() {
	fd.Stop()
}
