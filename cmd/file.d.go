package main

import (
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/alecthomas/kingpin"
	"github.com/ozonru/file.d/cfg"
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/pipeline"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/automaxprocs/maxprocs"

	_ "github.com/ozonru/file.d/plugin/action/add_host"
	_ "github.com/ozonru/file.d/plugin/action/convert_date"
	_ "github.com/ozonru/file.d/plugin/action/debug"
	_ "github.com/ozonru/file.d/plugin/action/discard"
	_ "github.com/ozonru/file.d/plugin/action/flatten"
	_ "github.com/ozonru/file.d/plugin/action/join"
	_ "github.com/ozonru/file.d/plugin/action/json_decode"
	_ "github.com/ozonru/file.d/plugin/action/keep_fields"
	_ "github.com/ozonru/file.d/plugin/action/modify"
	_ "github.com/ozonru/file.d/plugin/action/parse_es"
	_ "github.com/ozonru/file.d/plugin/action/remove_fields"
	_ "github.com/ozonru/file.d/plugin/action/rename"
	_ "github.com/ozonru/file.d/plugin/action/throttle"
	_ "github.com/ozonru/file.d/plugin/input/dmesg"
	_ "github.com/ozonru/file.d/plugin/input/fake"
	_ "github.com/ozonru/file.d/plugin/input/file"
	_ "github.com/ozonru/file.d/plugin/input/http"
	_ "github.com/ozonru/file.d/plugin/input/journalctl"
	_ "github.com/ozonru/file.d/plugin/input/k8s"
	_ "github.com/ozonru/file.d/plugin/input/kafka"
	_ "github.com/ozonru/file.d/plugin/output/devnull"
	_ "github.com/ozonru/file.d/plugin/output/elasticsearch"
	_ "github.com/ozonru/file.d/plugin/output/gelf"
	_ "github.com/ozonru/file.d/plugin/output/kafka"
	_ "github.com/ozonru/file.d/plugin/output/stdout"
	_ "github.com/ozonru/file.d/plugin/output/file"
	_ "github.com/ozonru/file.d/plugin/output/s3"
)

var (
	fileD   *fd.FileD
	exit    = make(chan bool)
	version = "v0.0.1"

	config = kingpin.Flag("config", `config file name`).Required().ExistingFile()
	http   = kingpin.Flag("http", `http listen addr eg. ":9000", "off" to disable`).Default(":9000").String()

	gcPercent = 20
)

func main() {
	kingpin.Version(version)
	kingpin.Parse()

	logger.Infof("hi!")

	debug.SetGCPercent(gcPercent)
	insaneJSON.DisableBeautifulErrors = true
	insaneJSON.StartNodePoolSize = pipeline.DefaultJSONNodePoolSize

	_, _ = maxprocs.Set(maxprocs.Logger(logger.Debugf))

	go listenSignals()
	go start()

	<-exit
	logger.Infof("see you soon...")
}

func start() {
	fileD = fd.New(cfg.NewConfigFromFile(*config), *http)
	fileD.Start()
}

func listenSignals() {
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGTERM)

	for {
		s := <-signalChan

		switch s {
		case syscall.SIGHUP:
			logger.Infof("SIGHUP received")
			fileD.Stop()
			start()
		case syscall.SIGINT:
			fallthrough
		case syscall.SIGTERM:
			logger.Infof("SIGTERM received")
			fileD.Stop()
			exit <- true
		}
	}
}
