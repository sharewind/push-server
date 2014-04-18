package main

import (
	"code.sohuno.com/kzapp/push-server/worker"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
	"github.com/op/go-logging"
)

var (
	log     = logging.MustGetLogger("main")
	flagSet = flag.NewFlagSet("worker", flag.ExitOnError)

	config      = flagSet.String("config", "./worker.cfg", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")

	httpAddress      = flagSet.String("http-address", "", "<addr>:<port> to listen on for HTTP clients")
	brokerTcpAddress = flagSet.String("broker-tcp-address", "", "<addr>:<port> to connect broker")
	templateDir      = flagSet.String("template-dir", "", "path to templates directory")
)

func main() {
	flagSet.Parse(os.Args[1:])

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// httpAddr := "0.0.0.0:8710"

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}

	opts := worker.NewWorkerOptions()

	options.Resolve(opts, flagSet, cfg)

	w := worker.NewWorker(opts)
	w.Main()
	log.Debug("opts.BrokerTcpAddress %s", opts.BrokerTcpAddress)
	w.SafeConnectToBroker(opts.BrokerTcpAddress)

	<-exitChan
	w.Stop()
}
