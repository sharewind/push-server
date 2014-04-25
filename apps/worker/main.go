package main

import (
	"code.sohuno.com/kzapp/push-server/worker"
	"flag"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
	"github.com/op/go-logging"
)

var (
	log     = logging.MustGetLogger("main")
	flagSet = flag.NewFlagSet("worker", flag.ExitOnError)

	// pprof options
	cpuprofile = flag.String("cpuprofile", "worker.prof", "write cpu profile to file")
	memprofile = flag.String("memprofile", "worker.mprof", "write memory profile to this file")

	config      = flagSet.String("config", "", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")

	httpAddress      = flagSet.String("http-address", "0.0.0.0:8710", "<addr>:<port> to listen on for HTTP clients")
	brokerTcpAddress = flagSet.String("broker-tcp-address", "", "<addr>:<port> to connect broker")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	flagSet.Parse(os.Args[1:])

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}

	opts := worker.NewWorkerOptions()
	options.Resolve(opts, flagSet, cfg)

	log.Debug("options %#v", opts)
	w := worker.NewWorker(opts)

	w.Main()
	log.Debug("opts.BrokerTcpAddress %s", opts.BrokerTcpAddress)
	w.SafeConnectToBroker(opts.BrokerTcpAddress)

	<-exitChan
	w.Stop()
}
