package main

import (
	"flag"
	"fmt"
	// "log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"code.sohuno.com/kzapp/push-server/broker"
	"code.sohuno.com/kzapp/push-server/model"
	"code.sohuno.com/kzapp/push-server/util"
	"code.sohuno.com/kzapp/push-server/worker"

	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
	"github.com/op/go-logging"
)

var (
	log = logging.MustGetLogger("main")

	config      = flagSet.String("config", "", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")

	workerHttpAddress = flagSet.String("worker-http-address", "0.0.0.0:8710", "<addr>:<port> to listen on for HTTP clients")
	brokerTcpAddress  = flagSet.String("broker-tcp-address", "", "<addr>:<port> to connect broker")
	flagSet           = flag.NewFlagSet("broker", flag.ExitOnError)

	// basic options
	verbose           = flagSet.Bool("verbose", false, "enable verbose logging")
	workerId          = flagSet.Int64("worker-id", 0, "unique identifier (int) for this worker (will default to a hash of hostname)")
	tcpAddress        = flagSet.String("tcp-address", "0.0.0.0:8600", "<addr>:<port> to listen on for TCP clients")
	brokerHttpAddress = flagSet.String("broker-http-address", "0.0.0.0:8601", "<addr>:<port> to listen on for HTTP clients")
	broadcastAddress  = flagSet.String("broadcast-address", "", "address that will be locate client conn by worker")

	maxBodySize = flagSet.Int64("max-body-size", 5*1024768, "maximum size of a single command body")

	// TLS config
	tlsCert = flagSet.String("tls-cert", "", "path to certificate file")
	tlsKey  = flagSet.String("tls-key", "", "path to private key file")

	// compression
	deflateEnabled  = flagSet.Bool("deflate", true, "enable deflate feature negotiation (client compression)")
	maxDeflateLevel = flagSet.Int("max-deflate-level", 6, "max deflate compression level a client can negotiate (> values == > nsqd CPU usage)")
	snappyEnabled   = flagSet.Bool("snappy", true, "enable snappy feature negotiation (client compression)")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 8)

	flagSet.Parse(os.Args[1:])

	if *showVersion {
		fmt.Println(util.Version("broker"))
		return
	}

	//
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
	b, c := RunBroker(flagSet, cfg)
	w := RunWorker(flagSet, cfg, c)

	// log.Debug("worker id %d", opts.ID)

	<-exitChan
	b.Exit()
	w.Stop()
	// if *memprofile != "" {
	// 	f, err := os.Create(*memprofile)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	pprof.WriteHeapProfile(f)
	// 	defer f.Close()
	// 	return
	// }
}

func RunBroker(flagSet *flag.FlagSet, cfg map[string]interface{}) (*broker.Broker, chan *model.PubMessage) {
	opts := broker.NewBrokerOptions()
	options.Resolve(opts, flagSet, cfg)
	log.Debug("broker options %#v", opts)

	b, c := broker.NewBroker(opts)

	log.Info(util.Version("broker"))
	b.Main()
	return b, c
}

func RunWorker(flagSet *flag.FlagSet, cfg map[string]interface{}, brokerPubChan chan *model.PubMessage) *worker.Worker {
	// runtime.GOMAXPROCS(runtime.NumCPU() * 8)
	// flagSet.Parse(os.Args[1:])

	// exitChan := make(chan int)
	// signalChan := make(chan os.Signal, 1)
	// go func() {
	// 	<-signalChan
	// 	exitChan <- 1
	// }()
	// signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// var cfg map[string]interface{}
	// if *config != "" {
	// 	_, err := toml.DecodeFile(*config, &cfg)
	// 	if err != nil {
	// 		log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
	// 	}
	// }

	opts := worker.NewWorkerOptions()
	options.Resolve(opts, flagSet, cfg)

	log.Debug("worker options %#v", opts)
	w := worker.NewWorker(opts, brokerPubChan)

	w.Main()
	log.Debug("opts.BrokerTcpAddress %s", opts.BrokerTcpAddress)
	w.SafeConnectToBroker(opts.BrokerTcpAddress)
	return w

	// <-exitChan
	// w.Stop()
}
