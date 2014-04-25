package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"code.sohuno.com/kzapp/push-server/broker"
	"code.sohuno.com/kzapp/push-server/util"
	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
)

var (
	flagSet = flag.NewFlagSet("broker", flag.ExitOnError)

	// basic options
	config           = flagSet.String("config", "", "path to config file")
	showVersion      = flagSet.Bool("version", false, "print version string")
	verbose          = flagSet.Bool("verbose", false, "enable verbose logging")
	workerId         = flagSet.Int64("worker-id", 0, "unique identifier (int) for this worker (will default to a hash of hostname)")
	tcpAddress       = flagSet.String("tcp-address", "0.0.0.0:8600", "<addr>:<port> to listen on for TCP clients")
	httpAddress      = flagSet.String("http-address", "0.0.0.0:8601", "<addr>:<port> to listen on for HTTP clients")
	broadcastAddress = flagSet.String("broadcast-address", "", "address that will be locate client conn by worker")

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
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)

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

	opts := broker.NewBrokerOptions()
	options.Resolve(opts, flagSet, cfg)
	b := broker.NewBroker(opts)

	log.Println(util.Version("broker"))
	// log.Debug("worker id %d", opts.ID)

	b.Main()
	<-exitChan
	b.Exit()

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
