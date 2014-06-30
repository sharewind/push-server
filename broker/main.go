package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"code.sohuno.com/kzapp/push-server/util"
	"github.com/mreiferson/go-options"
)

var (
	flagSet = flag.NewFlagSet("broker", flag.ExitOnError)

	// basic options
	showVersion      = flagSet.Bool("version", false, "print version string")
	verbose          = flagSet.Bool("verbose", false, "enable verbose logging")
	workerId         = flagSet.Int64("worker-id", 0, "unique identifier (int) for this worker (will default to a hash of hostname)")
	tcpAddress       = flagSet.String("tcp-address", "0.0.0.0:8600", "<addr>:<port> to listen on for TCP clients")
	httpAddress      = flagSet.String("http-address", "0.0.0.0:8601", "<addr>:<port> to listen on for HTTP clients")
	broadcastAddress = flagSet.String("broadcast-address", "", "address that will be locate client conn by worker")

	maxBodySize = flagSet.Int64("max-body-size", 5*1024768, "maximum size of a single command body")
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

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
	opts := NewBrokerOptions()
	options.Resolve(opts, flagSet, cfg)
	b := NewBroker(opts)

	log.Println(util.Version("broker"))
	// log.Debug("worker id %d", opts.ID)

	b.Main()
	<-exitChan
	b.Exit()
}
