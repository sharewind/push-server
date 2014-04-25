package main

import (
	"flag"
	// "fmt"
	// "github.com/op/go-logging"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	// "time"

	"code.sohuno.com/kzapp/push-server/api"
)

var (
	flagSet = flag.NewFlagSet("pushapi", flag.ExitOnError)
	config  = flagSet.String("config", "", "path to config file")

	showVersion      = flagSet.Bool("version", false, "print version string")
	httpAddress      = flagSet.String("http-address", "0.0.0.0:4171", "<addr>:<port> to listen on for HTTP clients")
	brokerTcpAddress = flagSet.String("broker-tcp-address", "", "<addr>:<port> to connect broker")

	// pprof options
	cpuprofile = flag.String("cpuprofile", "api.prof", "write cpu profile to file")
	memprofile = flag.String("memprofile", "api.mprof", "write memory profile to this file")
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

	pushAPI := api.NewPushAPI(httpAddress, brokerTcpAddress)
	pushAPI.Main()
	<-exitChan
	pushAPI.Exit()
}
