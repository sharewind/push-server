package main

import (
	"flag"
	// "fmt"
	// "github.com/op/go-logging"
	// "log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	// "time"
)

var (
	flagSet          = flag.NewFlagSet("pushapi", flag.ExitOnError)
	showVersion      = flagSet.Bool("version", false, "print version string")
	httpAddress      = flagSet.String("http-address", "0.0.0.0:8501", "<addr>:<port> to listen on for HTTP clients")
	brokerTcpAddress = flagSet.String("broker-tcp-address", "", "<addr>:<port> to connect broker")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)

	flagSet.Parse(os.Args[1:])

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	pushAPI := NewPushAPI(httpAddress, brokerTcpAddress)
	pushAPI.Main()
	<-exitChan
	pushAPI.Exit()
}
