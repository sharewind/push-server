package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"code.sohuno.com/kzapp/push-server/client"
)

var (
	flagSet        = flag.NewFlagSet("client", flag.ExitOnError)
	apiHttpAddress = flagSet.String("api-http-address", "0.0.0.0:8501", "<addr>:<port> to listen on for HTTP clients")
	subChannel     = flagSet.Int64("sub-channel", int64(11111), "client sub channel id")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	flagSet.Parse(os.Args[1:])
	fmt.Println("client start!")

	c := client.NewClient()
	c.AutoPump(*apiHttpAddress, *subChannel)

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-exitChan
	c.Close()

}
