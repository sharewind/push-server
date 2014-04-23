package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"code.sohuno.com/kzapp/push-server/client"
)

var (
	flagSet          = flag.NewFlagSet("client", flag.ExitOnError)
	apiHttpAddress   = flagSet.String("api-http-address", "0.0.0.0:4171", "<addr>:<port> to listen on for HTTP clients")
	brokerTcpAddress = flagSet.String("broker-tcp-address", "0.0.0.0:8600", "<addr>:<port> to connect broker")
)

func main() {
	flagSet.Parse(os.Args[1:])
	fmt.Println("client start!")

	client_id := int64(451294706224070111)
	c := client.NewClient(*brokerTcpAddress, client_id)
	c.Register(*apiHttpAddress)
	c.Connect()

	channel_id := int64(11111)
	c.Subscribe(channel_id)

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
