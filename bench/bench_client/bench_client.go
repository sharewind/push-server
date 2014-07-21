package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	// "time"

	client "code.sohuno.com/kzapp/push-server/client"
)

var (
	flagSet          = flag.NewFlagSet("client", flag.ExitOnError)
	apiHttpAddress   = flagSet.String("api-http-address", "0.0.0.0:8501", "<addr>:<port> to listen on for HTTP clients")
	brokerTcpAddress = flagSet.String("broker-tcp-address", "0.0.0.0:8600", "<addr>:<port> to listen on for HTTP clients")
	subChannel       = flagSet.String("sub-channel", "28001", "client sub channel id")
	clientCount      = flagSet.Int64("client-count", 50000, "nums of client start")
	startID          = flagSet.Int64("start-id", 1, "client start id")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	flagSet.Parse(os.Args[1:])

	fmt.Println("client start!")

	clientChan := make(chan *client.Client, *clientCount)
	endID := *startID + *clientCount
	for i := *startID; i <= endID; i++ {
		go createClient(clientChan, i)
		log.Printf("start client  %d\n", i)
		// time.Sleep(5 * time.Millisecond)
	}

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-exitChan

	for c := range clientChan {
		c.Close()
		log.Printf("close client %d \n", c.ID)
	}
}

func createClient(clientChan chan *client.Client, clientID int64) {
	c := client.NewClient()
	c.ID = clientID
	c.BrokerAddr = *brokerTcpAddress
	c.SubChannel = *subChannel
	c.ConnectWithRetry()
	clientChan <- c
}
