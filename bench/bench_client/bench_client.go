package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"code.sohuno.com/kzapp/push-server/client"
)

var (
	flagSet        = flag.NewFlagSet("client", flag.ExitOnError)
	apiHttpAddress = flagSet.String("api-http-address", "0.0.0.0:4171", "<addr>:<port> to listen on for HTTP clients")
	subChannel     = flagSet.Int64("sub-channel", int64(11111), "client sub channel id")
	clientCount    = flagSet.Int("client-count", 50000, "nums of client start")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	flagSet.Parse(os.Args[1:])

	fmt.Println("client start!")

	clientChan := make(chan *client.Client, *clientCount)
	for i := 0; i < *clientCount; i++ {
		go createClient(clientChan)
		log.Printf("start client  %d\n", i)
		time.Sleep(10 * time.Millisecond)
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

func createClient(clientChan chan *client.Client) {
	c := client.NewClient()
	c.AutoPump(*apiHttpAddress, *subChannel)
	clientChan <- c
}
