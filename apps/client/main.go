package main

import (
	"code.sohuno.com/kzapp/push-server/client"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	fmt.Println("client start!")

	addr := "10.10.79.134:8600"
	client_id := int64(451294706224070111)
	c := client.NewClient(addr, client_id)
	c.Register("10.10.79.134:4171")
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
