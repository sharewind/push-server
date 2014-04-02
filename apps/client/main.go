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

	addr := "localhost:8600"
	client_id := int64(185980656)
	c := client.NewClient(addr, client_id)
	c.Connect()

	channel_id := int64(1001)
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
