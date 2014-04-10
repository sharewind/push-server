package main

import (
	"code.sohuno.com/kzapp/push-server/client"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

func main() {
	runtime.GOMAXPROCS(4)
	fmt.Println("client start!")

	for i := 0; i < 10000; i++ {
		go wrapperFunc()
		fmt.Println(i)
		// runtime.Gosched()
		time.Sleep(1 * time.Millisecond)

	}

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-exitChan
	// c.Close()

}

func wrapperFunc() {
	defer func() {
		fmt.Println("warp func done !")
	}()
	go createClient()

}

func createClient() {
	defer func() {
		fmt.Println("createClient func done !")
	}()
	addr := "10.2.58.178:8600"

	client_id := int64(451294706224070657)
	c := client.NewClient(addr, client_id)
	c.Register("10.2.58.178:4171")
	c.Connect()

	channel_id := int64(1001)
	c.Subscribe(channel_id)
}
