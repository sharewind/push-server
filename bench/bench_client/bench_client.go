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

	for i := 0; i < 1000; i++ {

		go createClient()
		fmt.Println(i)

		// runtime.Gosched()
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
	// c.Close()

}

var count = 0

func createClient() {
	addr := "localhost:8600"

	client_id := int64(451294706224070657)
	c := client.NewClient(addr, client_id)
	err := c.Register("10.10.79.134:4171")
	if err != nil {
		fmt.Println("error1:", err.Error())
		count++
		fmt.Println("timeoutcount ", count)
		return
	}
	err = c.Connect()
	if err != nil {
		fmt.Println("error2:", err.Error())
		return
	}
	channel_id := int64(11111)
	err = c.Subscribe(channel_id)
	if err != nil {
		fmt.Println("error3:", err.Error())
		return
	}
}
