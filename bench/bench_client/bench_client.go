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
	ip := os.Args[1]
	if ip == "" {
		fmt.Println("need ip")
		return
	}
	runtime.GOMAXPROCS(4)
	fmt.Println("client start!")

	for i := 0; i < 50000; i++ {

		go createClient(ip)
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

func createClient(ip string) {
	addr := ip + ":8600"

	client_id := int64(time.Now().UnixNano())
	c := client.NewClient(addr, client_id)
	err := c.Register(ip + ":4171")
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
