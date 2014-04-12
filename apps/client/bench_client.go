package main

import (
	"code.sohuno.com/kzapp/push-server/client"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	// "time"
)

func main() {
	runtime.GOMAXPROCS(4)
	fmt.Println("client start!")

	for i := 0; i < 10000; i++ {
<<<<<<< HEAD
		go wrapperFunc()
		fmt.Println("connection: ", i)
=======
		go createClient()
		fmt.Println(i)
>>>>>>> 96f3fffc95d61df0a52ed811283a5221e71e8106
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

<<<<<<< HEAD
func wrapperFunc() {
	defer func() {
		fmt.Println("warp func done !")
	}()
	go createClient()

}

var count = 0

=======
>>>>>>> 96f3fffc95d61df0a52ed811283a5221e71e8106
func createClient() {
	defer func() {
		fmt.Println("createClient func done !")
	}()
	addr := "10.2.58.178:8600"

	client_id := int64(451294706224070657)
	c := client.NewClient(addr, client_id)
	err := c.Register("10.2.58.178:4171")
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
	channel_id := int64(1001)
	err = c.Subscribe(channel_id)
	if err != nil {
		fmt.Println("error3:", err.Error())
		return
	}
}
