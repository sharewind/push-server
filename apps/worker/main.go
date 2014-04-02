package main

import (
	"code.sohuno.com/kzapp/push-server/worker"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	httpAddr := "localhost:8710"
	w := worker.NewWorker(httpAddr)
	w.Main()
	w.ConnectToBroker("localhost:8600")

	<-exitChan

}
