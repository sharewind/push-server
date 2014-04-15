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

	httpAddr := "0.0.0.0:8710"
	w := worker.NewWorker(httpAddr)
	w.Main()
	w.ConnectToBroker("127.0.0.1:8600")

	<-exitChan
	w.Stop()
}
