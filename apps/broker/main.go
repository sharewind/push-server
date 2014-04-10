package main

import (
	"code.sohuno.com/kzapp/push-server/broker"
	"os"
	"os/signal"
	"syscall"
)

//sh /opt/scripts/mongodb/mongodb.sh start
//sh /opt/scripts/redis/redis.sh start
func main() {
	// flagSet.Parse(os.Args[1:])

	// rand.Seed(time.Now().UTC().UnixNano())

	// if *showVersion {
	// 	fmt.Println(util.Version("nsqd"))
	// 	return
	// }

	//
	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// var cfg map[string]interface{}
	// if *config != "" {
	// 	_, err := toml.DecodeFile(*config, &cfg)
	// 	if err != nil {
	// 		log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
	// 	}
	// }

	opts := broker.NewBrokerOptions()
	// options.Resolve(opts, flagSet, cfg)
	opts.TCPAddress = "0.0.0.0:8600"
	opts.HTTPAddress = "0.0.0.0:8601"
	b := broker.NewBroker(opts)

	// log.Println(util.Version("nsqd"))
	// log.Debug("worker id %d", opts.ID)

	// nsqd.LoadMetadata()
	// err := nsqd.PersistMetadata()
	// if err != nil {
	// 	log.Fatalf("ERROR: failed to persist metadata - %s", err.Error())
	// }
	b.Main()
	<-exitChan
	b.Exit()

}
