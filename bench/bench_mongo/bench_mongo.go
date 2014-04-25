package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	. "code.sohuno.com/kzapp/push-server/model"
	"code.sohuno.com/kzapp/push-server/util"
)

var wg sync.WaitGroup

var factory = &util.GuidFactory{}
var device = &Device{
	ID:               int64(1),
	DeviceType:       Android,
	DeviceName:       "测试的Android设备",
	SerialNO:         "SOHUTEST20140401",
	CreatedAt:        time.Now().UnixNano(),
	OnlineStatus:     StatusOffline,
	OnlineTimestamp:  time.Now().UnixNano() - 1000000,
	OfflineTimestamp: time.Now().UnixNano(),
}

var (
	flagSet = flag.NewFlagSet("bench", flag.ExitOnError)

	// pprof options
	cpuprofile = flag.String("cpuprofile", "bench_mongo.prof", "write cpu profile to file")
	memprofile = flag.String("memprofile", "bench_mongo.mprof", "write memory profile to this file")
)

func main() {

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	concurrent := runtime.NumCPU() * 4
	count := int64(100000)
	countChan := make(chan int64, count)

	runtime.GOMAXPROCS(concurrent)

	start := time.Now().UnixNano()
	for i := 0; i < concurrent; i++ {
		go work(countChan)
	}

	for i := int64(0); i < count; i++ {
		<-countChan
		fmt.Printf("finished %d \n", i)
	}

	end := time.Now().UnixNano()
	cost := end - start
	per := cost / count
	fmt.Printf("total %d ns, per %f ns", cost, per)

}

func work(countChan chan int64) {
	for {
		device.ID, _ = factory.NewGUID(0)
		SaveDevice(device)
		countChan <- 1
	}
}
