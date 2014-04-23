package main

import (
	. "code.sohuno.com/kzapp/push-server/model"
	"code.sohuno.com/kzapp/push-server/util"
	"fmt"
	"runtime"
	"sync"
	"time"
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

func main() {
	runtime.GOMAXPROCS(100)
	count := int64(1000000)

	countChan := make(chan int64, count)

	start := time.Now().UnixNano()
	for i := int64(0); i < int64(100); i++ {
		go work(i, countChan)
	}

	for i := int64(0); i < count; i++ {
		<-countChan
	}

	end := time.Now().UnixNano()
	cost := end - start
	per := cost / count
	fmt.Printf("total %d ns, per %f ns", cost, per)

}

func work(step int64, countChan chan int64) {
	id := step
	for {
		id = id + step
		device.ID = id
		SaveDevice(device)
		countChan <- id
	}
}
