package model

import (
	// "fmt"
	// "github.com/op/go-logging"
	"testing"
	"time"

	// "github.com/bmizerany/assert"
	// . "code.sohuno.com/kzapp/push-server/model"
	"code.sohuno.com/kzapp/push-server/util"
)

func TestSaveDevice(t *testing.T) {
	device := &Device{
		ID:               int64(1),
		DeviceType:       Android,
		DeviceName:       "测试的Android设备",
		SerialNO:         "SOHUTEST20140401",
		CreatedAt:        time.Now().UnixNano(),
		OnlineStatus:     StatusOffline,
		OnlineTimestamp:  time.Now().UnixNano() - 1000000,
		OfflineTimestamp: time.Now().UnixNano(),
	}
	SaveDevice(device)
}

func TestFindDeviceIDBySerialNO(t *testing.T) {
	serialNO := "SOHUTEST20140401"
	deviceID, err := FindDeviceIDBySerialNO(serialNO)
	if err != nil {
		t.Logf("error %s", err)
	}
	t.Logf("%d", deviceID)
}

func BenchmarkSaveDevice(b *testing.B) {
	factory := &util.GuidFactory{}
	device := &Device{
		ID:               int64(1),
		DeviceType:       Android,
		DeviceName:       "测试的Android设备",
		SerialNO:         "SOHUTEST20140401",
		CreatedAt:        time.Now().UnixNano(),
		OnlineStatus:     StatusOffline,
		OnlineTimestamp:  time.Now().UnixNano() - 1000000,
		OfflineTimestamp: time.Now().UnixNano(),
	}

	for i := 0; i < b.N; i++ {
		id, _ := factory.NewGUID(0)
		device.ID = id
		SaveDevice(device)
	}

}
