package model

import (
	// "fmt"
	// "log"
	"testing"
	"time"

	// "github.com/bmizerany/assert"
	"code.sohuno.com/kzapp/push-server/model"
)

func TestSaveDevice(t *testing.T) {
	device := &model.Device{
		ID:               int64(1),
		DeviceType:       model.Android,
		DeviceName:       "测试的Android设备",
		SerialNO:         "SOHUTEST20140401",
		CreatedAt:        time.Now().UnixNano(),
		OnlineStatus:     model.StatusOffline,
		OnlineTimestamp:  time.Now().UnixNano() - 1000000,
		OfflineTimestamp: time.Now().UnixNano(),
	}
	model.SaveDevice(device)
}

func TestFindDeviceIDBySerialNO(t *testing.T) {
	serialNO := "SOHUTEST20140401"
	deviceID, err := model.FindDeviceIDBySerialNO(serialNO)
	if err != nil {
		T.Logf("error %s", err)
	}
	T.Logf("%s", deviceID)
}
