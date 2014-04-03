package model

import (
	// "fmt"
	// "log"
	"testing"
	"time"

	// "github.com/bmizerany/assert"
	"code.sohuno.com/kzapp/push-server/model"
)

func TestSaveChannel(t *testing.T) {
	channel := &model.Channel{
		ID:           int64(1001),
		Name:         "test_channel",
		CreatedAt:    time.Now().UnixNano(),
		Creator:      "tester",
		APPID:        int64(1001),
		APPName:      "test_app",
		MessageCount: int64(0),
	}
	model.SaveChannel(channel)
}

func TestFindChannelByID(t *testing.T) {
	ID := int64(1001)
	channel, err := model.FindChannelByID(ID)
	if err != nil {
		t.Logf("error %s", err)
	}
	t.Logf("%s", channel)
}
