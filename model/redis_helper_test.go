package model

import (
	// "fmt"
	// "log"
	"testing"
	// "time"

	// "github.com/bmizerany/assert"
)

func TestIncrMsgOKCount(t *testing.T) {
	messageID := int64(1)
	delta := 1
	result, err := IncrMsgOKCount(messageID, delta)
	if err != nil {
		t.Logf("ERROR: IncrMsgOKCount %s", err)
	}
	t.Logf("INFO: IncrMsgOKCount %d", result)
}

func TestIncrMsgErrCount(t *testing.T) {
	messageID := int64(1)
	delta := 1
	result, err := IncrMsgErrCount(messageID, delta)
	if err != nil {
		t.Logf("ERROR: IncrMsgErrCount %s", err)
	}
	t.Logf("INFO: IncrMsgErrCount %d", result)
}
