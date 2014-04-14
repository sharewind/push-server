package model

const (
	StatusOnline = iota
	StatusOffline
)

const (
	ALLDevice    = 0
	Browser      = 1
	PC           = 2
	Android      = 3
	IOS          = 4
	WindowsPhone = 5
	Other        = 6
)

// Push Type
const (
	P2P = iota
	Push2Group
	Push2Channel
	Push2All
)

// Message Created By
const (
	CreatdByOperator = iota
	CreatedByUser
)

type OfflineMessage struct {
	DeviceID  int64 `json:"device_id"`
	MessageID int64 `json:"message_id"`
	CreatedAt int64 `json:"created_at"`
	Expires   int64 `json:"expires"`
}
