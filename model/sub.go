package model

type Subscribe struct {
	ChannelID int64 `json:"channel_id" bson:"channel_id"`
	DeviceID  int64 `json:"device_id" bson:"device_id"`
	CreatedAt int64 `json:"created_at" bson:"created_at"`
}
