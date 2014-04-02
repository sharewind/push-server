package model

import (
	"labix.org/v2/mgo"
	// "labix.org/v2/mgo/bson"
)

type Message struct {
	ID        int64  `json:"id" bson:"_id"`
	Body      string `json"body" bson:"body"`
	CreatedAt int64  `json:"created_at" bson:"created_at"`
	CreatedBy int8   `json:"created_by" bson:"created_by"`
	CreatorID int64  `json:"creator_id" bson:"creator_id"`
	Expires   int64  `json:"expires" bson:"expires"`
	PushType  int8   `json:"push_type" bson:"push_type"`
	ChannelID int64  `json:"channel_id" bson:"channel_id"`
	DeviceID  int64  `json:"device_id" bson:"device_id"`
}

func FindMessageByID(ID int64) (result *Message, err error) {
	result = &Message{}
	query := func(c *mgo.Collection) error {
		fn := c.FindId(ID).One(result)
		return fn
	}
	err = withCollection("messages", query)
	return result, err
}

func SaveMessage(msg *Message) (err error) {
	insert := func(c *mgo.Collection) error {
		fn := c.Insert(msg)
		return fn
	}
	err = withCollection("messages", insert)
	return err
}
