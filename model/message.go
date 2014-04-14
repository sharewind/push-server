package model

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type Message struct {
	ID         int64  `json:"id" bson:"_id"`
	Body       string `json"body" bson:"body"`
	CreatedAt  int64  `json:"created_at" bson:"created_at"`
	CreatedBy  int8   `json:"created_by" bson:"created_by"`
	CreatorID  int64  `json:"creator_id" bson:"creator_id"`
	Expires    int64  `json:"expires" bson:"expires"`
	DeviceType int8   `json:"device_type" bson:"device_type"`
	PushType   int8   `json:"push_type" bson:"push_type"`
	ChannelID  int64  `json:"channel_id" bson:"channel_id"`
	DeviceID   int64  `json:"device_id" bson:"device_id"`
	OK         int    `json:"ok"`
	Err        int    `json:"err"`
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

func ListMessage() (result *[]Message, err error) {
	result = &[]Message{}
	query := func(c *mgo.Collection) error {
		fn := c.Find(nil).Skip(0).Limit(10).All(result)
		return fn
	}
	err = withCollection("messages", query)
	return result, err
}

func GetMessageByChannelId(channelId int64) (result *[]Message, err error) {
	log.Debug("GetMessageByChannelId")
	result = &[]Message{}
	query := func(c *mgo.Collection) error {
		fn := c.Find(bson.M{"channel_id": channelId}).Skip(0).Limit(10).All(result)
		return fn
	}
	err = withCollection("messages", query)
	return result, err
}

func GetMessageByDeviceId(deviceId int64) (result *[]Message, err error) {
	result = &[]Message{}
	query := func(c *mgo.Collection) error {
		fn := c.Find(bson.M{"device_id": deviceId}).Skip(0).Limit(10).All(result)
		return fn
	}
	err = withCollection("messages", query)
	return result, err
}

func CountMessageByChannelId(channelId int64) (result int, err error) {
	query := func(c *mgo.Collection) error {
		var fn error
		result, fn = c.Find(bson.M{"channel_id": channelId}).Count()
		return fn
	}
	err = withCollection("messages", query)
	return result, err
}

func CountMessageByDeviceId(deviceId int64) (result int, err error) {
	query := func(c *mgo.Collection) error {
		var fn error
		result, fn = c.Find(bson.M{"device_id": deviceId}).Count()
		return fn
	}
	err = withCollection("messages", query)
	return result, err
}
