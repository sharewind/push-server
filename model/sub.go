package model

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
)

type Subscribe struct {
	ChannelID  int64 `json:"channel_id" bson:"channel_id"`
	DeviceID   int64 `json:"device_id" bson:"device_id"`
	DeviceType int8  `json:"device_type" bson:"device_type"`
	CreatedAt  int64 `json:"created_at" bson:"created_at"`
	UpdatedAt  int64 `json:"updated_at" bson:"updated_at"`
}

func SaveSubscribe(sub *Subscribe) (err error) {
	insert := func(c *mgo.Collection) error {
		fn := c.Insert(sub)
		return fn
	}
	err = withCollection("subs", insert)
	return err
}

func FindSubscribeByDeviceID(channelID int64, deviceID int64) (result *Subscribe, err error) {
	result = &Subscribe{}
	query := func(c *mgo.Collection) error {
		fn := c.Find(bson.M{"channel_id": channelID, "device_id": deviceID}).One(&result)
		return fn
	}
	err = withCollection("subs", query)
	return result, err
}

func SaveOrUpdateSubscribe(sub *Subscribe) (err error) {
	exist, err := FindSubscribeByDeviceID(sub.ChannelID, sub.DeviceID)
	log.Debug(" exist sub %#v , err %s", exist, err)
	if err == nil && exist != nil {
		update := func(c *mgo.Collection) error {
			q := bson.M{"channel_id": sub.ChannelID, "device_id": sub.DeviceID}
			m := bson.M{"$set": bson.M{"updated_at": time.Now().UnixNano()}}
			fn := c.Update(q, m)
			return fn
		}
		err = withCollection("subs", update)
		return err
	} else {
		return SaveSubscribe(sub)
	}
}

func FindSubscribeByChannelID(channelID int64, deviceType int8, skip int, limit int) (results []Subscribe, err error) {
	results = []Subscribe{}
	query := func(c *mgo.Collection) error {
		q := bson.M{"channel_id": channelID}
		if deviceType != ALLDevice {
			q = bson.M{"channel_id": channelID, "device_type": deviceType}
		}

		fn := c.Find(q).Skip(skip).Limit(limit).All(&results)
		if limit < 0 {
			fn = c.Find(q).Skip(skip).All(&results)
		}
		return fn
	}
	err = withCollection("subs", query)
	return results, err
}
