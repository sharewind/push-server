package model

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
)

type Subscribe struct {
	ChannelID  string `json:"channel_id" bson:"channel_id"`
	DeviceID   int64  `json:"device_id" bson:"device_id"`
	DeviceType int8   `json:"device_type" bson:"device_type"`
	CreatedAt  int64  `json:"created_at" bson:"created_at"`
	UpdatedAt  int64  `json:"updated_at" bson:"updated_at"`
}

func SaveSubscribe(sub *Subscribe) (err error) {
	insert := func(c *mgo.Collection) error {
		fn := c.Insert(sub)
		return fn
	}
	err = withCollection("subs", insert)
	return err
}

func FindSubscribeByDeviceID(channelID string, deviceID int64) (result *Subscribe, err error) {
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
	// log.Debug(" exist sub %#v , err %s", exist, err)
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

// func FindSubscribeBySubscribeID(channelID string, deviceType int8, skip int, limit int) (results []Subscribe, err error) {
// 	results = []Subscribe{}
// 	query := func(c *mgo.Collection) error {
// 		q := bson.M{"channel_id": channelID}
// 		if deviceType != ALLDevice {
// 			q = bson.M{"channel_id": channelID, "device_type": deviceType}
// 		}

// 		fn := c.Find(q).Skip(skip).Limit(limit).All(&results)
// 		if limit < 0 {
// 			fn = c.Find(q).Skip(skip).All(&results)
// 		}
// 		return fn
// 	}
// 	err = withCollection("subs", query)
// 	return results, err
// }

func ListSubscribe(skip int, limit int) (result *[]Subscribe, err error) {
	result = &[]Subscribe{}
	query := func(c *mgo.Collection) error {
		fn := c.Find(nil).Skip(skip).Limit(limit).All(result)
		return fn
	}
	err = withCollection("subs", query)
	return result, err
}

func CountSubscribe(channelId string, deviceType int8) (result int, err error) {
	query := func(c *mgo.Collection) error {
		var fn error
		result, fn = c.Find(bson.M{}).Count()
		return fn
	}
	err = withCollection("subs", query)
	return result, err
}

func CountSubscribeByChannelId(channelId string, deviceType int8) (result int, err error) {
	if deviceType == ALLDevice {
		query := func(c *mgo.Collection) error {
			var fn error
			result, fn = c.Find(bson.M{"channel_id": channelId}).Count()
			return fn
		}
		err = withCollection("subs", query)
		return result, err
	}
	query := func(c *mgo.Collection) error {
		var fn error
		result, fn = c.Find(bson.M{"channel_id": channelId, "device_type": deviceType}).Count()
		return fn
	}
	err = withCollection("subs", query)
	return result, err
}

func GetSubscribeByChannelId(channelId string, skip int, limit int) (result *[]Subscribe, err error) {
	log.Debug("GetMessageByChannelId")
	result = &[]Subscribe{}
	query := func(c *mgo.Collection) error {
		fn := c.Find(bson.M{"channel_id": channelId}).Skip(skip).Limit(limit).All(result)
		return fn
	}
	err = withCollection("subs", query)
	return result, err
}

func FindSubscribeByChannelID(channelID string, deviceType int8, skip int, limit int) (results []Subscribe, err error) {
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
