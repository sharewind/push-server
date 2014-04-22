package model

import (
	"labix.org/v2/mgo"
	// "labix.org/v2/mgo/bson"
)

//TODO need count success/failure
type Channel struct {
	ID           int64  `json:"id" bson:"_id"`
	Name         string `json:"name" bson:"name"`
	CreatedAt    int64  `json:"created_at" bson:"created_at"`
	Creator      string `json:"creator" bson:"creator"`
	APPID        int64  `json:"app_id" bson:"app_id"`
	APPName      string `json:"app_name" bson:"app_name"`
	MessageCount int64  `json:"message_count" bson:"message_count"`
}

func FindChannelByID(ID int64) (result *Channel, err error) {
	result = &Channel{}
	query := func(c *mgo.Collection) error {
		fn := c.FindId(ID).One(result)
		return fn
	}
	err = withCollection("channels", query)
	if err != nil {
		log.Error("FindChannelByID %s", err)
	}
	return result, err
}

func SaveChannel(channel *Channel) (err error) {
	insert := func(c *mgo.Collection) error {
		fn := c.Insert(channel)
		return fn
	}
	err = withCollection("channels", insert)
	return err
}

func IncreaseChannelMessageCount(ID int64, delta int) {

}

func ListChannel(skip int, limit int) (result *[]Channel, err error) {
	result = &[]Channel{}
	query := func(c *mgo.Collection) error {
		fn := c.Find(nil).Skip(skip).Limit(limit).All(result)
		return fn
	}
	err = withCollection("channels", query)
	return result, err
}

func CountChannel() (result int, err error) {
	query := func(c *mgo.Collection) error {
		var fn error
		result, fn = c.Find(nil).Count()
		return fn
	}
	err = withCollection("channels", query)
	return result, err
}

func CheckOrCreateChannel(channel_id int64) (result bool) {
	_, err := FindChannelByID(channel_id)
	if err != nil {
		log.Debug("channel check not exist")
		channel := &Channel{channel_id, "", 0, "", 0, "", 0}
		err := SaveChannel(channel)
		if err != nil {
			log.Error("channel insert error: %s", err.Error())
			return false
		}
	}
	return true
}

// func IsValidChannel(channelId int64) (valid bool) {
// 	query := func(c *mgo.Collection) error {
// 		result, fn := c.FindId(channelId).Count()
// 		if result <= 0 {
// 			valid = false
// 		} else {
// 			valid = true
// 		}
// 		return fn
// 	}
// 	err = withCollection("channels", query)
// 	return valid, err
// }
