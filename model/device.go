package model

import (
	"errors"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
)

type Device struct {
	ID               int64  `json:"id" bson:"_id,omitempty"`
	DeviceType       int8   `json:"device_type" bson:"device_type"`
	DeviceName       string `json:"device_name" bson:"device_name"`
	SerialNO         string `json:"serial_no" bson:"serial_no"`
	CreatedAt        int64  `json:"created_at" bson:"created_at"`
	OnlineStatus     int8   `json:"online_status" bson:"online_status"`
	OnlineTimestamp  int64  `json:"online_timestamp" bson:"online_timestamp"`
	OfflineTimestamp int64  `json:"offline_timestamp" bson:"offline_timestamp"`
}

func FindDeviceIDBySerialNO(serialNO string) (ID int64, err error) {
	result := &Device{}
	query := func(c *mgo.Collection) error {
		fn := c.Find(bson.M{"serial_no": serialNO}).One(result)
		return fn
	}
	err = withCollection("devices", query)
	if err != nil {
		log.Debug("FindDeviceIDBySerialNO %s", err)
		return int64(-1), err
	}

	if result == nil {
		return int64(-1), errors.New("device not exist")
	}
	return result.ID, nil
}

func FindDeviceByID(ID int64) (result *Device, err error) {
	result = &Device{}
	query := func(c *mgo.Collection) error {
		fn := c.FindId(ID).One(result)
		return fn
	}
	err = withCollection("devices", query)
	if err != nil {
		log.Error("FindDeviceByID %d %s", ID, err)
	}
	return result, err
}

func SaveDevice(device *Device) (err error) {
	insert := func(c *mgo.Collection) error {
		fn := c.Insert(device)
		return fn
	}
	err = withCollection("devices", insert)
	return err
}

func TouchDeviceOnline(deviceID int64) (err error) {
	update := func(c *mgo.Collection) error {
		q := bson.M{"_id": deviceID}
		m := bson.M{"$set": bson.M{"online_timestamp": time.Now().UnixNano()}}
		fn := c.Update(q, m)
		return fn
	}
	err = withCollection("devices", update)
	return err
}

func TouchDeviceOffline(deviceID int64) (err error) {
	update := func(c *mgo.Collection) error {
		q := bson.M{"_id": deviceID}
		m := bson.M{"$set": bson.M{"offline_timestamp": time.Now().UnixNano()}}
		fn := c.Update(q, m)
		return fn
	}
	err = withCollection("devices", update)
	return err
}

func ListDevice(skip int, limit int) (result *[]Device, err error) {
	result = &[]Device{}
	query := func(c *mgo.Collection) error {
		fn := c.Find(nil).Skip(skip).Limit(limit).All(result)
		return fn
	}
	err = withCollection("devices", query)
	return result, err
}
