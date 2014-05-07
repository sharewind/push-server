package model

import (
	"errors"
	"fmt"
	"labix.org/v2/mgo"
	"sync"
	"time"

	"code.sohuno.com/kzapp/push-server/util"
)

var (
	mgoSession *mgo.Session
	// databaseServer = "mongodb://192.168.230.52:27017,192.168.230.53:27017,192.168.230.54:27017?connect=replicaSet"
	databaseServer = "mongodb://10.10.69.191:27017,10.10.69.191:27018?connect=replicaSet"
	// databaseServer = "mongodb://10.2.58.178:27017"
	// databaseServer = "mongodb://localhost:27017"
	databaseName = "push"
	mux          sync.Mutex
	pool         *util.Semaphore
)

func init() {
	mux.Lock()
	defer mux.Unlock()

	pool = util.NewSemaphore(10)

	initIndex()
}

func getSession() *mgo.Session {
	if mgoSession == nil {
		var err error
		mgoSession, err = mgo.DialWithTimeout(databaseServer, 60*time.Second)
		mgoSession.SetMode(mgo.Monotonic, false)
		mgoSession.SetMode(mgo.Strong, false)
		if err != nil {
			panic(err) // no, not really
		}
	}
	return mgoSession.Clone()
}

func initIndex() {

	serial_no_index := mgo.Index{
		Key:        []string{"serial_no"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     true,
	}
	err := createIndex("devices", &serial_no_index)
	if err != nil {
		log.Error("ERROR: create index serial_no_index error %s", err)
	}

	sub_index := mgo.Index{
		Key:        []string{"channel_id", "device_id"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     true,
	}
	err = createIndex("subs", &sub_index)
	if err != nil {
		log.Error("ERROR: create index serial_no_index error %s", err)
	}

}

func createIndex(collection string, index *mgo.Index) error {
	update := func(c *mgo.Collection) error {
		fn := c.EnsureIndex(*index)
		return fn
	}

	err := withCollection(collection, update)
	if err != nil {
		return err
	}
	return nil
}

func withCollection(collection string, s func(*mgo.Collection) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("PANIC: Recovered in mgo_helper.withCollection %s", r)
			err = errors.New("panic on mongodb withCollection: " + fmt.Sprintf("%s", r))
		}
	}()

	pool.Acquire()
	defer pool.Release()

	session := getSession()
	defer session.Close()
	c := session.DB(databaseName).C(collection)
	err = s(c)
	return err
}
