package model

import (
	"code.sohuno.com/kzapp/push-server/util"
	"labix.org/v2/mgo"
	"sync"
	"time"
)

var (
	mgoSession *mgo.Session
	//databaseServer = "mongodb://192.168.230.52:27017,192.168.230.53:27017,192.168.230.54:27017?connect=replicaSet"
	databaseServer = "mongodb://10.10.69.191:27017,10.10.69.191:27018?connect=replicaSet"
	// databaseServer = "mongodb://10.2.58.178:27017"
	databaseName = "push"
	mux          sync.Mutex
	pool         *util.Semaphore
)

func init() {
	mux.Lock()
	defer mux.Unlock()

	pool = util.NewSemaphore(10)
	getSession()
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

func withCollection(collection string, s func(*mgo.Collection) error) error {
	pool.Acquire()
	defer pool.Release()

	session := getSession()
	defer session.Close()
	c := session.DB(databaseName).C(collection)
	return s(c)
}
