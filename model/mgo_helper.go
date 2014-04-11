package model

import (
	"labix.org/v2/mgo"
)

var (
	mgoSession     *mgo.Session
	databaseServer = "10.10.69.191:27017"
	databaseName   = "push"
)

func getSession() *mgo.Session {
	if mgoSession == nil {
		var err error
		mgoSession, err = mgo.Dial(databaseServer)
		if err != nil {
			panic(err) // no, not really
		}
	}
	return mgoSession.Clone()
}

func withCollection(collection string, s func(*mgo.Collection) error) error {
	session := getSession()
	defer session.Close()
	c := session.DB(databaseName).C(collection)
	return s(c)
}
