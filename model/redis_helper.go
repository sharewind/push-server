package model

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/op/go-logging"
	"time"
)

var (
	log         = logging.MustGetLogger("model")
	redisPool   *redis.Pool
	redisServer string = "10.10.79.123:15151" // host:port of server
	password    string
)

func init() {
	redisPool = &redis.Pool{
		MaxIdle:     3,
		MaxActive:   30,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisServer)
			if err != nil {
				return nil, err
			}
			// if _, err := c.Do("AUTH", password); err != nil {
			// 	c.Close()
			// 	return nil, err
			// }
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func SetClientConn(clientID int64, brokerAddr string) error {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("client:%d", clientID)
	expiresSecconds := 3600 * 24
	conn.Send("SET", key, brokerAddr)
	conn.Send("EXPIRE", key, expiresSecconds)
	if _, err := conn.Do(""); err != nil {
		return err
	}
	return nil
}

func GetClientConn(clientID int64) (result string, err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("client:%d", clientID)
	if result, err = redis.String(conn.Do("GET", key)); err != nil {
		return "", err
	}
	return result, err
}

func DelClientConn(clientID int64) (err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("client:%d", clientID)
	if _, err = conn.Do("DEL", key); err != nil {
		return err
	}
	return nil
}

func SaveOfflineMessage(clientID int64, messageID int64) error {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("off:%d", clientID)
	expiresSecconds := 3600 * 24 * 7
	conn.Send("ZADD", key, messageID, messageID)
	conn.Send("ZREMRANGEBYRANK", key, 1000, 10000) // limit 1000
	conn.Send("EXPIRE", key, expiresSecconds)
	if _, err := conn.Do(""); err != nil {
		return err
	}
	return nil
}

func RemoveOfflineMessage(clientID int64, messageID int64) (err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("off:%d", clientID)
	conn.Send("ZREM", key, messageID)
	if _, err := conn.Do(""); err != nil {
		return err
	}
	return nil
}

func GetOfflineMessages(clientID int64) (messageIDs []int64, err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("off:%d", clientID)
	values, err := redis.Values(conn.Do("ZRANGE", key, 0, -1))
	if err != nil {
		return nil, err
	}
	// log.Info("GetOfflineMessages key %s reply %#v", key, values)

	for len(values) > 0 {
		var id int64
		// rating := -1 // initialize to illegal value to detect nil.
		values, err = redis.Scan(values, &id)
		if err != nil {
			return nil, err
		}
		// log.Debug("%#v", id)
		messageIDs = append(messageIDs, id)
	}
	return messageIDs, nil
}

func IncrMsgOKCount(messageID int64, delta int) (reply int, err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("m:ok:%d", messageID)
	reply, err = redis.Int(conn.Do("INCRBY", key, delta))
	if err != nil {
		return -1, err
	}
	log.Info("IncrMsgOKCount key %s reply %d", key, reply)
	return reply, err
}

func IncrMsgErrCount(messageID int64, delta int) (reply int, err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("m:err:%d", messageID)
	reply, err = redis.Int(conn.Do("INCRBY", key, delta))
	if err != nil {
		return -1, err
	}
	log.Info("IncrMsgErrCount key %s reply %d", key, reply)
	return reply, err
}

func GetMsgOKErrCount(messageID int64) (okCount int, errCount int) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("m:ok:%d", messageID)
	okCount, err := redis.Int(conn.Do("get", key))
	if err != nil {
		okCount = 0
		log.Error(err.Error())
	}
	log.Info("getMsgOKCount key %s reply %d", key, okCount)

	key = fmt.Sprintf("m:err:%d", messageID)
	errCount, err = redis.Int(conn.Do("get", key))
	if err != nil {
		errCount = 0
		log.Error(err.Error())
	}
	log.Info("getMsgErrCount key %s reply %d", key, errCount)
	return okCount, errCount
}

func IncrClientOKCount(clientID int64, delta int) (reply int, err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("c:ok:%d", clientID)
	reply, err = redis.Int(conn.Do("INCRBY", key, delta))
	if err != nil {
		return -1, err
	}
	log.Info("IncrClientOKCount key %s reply %d", key, reply)
	return reply, err
}

func IncrClientErrCount(clientID int64, delta int) (reply int, err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("c:err:%d", clientID)
	reply, err = redis.Int(conn.Do("INCRBY", key, delta))
	if err != nil {
		return -1, err
	}
	log.Info("IncrClientErrCount key %s reply %d", key, reply)
	return reply, err
}
