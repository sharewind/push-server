package model

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

var (
	redisPool   *redis.Pool
	redisServer string = "10.10.79.123:15151" // host:port of server
	password    string
)

func init() {
	redisPool = &redis.Pool{
		MaxIdle:     3,
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

func SetClientConn(clientID string, brokerAddr string) error {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("client:%s", clientID)
	expiresSecconds := 3600 * 24
	conn.Send("SET", key, brokerAddr)
	conn.Send("EXPIRE", key, expiresSecconds)
	if _, err := conn.Do(""); err != nil {
		return err
	}
	return nil
}

func GetClientConn(clientID string) (result string, err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("client:%s", clientID)
	if result, err = redis.String(conn.Do("GET", key)); err != nil {
		return "", err
	}
	return result, err
}

func DelClientConn(clientID string) (err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("client:%s", clientID)
	if _, err = conn.Do("DEL", key); err != nil {
		return err
	}
	return nil
}

func SaveOfflineMessage(clientID string, messageID int64) error {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("off:%s", clientID)
	expiresSecconds := 3600 * 24 * 7
	conn.Send("ZADD", key, messageID, messageID)
	conn.Send("ZREMRANGEBYRANK", key, 1000, 10000) // limit 1000
	conn.Send("EXPIRE", key, expiresSecconds)
	if _, err := conn.Do(""); err != nil {
		return err
	}
	return nil
}

func RemoveOfflineMessage(clientID string, messageID int64) (err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("off:%s", clientID)
	conn.Send("ZREM", key, messageID)
	if _, err := conn.Do(""); err != nil {
		return err
	}
	return nil
}

func GetOfflineMessages(clientID string) (messageIDs []int64, err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("off:%s", clientID)
	values, err := redis.Values(conn.Do("ZRANGE", key, 0, -1))
	if err != nil {
		return nil, err
	}
	log.Printf("INFO: GetOfflineMessages key %s reply %#v", key, values)

	for len(values) > 0 {
		var id int64
		// rating := -1 // initialize to illegal value to detect nil.
		values, err = redis.Scan(values, &id)
		if err != nil {
			panic(err)
		}
		log.Printf("%#v", id)
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
	log.Printf("INFO: IncrMsgOKCount key %s reply %d", key, reply)
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
	log.Printf("INFO: IncrMsgErrCount key %s reply %d", key, reply)
	return reply, err
}

func IncrClientOKCount(clientID int64, delta int) (reply int, err error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("c:ok:%d", clientID)
	reply, err = redis.Int(conn.Do("INCRBY", key, delta))
	if err != nil {
		return -1, err
	}
	log.Printf("INFO: IncrClientOKCount key %s reply %d", key, reply)
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
	log.Printf("INFO: IncrClientErrCount key %s reply %d", key, reply)
	return reply, err
}
