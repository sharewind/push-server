package model

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

var (
	redisPool   *redis.Pool
	redisServer string = "localhost:6379" // host:port of server
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
	conn.Send("EXPIRE", key, expiresSecconds)
	if _, err := conn.Do(""); err != nil {
		return err
	}
	return nil
}

func GetOfflineMessages(clientID string) {
	conn := redisPool.Get()
	defer conn.Close()

	// key := fmt.Sprintf("off:%s", clientID)
	// redis.conn.Do("ZREMRANGEBYSCORE", key)
}
