package storage

import (
	"context"
	"errors"
	"time"

	"github.com/YoungAgency/utils/date"
	"github.com/gomodule/redigo/redis"
)

// RedisEventStorage is used to persist processed events in a redis hashset
type RedisEventStorage struct {
	pool *redis.Pool
}

// NewRedisEventStorage creates a connection pool to redis
func NewRedisEventStorage(host string, db int) RedisEventStorage {
	if host == "" {
		host = ":6379"
	}
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host, redis.DialDatabase(db))
			if err != nil {
				return nil, err
			}
			return c, err
		},
		// Test if the connection is valid before returning it
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	return RedisEventStorage{pool}
}

// Add an order to the hash. If already exists panic
func (es RedisEventStorage) Add(ctx context.Context, topic string, eventID string) error {
	conn := es.pool.Get()
	defer conn.Close()
	exists, err := es.Exists(ctx, topic, eventID)
	if exists {
		return errors.New("Event already exists")
	}
	if err != nil {
		return err
	}
	now := date.NowTimestamp()
	_, err = conn.Do("HSET", topic, eventID, now)
	if err != nil {
		return err
	}
	return nil
}

// Exists checks if event is already processed in the hash
func (es RedisEventStorage) Exists(ctx context.Context, topic string, eventID string) (bool, error) {
	conn := es.pool.Get()
	defer conn.Close()
	exists, err := conn.Do("HEXISTS", topic, eventID)
	if err != nil {
		return false, err
	}
	if exists.(int64) == 0 {
		return false, nil
	}
	return true, nil
}

// Del remove an event from the hash
func (es RedisEventStorage) Del(ctx context.Context, topic string, eventID string) error {
	conn := es.pool.Get()
	defer conn.Close()
	exists, err := es.Exists(ctx, topic, eventID)
	if !exists {
		// No prob. Doesn't exist
		return nil
	}
	if err != nil {
		return err
	}
	_, err = conn.Do("HDEL", topic, eventID)
	if err != nil {
		return err
	}
	return nil
}
