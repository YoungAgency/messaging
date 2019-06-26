package redis

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gomodule/redigo/redis"
)

func TestPoolMessenger(t *testing.T) {
	m := PoolMessenger{
		Pool: &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", ":6379")
			},
		},
	}
	done := make(chan int, 0)
	channel := "test_channel"
	message := "test_message"

	received := ""
	go func() {
		defer func() {
			done <- 1
		}()
		replies, errs := m.Subscribe(context.Background(), channel)
		for {
			select {
			case r, ok := <-replies:
				if ok {
					received = r
					return
				}
			case err, ok := <-errs:
				if ok && err != nil {
					t.Error(err)
				}
			}
		}
	}()

	err := m.Publish(context.Background(), channel, message)
	assert.NoError(t, err)
	<-done
	assert.Equal(t, message, received)
}
