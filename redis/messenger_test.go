package redis

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

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
	done, subscribed := make(chan int, 0), make(chan int, 0)
	channel := "test_channel_" + uuid.New().String()
	message := "test_message"

	received := ""
	go func() {
		defer func() {
			done <- 1
		}()
		replies, errs := m.Subscribe(context.Background(), channel)
		subscribed <- 1
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
	<-subscribed
	err := m.Publish(context.Background(), channel, message)
	assert.NoError(t, err)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	select {
	case <-done:
		assert.Equal(t, message, received)
	case <-ctx.Done():
		t.Fatal(context.DeadlineExceeded)
	}
}
