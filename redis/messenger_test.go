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
	channel := "test_channel_" + uuid.New().String()[:4]
	message := "test_message"
	go func() {
		time.Sleep(time.Second * 2)
		err := m.Publish(context.Background(), channel, message)
		assert.NoError(t, err)
	}()

	replies, errs := m.Subscribe(context.Background(), channel)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	select {
	case r := <-replies:
		assert.Equal(t, message, r)
	case err := <-errs:
		t.Error(err)
	case <-ctx.Done():
		t.Fatal(context.DeadlineExceeded)
	}
}
