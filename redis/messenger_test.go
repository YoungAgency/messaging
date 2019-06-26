package redis

import (
	"context"
	"sync"
	"testing"
	"time"

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

	var sent int64
	var received int64

	go func() {
		replies, errs := m.Subscribe(context.Background(), "test")
		for {
			select {
			case _, ok := <-replies:
				if ok {
					received++
				}
			case err, ok := <-errs:
				if ok {
					if err != nil {
						t.Error(err)
					}
				}
			}
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer time.Sleep(2 * time.Second)
		time.Sleep(10 * time.Millisecond)
		for i := 0; i < 5000; i++ {
			go func() {
				err := m.Publish(context.Background(), "test", "message")
				assert.NoError(t, err)
			}()
			sent++
		}
	}()
	wg.Wait()
	assert.Equal(t, sent, received)
}
