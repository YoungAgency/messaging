package redis

import (
	"context"
	"errors"

	"github.com/gomodule/redigo/redis"
	client "github.com/gomodule/redigo/redis"
)

var (
	// ErrInvalidChannel is returned by Messenger if provided channel param is invalid
	ErrInvalidChannel = errors.New("redis: invalid channel name")
)

// Messenger interface wraps Subscriber and Publisher interfaces
type Messenger interface {
	Subscriber
	Publisher
}

// Subscriber interface permits to subscribe to redis channel
type Subscriber interface {
	Subscribe(ctx context.Context, channel string) (<-chan string, <-chan error)
}

// Publisher interface permits to publish messages to Redis Pub/Sub service
type Publisher interface {
	Publish(ctx context.Context, channel string, m string) error
}

// PoolMessenger implements Messenger interface
// It uses a pool of redis connection for Subscribe and Receive
type PoolMessenger struct {
	Pool *client.Pool
}

// Subscribe performs a subscription on channel until ctx is done
// Every message or error is sent with returned channels
func (m PoolMessenger) Subscribe(ctx context.Context, channel string) (<-chan string, <-chan error) {
	out, out2 := make(chan string), make(chan error)
	go func() {
		psc := redis.PubSubConn{Conn: m.Pool.Get()}
		defer func() {
			psc.Unsubscribe()
			psc.Close()
			close(out)
			close(out2)
		}()
		if valid := m.checkChannel(channel); !valid {
			out2 <- ErrInvalidChannel
			return
		}
		err := psc.Subscribe(channel)
		if err != nil {
			out2 <- err
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
				switch v := psc.Receive().(type) {
				case redis.Message:
					out <- string(v.Data)
				case redis.Subscription:
					// fmt.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
				case error:
					out2 <- v
				}
			}
		}
	}()
	return out, out2
}

// Publish publish given msg on given channel
func (m PoolMessenger) Publish(ctx context.Context, channel string, msg string) error {
	if valid := m.checkChannel(channel); !valid {
		return ErrInvalidChannel
	}
	c, err := m.Pool.GetContext(ctx)
	if err != nil {
		return context.DeadlineExceeded
	}
	defer c.Close()
	_, err = c.Do("PUBLISH", channel, msg)
	return err
}

func (m PoolMessenger) checkChannel(c string) bool {
	return c != ""
}
