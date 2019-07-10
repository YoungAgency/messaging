package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestManual(t *testing.T) {
	m := NewMessenger(context.Background(), &Options{
		SubscriptionName: "test-service",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewService(ctx, m, nil)
	ch := Multiplex(
		s.AddSubscription("test-1", func(ctx context.Context, msg RawMessage) error {
			fmt.Println("received on topic test-1")
			return nil
		}, &SubscriptionOptions{}),
		s.AddSubscription("test-2", func(ctx context.Context, msg RawMessage) error {
			fmt.Println("received on topic test-2")
			return nil
		}, &SubscriptionOptions{}),
	)
	go func() {
		for {
			<-time.After(time.Second * 5)
			fmt.Println("\n\nStopping")
			s.Stop()
			fmt.Println("Finish stop")
		}
	}()

	mul := make(chan SubscriptionError, 0)
	go func() {
		for v := range ch {
			mul <- v
		}
	}()
	for err := range mul {
		fmt.Println(err.Topic, err.Err)
		if err.Err != ErrStopped {
			topic := err.Topic
			newCh := s.AddSubscription(topic, func(ctx context.Context, msg RawMessage) error {
				fmt.Println("received on topic", topic)
				return nil
			}, &SubscriptionOptions{})
			go func(ch <-chan SubscriptionError) {
				for v := range newCh {
					mul <- v
				}
			}(newCh)
		}
	}
	time.Sleep(time.Minute * 30)
}
