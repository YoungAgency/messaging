package main

import (
	"context"
	"fmt"
	"time"

	"github.com/YoungAgency/messaging/pubsub"
)

func main() {
	m := pubsub.NewMessenger(context.Background(), &pubsub.Options{
		SubscriptionName: "test-service",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := pubsub.NewService(ctx, m)
	ch := pubsub.Multiplex(
		s.AddHandler("test-1", func(ctx context.Context, msg pubsub.RawMessage) error {
			fmt.Println("received on topic test-1")
			return nil
		}, &pubsub.SubscriptionOptions{}),
		s.AddHandler("test-2", func(ctx context.Context, msg pubsub.RawMessage) error {
			fmt.Println("received on topic test-2")
			return nil
		}, &pubsub.SubscriptionOptions{}),
	)
	go func() {
		for {
			<-time.After(time.Second * 5)
			fmt.Println("Stopping")
			s.Stop()
			fmt.Println("Finish stop")
		}
	}()

	mul := make(chan pubsub.ErrHandler, 0)
	go func() {
		for v := range ch {
			mul <- v
		}
	}()
	for err := range mul {
		fmt.Println(err.Topic, err.Err)
		if err.Err == pubsub.ErrStopped {
			topic := err.Topic
			newCh := s.AddHandler(topic, func(ctx context.Context, msg pubsub.RawMessage) error {
				fmt.Println("received on topic", topic)
				return nil
			}, &pubsub.SubscriptionOptions{})
			go func(ch <-chan pubsub.ErrHandler) {
				for v := range newCh {
					mul <- v
				}
			}(newCh)
		}
		fmt.Println("END")
	}
	time.Sleep(time.Minute * 30)
}
