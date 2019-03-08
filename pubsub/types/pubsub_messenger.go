package types

import "context"

// Defines our interface for connecting, producing and consuming messages.
type PubSubMessenger interface {
	Publish(obj interface{}, topicName string) error
	Subscribe(topic string, handlerFunc func(context.Context, string, string, int64, []byte) error) error
}
