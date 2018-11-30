package types

import "context"

// Defines our interface for connecting, producing and consuming messages.
// Publish send the given obj message to given PubSub topicName
// Subscribe perform a subscription to given topic. handlerFunc is invoked when a message is received
type PubSubMessenger interface {
	Publish(obj interface{}, topicName string) error
	Subscribe(topic string, handlerFunc func(ctx context.Context, eventID string, timestamp int64, message []byte) error) error
}
