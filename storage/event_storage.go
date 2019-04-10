package storage

import "context"

// EventStorage is used to persist processed events
type EventStorage interface {
	Add(ctx context.Context, topic string, eventID string) error
	Exists(ctx context.Context, topic string, eventID string) (bool, error)
	Del(ctx context.Context, topic string, eventID string) error
}
