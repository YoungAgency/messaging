package storage

import (
	"context"
	"errors"
)

var (
	// ErrDuplicateEvent is returned by Add method if given message already exists
	ErrDuplicateEvent = errors.New("storage: event already exists")
)

// EventStorage is used to persist processed events
type EventStorage interface {
	Add(ctx context.Context, topic string, eventID string) error
	Exists(ctx context.Context, topic string, eventID string) (bool, error)
	Del(ctx context.Context, topic string, eventID string) error
}
