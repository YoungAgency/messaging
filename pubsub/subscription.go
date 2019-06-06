package pubsub

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// SubscriptionError is a type that wrap an error and the topic
// on which it occured
type SubscriptionError struct {
	Err   error
	Topic string
}
type ctxWithCancel struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type subscription struct {
	ctx    ctxWithCancel
	topic  string
	h      Handler
	opt    *SubscriptionOptions
	active int32
}

func newSubscription(ctx ctxWithCancel, topic string, h Handler, opt *SubscriptionOptions) *subscription {
	ret := &subscription{
		ctx:   ctx,
		opt:   opt,
		topic: topic,
	}
	ret.h = func(ctx context.Context, msg RawMessage) (err error) {
		atomic.AddInt32(&ret.active, 1)
		defer atomic.AddInt32(&ret.active, -1)
		return h(ctx, msg)
	}
	return ret
}

func (s *subscription) start(m Messenger, wg *sync.WaitGroup) <-chan SubscriptionError {
	out := make(chan SubscriptionError, 0)
	go func() {
		defer close(out)
		defer wg.Done()
		out <- SubscriptionError{
			Err:   m.Subscribe(s.subParams()),
			Topic: s.topic,
		}
	}()
	return out
}

// stop cancel subscription context and wait all handlers to return
func (s *subscription) stop() {
	s.ctx.cancel()
	for {
		active := atomic.LoadInt32(&s.active)
		if active == 0 {
			return
		}
		<-time.After(50 * time.Millisecond)
	}
}

func (s *subscription) subParams() (context.Context, string, Handler, *SubscriptionOptions) {
	return s.ctx.ctx, s.topic, s.h, s.opt
}
