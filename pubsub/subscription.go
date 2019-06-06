package pubsub

import (
	"context"
	"sync/atomic"
	"time"
)

type subscription struct {
	h      Handler
	ctx    ctxWithCancel
	opt    *SubscriptionOptions
	active int32
}

func newSubscription(ctx ctxWithCancel, h Handler, opt *SubscriptionOptions) *subscription {
	ret := &subscription{
		ctx: ctx,
		opt: opt,
	}
	ret.h = func(ctx context.Context, msg RawMessage) (err error) {
		atomic.AddInt32(&ret.active, 1)
		defer atomic.AddInt32(&ret.active, -1)
		return h(ctx, msg)
	}
	return ret
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
