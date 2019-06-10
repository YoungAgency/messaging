package pubsub

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrStopped is returned when subscription was cancelled by stopping it
	ErrStopped = errors.New("Stop called for this topic")
	// ErrAlreadyExists is returned by AddSubscription when
	// a subcription on given topic already exists
	ErrAlreadyExists = errors.New("Subscription already exists")
	// ErrNotExists is returned by RemoveHandler when a subscription on given topic
	// does not exists
	ErrNotExists = errors.New("Subscription does not exist")
)

// Service is a wrapper around a Messenger that permits to stop
// and wait to return all subscription and handlers
type Service struct {
	m           Messenger
	mainContext context.Context
	// protect from concurrent access next fields
	mutex     sync.Mutex
	stopGroup sync.WaitGroup
	// map topic-subscriptions
	subscriptions map[string]*subscription
}

// NewService returns a new initialized service
// if ctx is canceled all subscriber will return, but it will be impossible to wait
// until all handlers have returned
func NewService(ctx context.Context, m Messenger) *Service {
	return &Service{
		m:             m,
		mainContext:   ctx,
		subscriptions: make(map[string]*subscription),
	}
}

// AddSubscription set given handler for topic, it also starts  it
func (s *Service) AddSubscription(topic string, h Handler, opt *SubscriptionOptions) <-chan SubscriptionError {
	out := make(chan SubscriptionError)
	go func() {
		defer close(out)
		s.mutex.Lock()
		sub, err := s.addMapLocked(topic, h, opt)
		if err != nil {
			s.mutex.Unlock()
			// topic already have an active subscription
			out <- SubscriptionError{err, topic}
			return
		}
		s.stopGroup.Add(1)
		s.mutex.Unlock()
		errH := <-sub.start(s.m, &s.stopGroup)
		// if err is nil, handler context was cancelled (Stop was called)
		if errH.Err == nil {
			errH.Err = ErrStopped
		} else {
			// since Stop holds mutex lock during his operations
			// this will result in a deadlock
			s.removeMap(topic)
		}
		out <- errH

	}()
	return out
}

// StopSubscription cancel active subscription on topic, if exists
func (s *Service) StopSubscription(topic string) error {
	s.mutex.Lock()
	sub, err := s.getMapLocked(topic)
	s.mutex.Unlock()
	if err != nil {
		return err
	}
	sub.stop()
	s.removeMap(topic)
	return nil
}

// Stop cancel all active subscriptions on Service.
// When this method returns all subscriptions are canceled and all handlers have returned
func (s *Service) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	wg := &sync.WaitGroup{}
	wg.Add(len(s.subscriptions))
	for _, sub := range s.subscriptions {
		go func(sub *subscription) {
			defer wg.Done()
			sub.stop()
		}(sub)
	}
	// wait all subscription to close their SubscriptionError channel.
	// (kerol) probably this is useless
	s.stopGroup.Wait()
	// block until all handlers invoked by pubsub returns
	wg.Wait()
	s.deleteMapLocked()
}

// Subscribe delegate to underlying Messenger interface
func (s *Service) Subscribe(ctx context.Context, topic string, h Handler, opt *SubscriptionOptions) error {
	return s.m.Subscribe(ctx, topic, h, opt)
}

// Publish delegate to underlying Messenger interface
func (s *Service) Publish(ctx context.Context, topic string, m RawMessage) error {
	return s.m.Publish(ctx, topic, m)
}

func (s *Service) removeMap(topic string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.subscriptions, topic)
}

func (s *Service) addMapLocked(topic string, h Handler, opt *SubscriptionOptions) (sub *subscription, err error) {
	if _, ok := s.subscriptions[topic]; ok {
		return nil, ErrAlreadyExists
	}
	// this way canceling mainContext will result in all handlers stop
	ctx, cancel := context.WithCancel(s.mainContext)
	sub = newSubscription(ctxWithCancel{ctx, cancel}, topic, h, opt)
	s.subscriptions[topic] = sub
	return
}

func (s *Service) getMapLocked(topic string) (sub *subscription, err error) {
	var ok bool
	if sub, ok = s.subscriptions[topic]; !ok {
		err = ErrNotExists
	}
	return
}

// delete all subscriptions from map
func (s *Service) deleteMapLocked() {
	for key := range s.subscriptions {
		delete(s.subscriptions, key)
	}
}

// Multiplex given channels. Returned channel is closed when
// all in channels are closed
func Multiplex(channels ...<-chan SubscriptionError) <-chan SubscriptionError {
	out := make(chan SubscriptionError)
	wg := &sync.WaitGroup{}
	f := func(ch <-chan SubscriptionError) {
		defer wg.Done()
		for v := range ch {
			out <- v
		}
	}
	wg.Add(len(channels))
	for _, ch := range channels {
		go f(ch)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
