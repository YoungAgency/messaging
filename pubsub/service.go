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

type ctxWithCancel struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// Service is a wrapper around a Messenger that permits to stop
// and wait to return all subscription and handlers
type Service struct {
	m           Messenger
	mainContext context.Context
	stopMutex   sync.Mutex
	stopGroup   sync.WaitGroup
	// protect from concurrent access next fields
	mutex sync.Mutex
	// map topic-subscriptions
	subscriptions map[string]*subscription
}

// SubscriptionError is a type that wrap an error and the topic
// on which it occured
type SubscriptionError struct {
	Err   error
	Topic string
}

// NewService returns a new initialized service
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
		err := s.addMapLocked(topic, h, opt)
		s.mutex.Unlock()
		if err != nil {
			// topic already have an active subscription
			out <- SubscriptionError{err, topic}
			return
		}
		errH := <-s.startHandler(topic)
		// if err is nil, handler context was cancelled
		if errH.Err == nil {
			errH.Err = ErrStopped
		}
		out <- errH
		s.removeMap(topic)
	}()
	return out
}

// StopSubscription cancel active subscription on topic, if exists
func (s *Service) StopSubscription(topic string) error {
	s.stopMutex.Lock()
	defer s.stopMutex.Unlock()
	s.mutex.Lock()
	sub, err := s.getMapLocked(topic)
	s.mutex.Unlock()
	if err != nil {
		return err
	}
	sub.stop()
	return nil
}

// Stop cancel all active subscriptions on Service.
// When this method returns all subscriptions are canceled and all handlers have returned
func (s *Service) Stop() {
	s.stopMutex.Lock()
	defer s.stopMutex.Unlock()
	wg := &sync.WaitGroup{}
	s.mutex.Lock()
	wg.Add(len(s.subscriptions))
	for _, sub := range s.subscriptions {
		go func(sub *subscription) {
			defer wg.Done()
			sub.stop()
		}(sub)
	}
	s.mutex.Unlock()
	// wait all subscription to close their ErrHandlerChan
	s.stopGroup.Wait()
	// wait all handlers to return
	wg.Wait()
}

// Subscribe delegate to underlying Messenger interface
func (s *Service) Subscribe(ctx context.Context, topic string, h Handler, opt *SubscriptionOptions) error {
	return s.m.Subscribe(ctx, topic, h, opt)
}

// Publish delegate to underlying Messenger interface
func (s *Service) Publish(ctx context.Context, topic string, m RawMessage) error {
	return s.m.Publish(ctx, topic, m)
}

func (s *Service) startHandler(topic string) <-chan SubscriptionError {
	s.stopMutex.Lock()
	defer s.stopMutex.Unlock()
	s.mutex.Lock()
	sub, _ := s.getMapLocked(topic)
	s.mutex.Unlock()

	out := make(chan SubscriptionError)
	s.stopGroup.Add(1)
	go func() {
		defer s.stopGroup.Done()
		defer close(out)
		out <- SubscriptionError{
			Err:   s.m.Subscribe(sub.ctx.ctx, topic, sub.h, sub.opt),
			Topic: topic,
		}
	}()
	return out
}

func (s *Service) addMapLocked(topic string, h Handler, opt *SubscriptionOptions) (err error) {
	_, ok := s.subscriptions[topic]
	if ok {
		return ErrAlreadyExists
	}
	// this way canceling mainContext will result in all handlers stop
	ctx, cancel := context.WithCancel(s.mainContext)
	s.subscriptions[topic] = newSubscription(ctxWithCancel{ctx, cancel}, h, opt)
	return
}

func (s *Service) getMapLocked(topic string) (sub *subscription, err error) {
	var ok bool
	sub, ok = s.subscriptions[topic]
	if !ok {
		return nil, ErrNotExists
	}
	return
}

func (s *Service) removeMap(topic string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.subscriptions, topic)
}

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
