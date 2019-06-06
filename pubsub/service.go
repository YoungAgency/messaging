package pubsub

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrStopped is returned when subscription was cancelled by stopping it
	ErrStopped = errors.New("Stop called for this topic")
	// ErrAlreadyExists is returned by AddHandler when
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
	stopGroup   sync.WaitGroup
	// protect from concurrent access next fields
	mutex sync.Mutex
	// map topic-subscriptions
	subscriptions map[string]*subscription
}

// ErrHandler is a type that wrap an error and the topic
// on which it occured
type ErrHandler struct {
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

// AddHandler set given handler for topic, it also starts  it
func (s *Service) AddHandler(topic string, h Handler, opt *SubscriptionOptions) <-chan ErrHandler {
	out := make(chan ErrHandler)
	go func() {
		defer close(out)
		if err := s.setHandler(topic, h, opt); err != nil {
			// topic already have an active subscription
			out <- ErrHandler{err, topic}
			return
		}
		err := <-s.startHandler(topic)
		// if err is nil, handler context was cancelled
		if err.Err == nil {
			err.Err = ErrStopped
		}
		out <- err
	}()
	return out
}

// StopHandler cancel active subscription on topic, if exists
func (s *Service) StopHandler(topic string) error {
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
	s.mutex.Lock()
	wg := &sync.WaitGroup{}
	wg.Add(len(s.subscriptions))
	for _, sub := range s.subscriptions {
		go func(sub *subscription) {
			defer wg.Done()
			sub.stop()
		}(sub)
	}
	s.mutex.Unlock()

	s.stopGroup.Wait()
	wg.Wait()
}

func (s *Service) startHandler(topic string) <-chan ErrHandler {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	out := make(chan ErrHandler)
	sub, _ := s.getMapLocked(topic)
	s.stopGroup.Add(1)
	go func() {
		defer s.stopGroup.Done()
		defer close(out)
		out <- ErrHandler{
			Err:   s.m.Subscribe(sub.ctx.ctx, topic, sub.h, sub.opt),
			Topic: topic,
		}
		// an error occured, delete handler stuff from service
		s.removeMap(topic)
	}()
	return out
}

// SetHandler set given handler for topic
func (s *Service) setHandler(topic string, h Handler, opt *SubscriptionOptions) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.addMapLocked(topic, h, opt)
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

func Multiplex(channels ...<-chan ErrHandler) <-chan ErrHandler {
	out := make(chan ErrHandler)
	wg := &sync.WaitGroup{}
	f := func(ch <-chan ErrHandler) {
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
