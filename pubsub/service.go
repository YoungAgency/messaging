package pubsub

import (
	"context"
	"errors"
	"sync"
)

type subscription struct {
	h     Handler
	ctx   ctxWithCancel
	opt   *SubscriptionOptions
	errCh chan ErrHandler
}

func (s subscription) start() {

}

func (s subscription) stop() {
	s.ctx.cancel()
}

type ctxWithCancel struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type Service struct {
	m             Messenger
	mutex         sync.Mutex
	subscriptions map[string]subscription
	stopGroup     sync.WaitGroup
	mainContext   ctxWithCancel
}

func NewService(ctx context.Context, m Messenger) *Service {
	newCtx, cancel := context.WithCancel(ctx)
	return &Service{
		m: m,
		mainContext: ctxWithCancel{
			ctx:    newCtx,
			cancel: cancel,
		},
	}
}

type ErrHandler struct {
	Err   error
	Topic string
}

// AddHandler set given handler for topic, it also starts  it
func (s *Service) AddHandler(topic string, h Handler, opt *SubscriptionOptions) <-chan ErrHandler {
	out := make(chan ErrHandler)
	go func() {
		defer close(out)
		if err := s.setHandler(topic, h, opt); err != nil {
			// topic already have an active subscription
			out <- ErrHandler{
				Err:   err,
				Topic: topic,
			}
			return
		}
		err := <-s.startHandler(topic)
		// an error occured, delete handler stuff from service
		// if err is nil, handler context was cancelled
		s.removeMap(topic)
		out <- err
	}()
	return out
}

// RemoveHandler cancel active subscription on topic, if exists
func (s *Service) RemoveHandler(topic string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	w, err := s.getMapLocked(topic)
	if err != nil {
		return err
	}
	w.stop()
	return nil
}

/* func (s *Service) Start() <-chan ErrHandler {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.stopGroup.Add(len(s.subscriptions))
	for topic, w := range s.subscriptions {
		go func(topic string, w subscription) {
			defer s.stopGroup.Done()
			s.errs <- ErrHandler{
				Err:   s.m.Subscribe(w.ctx, topic, w.h, w.opt),
				Topic: topic,
			}
		}(topic, w)
	}
	return s.Errors()
}*/

func (s *Service) Stop(wait bool) {
	s.mainContext.cancel()
	if wait {
		s.stopGroup.Wait()
	}
}

// SetHandler set given handler for topic
func (s *Service) setHandler(topic string, h Handler, opt *SubscriptionOptions) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.addMapLocked(topic, h, opt)
}

func (s *Service) startHandler(topic string) <-chan ErrHandler {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	out := make(chan ErrHandler)
	w, err := s.getMapLocked(topic)
	if err != nil {
		panic(err)
	}
	s.stopGroup.Add(1)
	go func() {
		defer s.stopGroup.Done()
		defer close(out)
		out <- ErrHandler{
			Err:   s.m.Subscribe(w.ctx, topic, w.h, w.opt),
			Topic: topic,
		}
		s.removeMap(topic)
	}()
	return out
}

func (s *Service) addMapLocked(topic string, h Handler, opt *SubscriptionOptions) (err error) {
	_, ok := s.subscriptions[topic]
	if ok {
		err = errors.New("")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.subscriptions[topic] = subscription{
		h: h,
		ctx: ctxWithCancel{
			ctx:    ctx,
			cancel: cancel,
		},
		opt:   opt,
		errCh: make(chan ErrHandler),
	}
	return
}

func (s *Service) getMapLocked(topic string) (w subscription, err error) {
	var ok bool
	w, ok = s.subscriptions[topic]
	if !ok {
		err = errors.New("")
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
