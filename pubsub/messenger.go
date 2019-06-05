package pubsub

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/pubsub"
	ps "cloud.google.com/go/pubsub"
)

type Messenger interface {
	Subscriber
	Publisher
}

type Subscriber interface {
	Subscribe(ctx context.Context, topic string, h Handler, opt *SubscriptionOptions) error
}

type Publisher interface {
	Publish(ctx context.Context, topic string, m RawMessage) error
}

// RawMessage is pub sub message model
type RawMessage struct {
	TopicName        string
	Data             []byte
	Attributes       map[string]string
	MsgID            string
	Timestamp        int64
	SubscriptionName string
}

// Handler is invoked on new messages
type Handler func(context.Context, RawMessage) error

// NewMessenger returns a new Messenger with given
// Client code must not modify options
func NewMessenger(ctx context.Context, opt *Options) *PubSubMessenger {
	client, err := ps.NewClient(ctx, opt.ProjectID, parseOptions(opt)...)
	if err != nil {
		panic(err)
	}
	return &PubSubMessenger{
		c:        client,
		opt:      opt,
		topicMap: make(map[string]*ps.Topic),
	}
}

// PubSubMessenger implements Messenger interface
type PubSubMessenger struct {
	c        *ps.Client
	logger   *log.Logger
	opt      *Options
	topicMap map[string]*ps.Topic
	m        sync.Mutex
}

// SetLogger sets logger to messenger
// Processed messages ID will be printed
// Error returned by handler will be printed
func (s *PubSubMessenger) SetLogger(l *log.Logger) {
	s.logger = l
}

// Subscribe perform a subscription on topic with given options
func (s *PubSubMessenger) Subscribe(ctx context.Context, topicName string, h Handler, opt *SubscriptionOptions) error {
	// !! do not use opt param directly
	options := s.checkOptions(opt)
	topic, err := s.getTopic(ctx, topicName)
	if err != nil {
		return err
	}
	s.subscriptionName(options, topicName)
	sub, err := s.getSubscription(ctx, topic, options)
	if err != nil {
		return err
	}
	return sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var err error
		if s.logger != nil {
			defer func() {
				if err != nil {
					s.logger.Println("error processing message", msg.ID, "err:", err)
				} else {
					s.logger.Println("Successfully processed message", msg.ID)
				}
			}()
		}
		defer func() {
			if err != nil {
				msg.Nack()
			} else {
				msg.Ack()
			}
		}()
		rm := RawMessage{
			TopicName:        topicName,
			MsgID:            msg.ID,
			Timestamp:        msg.PublishTime.UnixNano() / int64(time.Millisecond),
			Data:             msg.Data,
			SubscriptionName: options.SubscriptionName,
		}
		err = h(context.Background(), rm)
		return
	})
}

func (s *PubSubMessenger) Publish(ctx context.Context, topicName string, m RawMessage) error {
	topic, err := s.getTopic(ctx, topicName)
	if err != nil {
		return err
	}
	message := &ps.Message{
		Data:       m.Data,
		Attributes: m.Attributes,
	}
	result := topic.Publish(ctx, message)
	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	_, err = result.Get(ctx)
	code := status.Code(err)
	switch code {
	case codes.OK:
		return nil
	default:
		return err
	}
}

func (s *PubSubMessenger) getTopic(ctx context.Context, topicName string) (topic *ps.Topic, err error) {
	s.m.Lock()
	defer s.m.Unlock()
	topic, ok := s.topicMap[topicName]
	if !ok {
		topic = s.c.Topic(topicName)
		// Create the topic if it doesn't exist.
		var exists bool
		exists, err = topic.Exists(ctx)
		if err != nil {
			return
		}
		if !exists {
			topic, err = s.c.CreateTopic(ctx, topicName)
			if err != nil {
				return
			}
		}
		s.topicMap[topicName] = topic
	}
	return
}

func (s *PubSubMessenger) getSubscription(ctx context.Context, topic *pubsub.Topic, opt *SubscriptionOptions) (sub *ps.Subscription, err error) {
	sub = s.c.Subscription(opt.SubscriptionName)
	// Create the topic if it doesn't exist.
	exists, err := sub.Exists(ctx)
	if err != nil {
		return
	}
	if !exists {
		sub, err = s.c.CreateSubscription(ctx, opt.SubscriptionName, pubsub.SubscriptionConfig{Topic: topic})
		if err != nil {
			return
		}
	}
	sub.ReceiveSettings.MaxOutstandingMessages = opt.ConcurrentHandlers
	return
}

func (s *PubSubMessenger) checkOptions(opt *SubscriptionOptions) *SubscriptionOptions {
	if opt == nil {
		panic("pubsub: subscription options can't be nil")
	}
	if s.opt.SubscriptionName == "" && opt.SubscriptionName == "" {
		panic("pubsub: subscription name must be provided")
	}
	return &SubscriptionOptions{
		ConcurrentHandlers: opt.ConcurrentHandlers,
		SubscriptionName:   opt.SubscriptionName,
	}
}

// subscriptionName modify opt SubscriptionName
func (s *PubSubMessenger) subscriptionName(opt *SubscriptionOptions, topic string) {
	if opt.SubscriptionName != "" {
		opt.SubscriptionName += "-" + topic
	} else {
		opt.SubscriptionName = s.opt.SubscriptionName + "-" + topic
	}
}
