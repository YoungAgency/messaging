package pubsub

import (
	"context"
	"log"
	"os"
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

type RawMessage struct {
	TopicName  string
	Data       []byte
	Attributes map[string]string
	MsgID      string
	Timestamp  int64
}

type Handler func(context.Context, RawMessage) error

func NewService(ctx context.Context, opt *Options) Messenger {
	client, err := ps.NewClient(ctx, opt.ProjectID, parseOptions(opt)...)
	if err != nil {
		panic(err)
	}
	return &PubSubService{
		c:      client,
		logger: log.New(os.Stdout, "PubSub: ", 0),
		opt:    opt,
	}
}

type PubSubService struct {
	c      *ps.Client
	logger *log.Logger
	opt    *Options
}

func (s *PubSubService) Subscribe(ctx context.Context, topicName string, h Handler, opt *SubscriptionOptions) error {
	topic, err := s.getTopic(ctx, topicName)
	if err != nil {
		return err
	}
	sub, err := s.getSubsciption(ctx, topic, opt)
	if err != nil {
		return err
	}
	return sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var err error
		defer func() {
			if err != nil {
				// m.logger.Printf("error processing message %v\n", msg.ID)
				msg.Nack()
			} else {
				msg.Ack()
			}
		}()
		rm := RawMessage{
			TopicName: topicName,
			MsgID:     msg.ID,
			Timestamp: msg.PublishTime.UnixNano() / int64(time.Millisecond),
			Data:      msg.Data,
		}
		err = h(ctx, rm)
		return
	})
}

func (s *PubSubService) Publish(ctx context.Context, topicName string, m RawMessage) error {
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

func (s *PubSubService) getTopic(ctx context.Context, topicName string) (topic *ps.Topic, err error) {
	topic = s.c.Topic(topicName)
	// Create the topic if it doesn't exist.
	exists, err := topic.Exists(ctx)
	if err != nil {
		return
	}
	if !exists {
		return s.c.CreateTopic(ctx, topicName)
	}
	return
}

func (s *PubSubService) getSubsciption(ctx context.Context, topic *pubsub.Topic, opt *SubscriptionOptions) (sub *ps.Subscription, err error) {
	s.checkOptions(opt)
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
		sub.ReceiveSettings.MaxOutstandingMessages = opt.ConcurrentHandlers
	}
	return
}

func (s *PubSubService) checkOptions(opt *SubscriptionOptions) {
	if opt == nil {
		panic("pubsub: subscription options can't be nil")
	} else {
		if opt.SubscriptionName == "" {
			panic("invalid subscription name")
		}
	}
}
