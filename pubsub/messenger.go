package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/YoungAgency/utils/date"

	"google.golang.org/api/option"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/pubsub"
	ps "cloud.google.com/go/pubsub"
)

type RawMessage struct {
	TopicName  string
	Data       []byte
	Attributes map[string]string
	MsgID      string
	Timestamp  int64
}

type Handler func(context.Context, RawMessage) error

type Subscriber interface {
	Subscribe(ctx context.Context, topic string, h Handler) error
}

type Publisher interface {
	Publish(ctx context.Context, topic string, m RawMessage) error
}

type Service interface {
	Subscriber
	Publisher
}

func NewService(ctx context.Context, opt *Options) Service {
	if opt == nil {
		panic("options must be set")
		// TODO validate options
	}
	client, err := ps.NewClient(ctx, opt.ProjectId, parseOptions(opt)...)
	if err != nil {
		panic(err)
	}
	return &PubSubService{
		c: client,
	}
}

type PubSubService struct {
	c             *ps.Client
	logger        *log.Logger
	token         string
	m             sync.Mutex
	topics        map[string]*ps.Topic
	subscriptions map[string]*ps.Subscription
}

func (s *PubSubService) Subscribe(ctx context.Context, topicName string, h Handler) error {
	topic, err := s.getTopic(ctx, topicName)
	if err != nil {
		return err
	}
	sub, err := s.getSubsciption(ctx, "DIO", topic)
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
		if s.token != "" {
			token, ok := msg.Attributes["token"]
			if !ok || token != s.token {
				err = errors.New("Unatuhanticatate message")
				return
			}
		}
		rm := RawMessage{
			TopicName: topicName,
			MsgID:     msg.ID,
			Timestamp: date.Timestamp(msg.PublishTime),
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
	/* if s.token != "" {
		message.Attributes = make(map[string]string)
		message.Attributes["token"] = s.token
	} */
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

func (s *PubSubService) getTopic(ctx context.Context, topicName string) (t *ps.Topic, err error) {
	topic := s.c.Topic(topicName)
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

func (s *PubSubService) getSubsciption(ctx context.Context, subscriptionName string, topic *pubsub.Topic) (sub *ps.Subscription, err error) {
	sub = s.c.Subscription(subscriptionName)
	// Create the topic if it doesn't exist.
	exists, err := sub.Exists(ctx)
	if err != nil {
		return
	}
	if !exists {
		return s.c.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic})
	}
	return
}

func parseOptions(opt *Options) (ret []option.ClientOption) {
	if len(opt.Host) == 0 {
		ret = make([]option.ClientOption, 0)
	} else {
		ret = make([]option.ClientOption, 3)
		ret[0] = option.WithoutAuthentication()
		ret[1] = option.WithEndpoint(fmt.Sprintf("%v:%v", opt.Host, opt.Port))
		ret[2] = option.WithGRPCDialOption(grpc.WithInsecure())
	}
	return
}

type Options struct {
	Host             string
	Port             int
	ProjectId        string
	SubscriptionName string
	Token            string
}
