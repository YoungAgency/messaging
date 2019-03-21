package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
	grpc "google.golang.org/grpc"
)

type PubSubMessengerClient struct {
	Client           *pubsub.Client
	Host             string
	Port             int
	ProjectId        string
	SubscriptionName string
	Token            string
}

func (m *PubSubMessengerClient) Connect() error {
	ctx := context.Background()
	var client *pubsub.Client
	var err error
	if len(m.Host) == 0 {
		client, err = pubsub.NewClient(ctx, m.ProjectId)
	} else {
		client, err = pubsub.NewClient(ctx, m.ProjectId, option.WithoutAuthentication(), option.WithEndpoint(fmt.Sprintf("%v:%v", m.Host, m.Port)), option.WithGRPCDialOption(grpc.WithInsecure()))
	}
	if err != nil {
		return err
	}
	m.Client = client
	return nil
}

func (m *PubSubMessengerClient) Publish(obj interface{}, topicName string) error {
	ctx := context.Background()
	topic := m.Client.Topic(topicName)
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	message := &pubsub.Message{Data: data}
	if m.Token != "" {
		message.Attributes = make(map[string]string)
		message.Attributes["token"] = m.Token
	}
	result := topic.Publish(ctx, message)

	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	_, err = result.Get(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (m *PubSubMessengerClient) Subscribe(topicName string, handlerFunc func(context.Context, string, string, int64, []byte) error) error {
	ctx := context.Background()
	topic, err := m.createTopic(ctx, m.Client, topicName)
	if err != nil {
		return err
	}
	subscription, err := m.createSubscription(ctx, m.Client, m.SubscriptionName+"-"+topicName, topic)
	if err != nil {
		return err
	}
	go func() {
		err = subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			if m.Token != "" {
				token, ok := msg.Attributes["token"]
				if !ok || token != m.Token {
					fmt.Printf("Unauthenticated message %v\n", msg.ID)
					msg.Ack()
					return
				}
			}
			err := handlerFunc(ctx, topicName, msg.ID, msg.PublishTime.UnixNano()/int64(time.Millisecond), msg.Data)
			if err != nil {
				fmt.Printf("Error processing message %v. %v\n", msg.ID, err.Error())
				msg.Nack()
			} else {
				msg.Ack()
			}
		})
		fmt.Println("Stoppped receiving")
		if err != nil {
			fmt.Printf("Subscription error: %v\n", err.Error())
			go func() {
				m.Subscribe(topicName, handlerFunc)
			}()
			return
		}
	}()
	return nil
}

func (m *PubSubMessengerClient) SubscribeWithLimit(topicName string, maxConcurrent int, handlerFunc func(context.Context, string, string, int64, []byte) error) error {
	ctx := context.Background()
	topic, err := m.createTopic(ctx, m.Client, topicName)
	if err != nil {
		return err
	}
	subscription, err := m.createSubscription(ctx, m.Client, m.SubscriptionName+"-"+topicName, topic)
	if err != nil {
		return err
	}
	if maxConcurrent < 0 {
		maxConcurrent = 0
	}
	subscription.ReceiveSettings.MaxOutstandingMessages = maxConcurrent
	go func() {
		err = subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			if m.Token != "" {
				token, ok := msg.Attributes["token"]
				if !ok || token != m.Token {
					fmt.Printf("Unauthenticated message %v\n", msg.ID)
					msg.Ack()
					return
				}
			}
			err := handlerFunc(ctx, topicName, msg.ID, msg.PublishTime.UnixNano()/int64(time.Millisecond), msg.Data)
			if err != nil {
				fmt.Printf("Error processing message %v. %v\n", msg.ID, err.Error())
				msg.Nack()
			} else {
				msg.Ack()
			}
		})
		fmt.Println("Stoppped receiving")
		if err != nil {
			fmt.Printf("Subscription error: %v\n", err.Error())
			go func() {
				m.Subscribe(topicName, handlerFunc)
			}()
			return
		}
	}()
	return nil
}

func (m *PubSubMessengerClient) createTopic(ctx context.Context, client *pubsub.Client, topicName string) (*pubsub.Topic, error) {
	topic := client.Topic(topicName)
	// Create the topic if it doesn't exist.
	exists, err := topic.Exists(ctx)
	if err != nil {
		return topic, err
	}
	if !exists {
		_, err = client.CreateTopic(ctx, topicName)
		if err != nil {
			return topic, err
		}
	}
	return topic, nil
}
func (m *PubSubMessengerClient) createSubscription(ctx context.Context, client *pubsub.Client, subscriptionName string, topic *pubsub.Topic) (*pubsub.Subscription, error) {
	subscription := client.Subscription(subscriptionName)
	// Create the topic if it doesn't exist.
	exists, err := subscription.Exists(ctx)
	if err != nil {
		return subscription, err
	}
	if !exists {
		_, err = client.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic})
		if err != nil {
			return subscription, err
		}
	}
	return subscription, nil
}
