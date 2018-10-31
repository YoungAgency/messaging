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
	Host             string
	Port             int
	ProjectId        string
	SubscriptionName string
}

func (m *PubSubMessengerClient) Publish(obj interface{}, topicName string) error {
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
	defer client.Close()

	topic, err := m.createTopic(ctx, client, topicName)
	if err != nil {
		return err
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	result := topic.Publish(ctx, &pubsub.Message{Data: data})

	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	_, err = result.Get(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (m *PubSubMessengerClient) Subscribe(topicName string, handlerFunc func(context.Context, string, int64, []byte) error) error {
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
	topic, err := m.createTopic(ctx, client, topicName)
	if err != nil {
		return err
	}
	subscription, err := m.createSubscription(ctx, client, m.SubscriptionName+"-"+topicName, topic)
	if err != nil {
		return err
	}
	go func() {
		err = subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			err := handlerFunc(ctx, msg.ID, msg.PublishTime.UnixNano()/int64(time.Millisecond), msg.Data)
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
