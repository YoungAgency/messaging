package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/youngagency/messaging/pubsub/types"

	"github.com/stretchr/testify/assert"
)

func initClient() types.PubSubMessenger {
	messagingClient := &PubSubMessengerClient{Host: "localhost", Port: 8519, ProjectId: "youngplatform", SubscriptionName: "rewardapi"}
	return messagingClient
}
func TestSubscribe(t *testing.T) {
	messagingClient := initClient()
	finish := make(chan bool, 0)
	messagingClient.Subscribe("testevent", func(ctx context.Context, msgId string, timestamp int64, msg []byte) error {
		finish <- true
		return nil
	})
	test := struct {
		TestString string
		TestInt    int
	}{
		"ciao",
		1,
	}
	if err := messagingClient.Publish(test, "testevent"); err != nil {
		assert.Error(t, err, "Something went wrong")
	}
	result := <-finish
	assert.True(t, result, "Something went wrong")
}

func TestMultiple(t *testing.T) {
	messagingClient := initClient()
	count := 0
	var wg sync.WaitGroup
	msgSent := rand.Intn(200)
	wg.Add(msgSent)
	messagingClient.Subscribe("testevent", func(ctx context.Context, msgId string, timestamp int64, msg []byte) error {
		test := struct {
			TestString string
			TestInt    int
		}{
			"ciao",
			1,
		}
		json.Unmarshal(msg, &test)
		fmt.Printf("Event received: %v", test.TestInt)
		count++
		wg.Done()
		return nil
	})

	for i := 0; i < msgSent; i++ {
		fmt.Println("New")
		msg := struct {
			TestString string
			TestInt    int
		}{
			"Event",
			i,
		}
		if err := messagingClient.Publish(msg, "testevent"); err != nil {
			assert.Error(t, err, "Something went wrong")
		}
	}
	wg.Wait()
	assert.Equal(t, msgSent, count, "Something went wrong")
}
