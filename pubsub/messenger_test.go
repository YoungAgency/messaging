package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPubSubMessenger_checkOptions(t *testing.T) {
	t.Run("Messenger without sub name with nil options should panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, "pubsub: subscription options can't be nil", r)
			} else {
				t.FailNow()
			}
		}()
		messenger := PubSubMessenger{
			opt: &Options{
				SubscriptionName: "",
			},
		}
		messenger.checkOptions(nil)
	})

	t.Run("Messenger without sub name with empty options sub name should panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, "pubsub: subscription name must be provided", r)
			} else {
				t.FailNow()
			}
		}()
		messenger := PubSubMessenger{
			opt: &Options{
				SubscriptionName: "",
			},
		}
		opt := &SubscriptionOptions{}
		messenger.checkOptions(opt)
	})

	t.Run("Messenger with sub name with empty options sub name should work", func(t *testing.T) {
		messenger := PubSubMessenger{
			opt: &Options{
				SubscriptionName: "foo",
			},
		}
		opt := &SubscriptionOptions{}
		newOpt := messenger.checkOptions(opt)
		assert.Equal(t, *opt, *newOpt)
	})

	t.Run("checkOptions should return a new pointer", func(t *testing.T) {
		messenger := PubSubMessenger{
			opt: &Options{
				SubscriptionName: "foo",
			},
		}
		opt := &SubscriptionOptions{}
		newOpt := messenger.checkOptions(opt)
		// test that check options returns a new pointer
		newOpt.ConcurrentHandlers = 100
		assert.NotEqual(t, opt, newOpt)
	})
}

func TestPubSubMessenger_subscriptionName(t *testing.T) {
	t.Run("Client sub name should be set", func(t *testing.T) {
		messenger := PubSubMessenger{
			opt: &Options{
				SubscriptionName: "foo",
			},
		}
		opt := &SubscriptionOptions{}
		topic := "bar"
		messenger.subscriptionName(opt, topic)
		assert.Equal(t, "foo-bar", opt.SubscriptionName)
	})

	t.Run("Options sub name should be set", func(t *testing.T) {
		messenger := PubSubMessenger{
			opt: &Options{
				SubscriptionName: "foo",
			},
		}
		opt := &SubscriptionOptions{
			SubscriptionName: "foobar",
		}
		topic := "bar"
		messenger.subscriptionName(opt, topic)
		assert.Equal(t, "foobar-bar", opt.SubscriptionName)
	})
}
