# messaging
[![Build Status](https://www.travis-ci.org/YoungAgency/messaging.svg?branch=master)](https://www.travis-ci.org/YoungAgency/messaging)

Messaging lib for Golang.
Google PubSub and Redis PubSub are currently implemented.

### Connect to Google PubSub
```golang
func main() {
  ctx := context.TODO()
  opt := &pubsub.Options{
    ProjectID: "foo",
  }
  client := pubsub.NewMessenger(ctx, opt)
}
```
A new client will be created with given options. If something goes wrong connecting to pubsub panic is invoked.

### Publish a message
```golang
func main() {
  ctx := context.TODO()
  opt := &pubsub.Options{
    ProjectID: "foo",
  }
  client := pubsub.NewMessenger(ctx, opt)

  msg := pubsub.RawMessage{
    Data: make([]byte, 0),
    Attributes: nil,
  }
  topic := "bar"
  err := client.Publish(context.TODO(), topic, msg)
}
```

### Subscribe to a message
```golang
func main() {
  ctx := context.TODO()
  opt := &pubsub.Options{
    ProjectID: "foo",
  }
  client := pubsub.NewMessenger(ctx, opt)
  topic := "bar"
  h := func(ctx context.Context, msg pubsub.RawMessage) error {
    // TODO, handle new message
    return nil
  }
  subOptions := &pubsub.SubscriptionOptions{
    ConcurrentHandlers: 5,
    SubscriptionName: "baz",
  }

  err := client.Subscribe(ctx, topic, h, subOptions)
}
```
**if Handler func returns an error, received message won't be acknowledged**. *Return nil if you want to ignore the message*.
Each topic subscription can have different subscription options.
`Client.Subscribe` method will block until an error happens.

### Service
Service is a wrapper around a Messenger that permits to stop and wait until all subscription and handlers have returned.
A `Messenger` and a `Context` are required to initialize `Service`.
If the context passed during initialization is cancelled, all subscriptions are stopped.
It is not allowed to create multiple subscriptions on the same topic with the same Service.
#### Add subscriptions
To create a subscription on a topic `AddSubscription` method must be invoked. This method returns a `SubscriptionError` channel.
```go
type SubscriptionError struct {
	Err   error
	Topic string
}
```
This struct contains the error and topic of canceled subscription. If subscription was stopped by user `Err` will be `ErrStopped`.
#### Stop subscriptions
To stop a subscription `StopSubscription` method must be invoked. Invoke `Stop` method if you want to stop all active subscriptions.
Both methods block until all handles have returned.

Example:

```go
	ctx := context.TODO()
	client := NewMessenger(ctx, &Options{
		ProjectID: "foo",
	})
	service := NewService(ctx, client, nil)
	h := func(ctx context.Context, msg RawMessage) error {
		return nil
	}
	opt := &SubscriptionOptions{
		SubscriptionName:   "sub-name",
		ConcurrentHandlers: 10,
	}
	ch := Multiplex(
		service.AddSubscription("topic-1", h, opt),
		service.AddSubscription("topic-2", h, opt),
	)
	for err := range ch {
		// err is Subscription error
	}
  
```
