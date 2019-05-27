# messaging
Messaging lib for Golang.
Google PubSub and Redis PubSub are currently implemented.

# Connect to Google PubSub
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

# Publish a message
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

# Subscribe to a message
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
if Handler func returns an error, received message won't be acknowledged. Return nil if you want to ignore the message
Each topic subscription can have different subscription options.
This method will block until an error happens.