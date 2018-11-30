# messaging
Messaging lib for Golang

# Connect to PubSub
```golang
func main() {
  client := &pubsub.PubSubMessengerClient{Host: host, Port: port, ProjectId: projectId, SubscriptionName: subscriptionName}
}
```

# Publish a message
```golang
func main() {
  client := &pubsub.PubSubMessengerClient{Host: host, Port: port, ProjectId: projectId, SubscriptionName: subscriptionName}
  
  // Publish MyMessage on the topic mymessageevent
  type MyMessage struct {
    Text string `json:"Text"`
  }
  client.Publish(MyMessage{Text: "Hello"}, "mymessageevent")
}
```

# Subscribe to a message
```golang
func main() {
  client := &pubsub.PubSubMessengerClient{Host: host, Port: port, ProjectId: projectId, SubscriptionName: subscriptionName}
  
  // Publish MyMessage on the topic mymessageevent
  client.Subscribe("mymessageevent", func(ctx context.Context, msgId string, timestamp int64, msg []byte) error {
    var myMessage struct {
      Text string `json:"Text"`
    }
    if err := json.Unmarshal(msg, &myMessage); err != nil {
      return err
    }
    ...
    return nil
  })
}
```
