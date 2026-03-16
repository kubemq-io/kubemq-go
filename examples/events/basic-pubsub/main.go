// Example: events/basic-pubsub
//
// Demonstrates basic fire-and-forget event publish/subscribe.
// A subscriber listens on a channel, then a publisher sends an event.
//
// Channel: go-events.basic-pubsub
// Client ID: go-events-basic-pubsub-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000), // TODO: Replace with your KubeMQ server address
		kubemq.WithClientId("go-events-basic-pubsub-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events.basic-pubsub"
	received := make(chan struct{})

	// Subscribe to events on the channel.
	sub, err := client.SubscribeToEvents(ctx, channel, "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("Received: channel=%s body=%s metadata=%s\n",
				event.Channel, event.Body, event.Metadata)
			close(received)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Subscription error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	// Allow subscription to fully establish before publishing.
	time.Sleep(time.Second)

	// Publish an event to the channel.
	err = client.SendEvent(ctx, kubemq.NewEvent().
		SetChannel(channel).
		SetBody([]byte("hello from Go SDK")).
		SetMetadata("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Event published")

	// Wait for the event to be received.
	select {
	case <-received:
		fmt.Println("Event received successfully")
	case <-ctx.Done():
		log.Fatal("Timed out waiting for event")
	}
}
