// Example: events/consumer-group
//
// Demonstrates load-balanced event consumption using consumer groups.
// When multiple subscribers share the same group on the same channel,
// each event is delivered to exactly one subscriber in the group.
//
// Channel: go-events.consumer-group
// Client ID: go-events-consumer-group-client
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
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-events-consumer-group-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events.consumer-group"
	group := "go-events-worker-group"
	received := make(chan struct{})

	// Subscribe with a consumer group for load-balanced delivery.
	sub, err := client.SubscribeToEvents(ctx, channel, group,
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("Consumer group received: channel=%s body=%s\n",
				event.Channel, event.Body)
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

	// Publish an event to the group channel.
	err = client.SendEvent(ctx, kubemq.NewEvent().
		SetChannel(channel).
		SetBody([]byte("hello consumer group")).
		SetMetadata("group-demo"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Event published to consumer group channel")

	select {
	case <-received:
		fmt.Println("Consumer group demo complete")
	case <-ctx.Done():
		log.Fatal("Timed out waiting for consumer group event")
	}
}
