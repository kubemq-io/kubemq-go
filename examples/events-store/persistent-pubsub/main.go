// Example: events-store/persistent-pubsub
//
// Demonstrates basic persistent event store publish/subscribe.
// Events are stored and can be replayed. This example sends an event
// and subscribes with StartFromNewEvents to receive only new events.
//
// Channel: go-events-store.persistent-pubsub
// Client ID: go-events-store-persistent-pubsub-client
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
		kubemq.WithClientId("go-events-store-persistent-pubsub-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events-store.persistent-pubsub"
	received := make(chan struct{})

	// Subscribe to new events on the store channel.
	sub, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromNewEvents(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("Received: seq=%d body=%s\n", e.Sequence, string(e.Body))
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

	// Send a persistent event.
	result, err := client.SendEventStore(ctx, kubemq.NewEventStore().
		SetChannel(channel).
		SetBody([]byte("persistent hello")).
		SetMetadata("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Event stored: id=%s sent=%v\n", result.Id, result.Sent)

	select {
	case <-received:
		fmt.Println("Persistent pub/sub demo complete")
	case <-ctx.Done():
		log.Fatal("Timed out waiting for event")
	}
}
