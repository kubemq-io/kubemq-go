// Example: events-store/start-new-only
//
// Demonstrates subscribing to event store with StartFromNewEvents.
// Only events published after the subscription is established are delivered.
// Previously stored events are not replayed.
//
// Channel: go-events-store.start-new-only
// Client ID: go-events-store-start-new-only-client
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
		kubemq.WithClientId("go-events-store-start-new-only-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events-store.start-new-only"
	received := make(chan struct{})

	// Subscribe with StartFromNewEvents — only new events are delivered.
	sub, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromNewEvents(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromNew] seq=%d body=%s\n", e.Sequence, string(e.Body))
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

	// Send an event after subscribing.
	_, err = client.SendEventStore(ctx, kubemq.NewEventStore().
		SetChannel(channel).
		SetBody([]byte("new event only")).
		SetMetadata("new-only"))
	if err != nil {
		log.Fatal(err)
	}

	select {
	case <-received:
		fmt.Println("Start new only demo complete")
	case <-ctx.Done():
		log.Fatal("Timed out")
	}
}
