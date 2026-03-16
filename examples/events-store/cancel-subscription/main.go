// Example: events-store/cancel-subscription
//
// Demonstrates cancelling an event store subscription.
// After Unsubscribe is called, no more events are delivered.
//
// Channel: go-events-store.cancel-subscription
// Client ID: go-events-store-cancel-subscription-client
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
		kubemq.WithClientId("go-events-store-cancel-subscription-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events-store.cancel-subscription"
	received := make(chan struct{})

	// Create an event store subscription.
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

	// Allow time for subscription to register on server
	time.Sleep(1 * time.Second)

	// Send an event and wait for it.
	_, err = client.SendEventStore(ctx, kubemq.NewEventStore().
		SetChannel(channel).
		SetBody([]byte("before cancel")))
	if err != nil {
		log.Fatal(err)
	}

	select {
	case <-received:
		fmt.Println("Event received before cancellation")
	case <-ctx.Done():
		log.Fatal("Timed out")
	}

	// Cancel the subscription.
	sub.Unsubscribe()
	fmt.Println("Subscription cancelled")

	if sub.IsDone() {
		fmt.Println("Subscription confirmed done")
	}
}
