// Example: events/cancel-subscription
//
// Demonstrates how to cancel (unsubscribe from) an event subscription.
// After cancellation, no more events are delivered to the handler.
//
// Channel: go-events.cancel-subscription
// Client ID: go-events-cancel-subscription-client
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
		kubemq.WithClientId("go-events-cancel-subscription-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events.cancel-subscription"
	received := make(chan struct{})

	// Create a subscription.
	sub, err := client.SubscribeToEvents(ctx, channel, "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("Received: body=%s\n", event.Body)
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
	err = client.SendEvent(ctx, kubemq.NewEvent().
		SetChannel(channel).
		SetBody([]byte("before cancel")).
		SetMetadata("test"))
	if err != nil {
		log.Fatal(err)
	}

	select {
	case <-received:
		fmt.Println("Event received before cancellation")
	case <-ctx.Done():
		log.Fatal("Timed out")
	}

	// Cancel the subscription. After this, no more events are delivered.
	sub.Unsubscribe()
	fmt.Println("Subscription cancelled")

	// Check that the subscription is done.
	if sub.IsDone() {
		fmt.Println("Subscription confirmed done")
	}
}
