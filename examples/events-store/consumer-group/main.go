// Example: events-store/consumer-group
//
// Demonstrates load-balanced event store consumption with consumer groups.
// When multiple subscribers share the same group, each event is delivered
// to exactly one member.
//
// Channel: go-events-store.consumer-group
// Client ID: go-events-store-consumer-group-client
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
		kubemq.WithClientId("go-events-store-consumer-group-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events-store.consumer-group"
	group := "go-events-store-worker-group"
	received := make(chan struct{})

	// Subscribe with a consumer group for load-balanced event store delivery.
	sub, err := client.SubscribeToEventsStore(ctx, channel, group,
		kubemq.StartFromFirstEvent(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[ConsumerGroup] seq=%d body=%s\n", e.Sequence, string(e.Body))
			select {
			case received <- struct{}{}:
			default:
			}
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Subscription error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	// Send an event.
	_, err = client.SendEventStore(ctx, kubemq.NewEventStore().
		SetChannel(channel).
		SetBody([]byte("group event")).
		SetMetadata("group-demo"))
	if err != nil {
		log.Fatal(err)
	}

	select {
	case <-received:
		fmt.Println("Consumer group demo complete")
	case <-ctx.Done():
		log.Fatal("Timed out")
	}
}
