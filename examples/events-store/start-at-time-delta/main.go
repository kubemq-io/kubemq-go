// Example: events-store/start-at-time-delta
//
// Demonstrates subscribing to event store with StartFromTimeDelta.
// Events are replayed from (now - delta). For example, 30 minutes ago.
//
// Channel: go-events-store.start-at-time-delta
// Client ID: go-events-store-start-at-time-delta-client
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
		kubemq.WithClientId("go-events-store-start-at-time-delta-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events-store.start-at-time-delta"
	received := make(chan struct{}, 5)

	// Subscribe starting from 30 minutes ago.
	sub, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromTimeDelta(30*time.Minute),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromTimeDelta] seq=%d body=%s\n", e.Sequence, string(e.Body))
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

	// Send an event that falls within the time delta window.
	_, err = client.SendEventStore(ctx, kubemq.NewEventStore().
		SetChannel(channel).
		SetBody([]byte("recent event")))
	if err != nil {
		log.Fatal(err)
	}

	select {
	case <-received:
		fmt.Println("Start at time delta demo complete")
	case <-ctx.Done():
		log.Fatal("Timed out")
	}
}
