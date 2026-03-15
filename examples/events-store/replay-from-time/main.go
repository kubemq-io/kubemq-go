// Example: events-store/replay-from-time
//
// Demonstrates subscribing to event store with StartFromTime.
// Events are replayed starting from a specific point in time.
//
// Channel: go-events-store.replay-from-time
// Client ID: go-events-store-replay-from-time-client
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
		kubemq.WithClientId("go-events-store-replay-from-time-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events-store.replay-from-time"
	received := make(chan struct{}, 5)

	// Subscribe starting from 1 hour ago — replays events stored in the last hour.
	since := time.Now().Add(-1 * time.Hour)
	sub, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromTime(since),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromTime] seq=%d body=%s\n", e.Sequence, string(e.Body))
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

	// Send a new event that should be received.
	_, err = client.SendEventStore(ctx, kubemq.NewEventStore().
		SetChannel(channel).
		SetBody([]byte("event within time window")))
	if err != nil {
		log.Fatal(err)
	}

	select {
	case <-received:
		fmt.Println("Replay from time demo complete")
	case <-ctx.Done():
		log.Fatal("Timed out")
	}
}
