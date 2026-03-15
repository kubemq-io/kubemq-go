// Example: events-store/start-from-first
//
// Demonstrates subscribing to event store with StartFromFirstEvent.
// All stored events from the beginning are replayed, then new events
// continue to be delivered.
//
// Channel: go-events-store.start-from-first
// Client ID: go-events-store-start-from-first-client
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
		kubemq.WithClientId("go-events-store-start-from-first-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events-store.start-from-first"

	// First, send some events so there is data to replay.
	for i := 1; i <= 3; i++ {
		_, err := client.SendEventStore(ctx, kubemq.NewEventStore().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "stored-msg-%d", i)).
			SetMetadata("replay-test"))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Sent 3 events to store")

	received := make(chan struct{}, 3)

	// Subscribe with StartFromFirstEvent — replays all stored events.
	sub, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromFirstEvent(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromFirst] seq=%d body=%s\n", e.Sequence, string(e.Body))
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

	// Wait for events to arrive.
	time.Sleep(3 * time.Second)
	fmt.Printf("Received %d events from replay\n", len(received))
}
