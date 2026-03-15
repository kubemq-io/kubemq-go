// Example: events-store/start-from-last
//
// Demonstrates subscribing to event store with StartFromLastEvent.
// The last stored event is replayed, then new events continue.
//
// Channel: go-events-store.start-from-last
// Client ID: go-events-store-start-from-last-client
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
		kubemq.WithClientId("go-events-store-start-from-last-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events-store.start-from-last"

	// Send some events so there is a "last" event.
	for i := 1; i <= 3; i++ {
		_, err := client.SendEventStore(ctx, kubemq.NewEventStore().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "msg-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Sent 3 events")

	received := make(chan struct{})

	// Subscribe with StartFromLastEvent — starts from the most recent stored event.
	sub, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromLastEvent(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromLast] seq=%d body=%s\n", e.Sequence, string(e.Body))
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

	select {
	case <-received:
		fmt.Println("Start from last demo complete")
	case <-ctx.Done():
		log.Fatal("Timed out")
	}
}
