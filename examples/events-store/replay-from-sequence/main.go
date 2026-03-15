// Example: events-store/replay-from-sequence
//
// Demonstrates subscribing to event store with StartFromSequence.
// Events are replayed starting from a specific sequence number.
//
// Channel: go-events-store.replay-from-sequence
// Client ID: go-events-store-replay-from-sequence-client
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
		kubemq.WithClientId("go-events-store-replay-from-sequence-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events-store.replay-from-sequence"

	// Send some events to build up sequence numbers.
	for i := 1; i <= 5; i++ {
		_, err := client.SendEventStore(ctx, kubemq.NewEventStore().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "msg-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Sent 5 events")

	received := make(chan struct{}, 5)

	// Subscribe starting at sequence 3 — replays events from seq 3 onward.
	sub, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromSequence(3),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromSeq(3)] seq=%d body=%s\n", e.Sequence, string(e.Body))
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

	time.Sleep(3 * time.Second)
	fmt.Printf("Received %d events from sequence 3 onward\n", len(received))
}
