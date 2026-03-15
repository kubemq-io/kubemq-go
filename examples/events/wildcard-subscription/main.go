// Example: events/wildcard-subscription
//
// Demonstrates subscribing to events using a wildcard pattern.
// Wildcard "go-events.wildcard.*" matches channels like
// "go-events.wildcard.a" and "go-events.wildcard.b".
//
// Channel: go-events.wildcard-subscription
// Client ID: go-events-wildcard-subscription-client
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
		kubemq.WithClientId("go-events-wildcard-subscription-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	received := make(chan struct{}, 2)

	// Subscribe with a wildcard pattern to match multiple channels.
	sub, err := client.SubscribeToEvents(ctx, "go-events.wildcard.*", "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("Wildcard received: channel=%s body=%s\n",
				event.Channel, event.Body)
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

	// Publish to two channels that match the wildcard pattern.
	for _, ch := range []string{"go-events.wildcard.a", "go-events.wildcard.b"} {
		err = client.SendEvent(ctx, kubemq.NewEvent().
			SetChannel(ch).
			SetBody([]byte("wildcard-msg")).
			SetMetadata("wildcard-demo"))
		if err != nil {
			log.Fatalf("SendEvent to %s: %v", ch, err)
		}
		fmt.Printf("Published to %s\n", ch)
	}

	// Wait for at least one event.
	select {
	case <-received:
		fmt.Println("Wildcard subscription demo complete")
	case <-ctx.Done():
		log.Fatal("Timed out waiting for wildcard event")
	}
}
