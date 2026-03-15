// Example: patterns/fan-out
//
// Demonstrates the fan-out pattern using events.
// A single publisher sends events that are delivered to all subscribers
// on the channel. Each subscriber receives every event independently.
//
// Channel: go-patterns.fan-out
// Client ID: go-patterns-fan-out-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-patterns-fan-out-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-patterns.fan-out"
	var deliveries atomic.Int32

	// Create three independent subscribers (no consumer group = fan-out).
	for i := 1; i <= 3; i++ {
		subscriberID := i
		sub, err := client.SubscribeToEvents(ctx, channel, "",
			kubemq.WithOnEvent(func(event *kubemq.Event) {
				fmt.Printf("Subscriber %d received: body=%s\n", subscriberID, event.Body)
				deliveries.Add(1)
			}),
			kubemq.WithOnError(func(err error) {
				log.Printf("Subscriber %d error: %v", subscriberID, err)
			}),
		)
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Unsubscribe()
	}

	// Publish a single event — all 3 subscribers should receive it.
	err = client.SendEvent(ctx, kubemq.NewEvent().
		SetChannel(channel).
		SetBody([]byte("broadcast message")).
		SetMetadata("fan-out"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Event published to all subscribers")

	// Wait for deliveries.
	time.Sleep(2 * time.Second)
	fmt.Printf("Total deliveries: %d (expected 3 for fan-out)\n", deliveries.Load())
}
