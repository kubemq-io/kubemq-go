// Example: events/multiple-subscribers
//
// Demonstrates multiple subscribers receiving the same event.
// Without a consumer group, each subscriber gets every event (fan-out).
//
// Channel: go-events.multiple-subscribers
// Client ID: go-events-multiple-subscribers-client
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
		kubemq.WithClientId("go-events-multiple-subscribers-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events.multiple-subscribers"
	var count atomic.Int32

	// Create two subscribers on the same channel without a group.
	// Both subscribers should receive every event (fan-out).
	sub1, err := client.SubscribeToEvents(ctx, channel, "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("Subscriber 1 received: body=%s\n", event.Body)
			count.Add(1)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Sub1 error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub1.Unsubscribe()

	sub2, err := client.SubscribeToEvents(ctx, channel, "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("Subscriber 2 received: body=%s\n", event.Body)
			count.Add(1)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Sub2 error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub2.Unsubscribe()

	// Publish an event — both subscribers should receive it.
	err = client.SendEvent(ctx, kubemq.NewEvent().
		SetChannel(channel).
		SetBody([]byte("hello to all subscribers")).
		SetMetadata("fan-out"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Event published")

	// Wait briefly for both subscribers to receive the event.
	time.Sleep(2 * time.Second)
	fmt.Printf("Total deliveries: %d (expected 2 for fan-out)\n", count.Load())
}
