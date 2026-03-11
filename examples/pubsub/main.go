// Package main demonstrates Events pub/sub with the KubeMQ Go SDK v2.
//
// This example publishes an event and subscribes to receive it. Run with a
// KubeMQ server on localhost:50000 (e.g., docker run -d -p 50000:50000 kubemq/kubemq).
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

	// Create a KubeMQ client
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Channel for coordination
	received := make(chan struct{})

	// Subscribe to events on the "demo" channel
	sub, err := client.SubscribeToEvents(ctx, "demo", "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("Received: channel=%s body=%s\n", event.Channel, event.Body)
			close(received)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Subscription error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	// Publish an event
	err = client.SendEvent(ctx, kubemq.NewEvent().
		SetChannel("demo").
		SetBody([]byte("hello from v2")).
		SetMetadata("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Event published")

	// Wait for the event or timeout
	select {
	case <-received:
	case <-ctx.Done():
		log.Fatal("Timed out waiting for event")
	}
}
