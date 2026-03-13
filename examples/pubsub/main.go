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

	// Additional demonstrations
	fmt.Println("\n--- Stream send demo ---")
	if err := demoStreamSend(ctx, client); err != nil {
		log.Printf("Stream send demo error: %v", err)
	}

	fmt.Println("\n--- Consumer group demo ---")
	if err := demoConsumerGroup(ctx, client); err != nil {
		log.Printf("Consumer group demo error: %v", err)
	}

	fmt.Println("\n--- Wildcard subscribe demo ---")
	if err := demoWildcardSubscribe(ctx, client); err != nil {
		log.Printf("Wildcard subscribe demo error: %v", err)
	}
}

// demoStreamSend demonstrates SendEventStream for high-throughput event publishing.
func demoStreamSend(ctx context.Context, client *kubemq.Client) error {
	handle, err := client.SendEventStream(ctx)
	if err != nil {
		return fmt.Errorf("SendEventStream: %w", err)
	}
	defer handle.Close()

	received := make(chan struct{})
	sub, err := client.SubscribeToEvents(ctx, "stream-demo", "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("Stream received: channel=%s body=%s\n", event.Channel, event.Body)
			close(received)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Subscription error:", err)
		}),
	)
	if err != nil {
		return fmt.Errorf("SubscribeToEvents: %w", err)
	}
	defer sub.Unsubscribe()

	// Drain errors in background
	go func() {
		for err := range handle.Errors {
			log.Println("Stream send error:", err)
		}
	}()

	// Send events via stream
	for i := 0; i < 3; i++ {
		ev := kubemq.NewEvent().
			SetChannel("stream-demo").
			SetBody([]byte(fmt.Sprintf("stream-msg-%d", i))).
			SetMetadata("stream-demo")
		if err := handle.Send(ev); err != nil {
			return fmt.Errorf("handle.Send: %w", err)
		}
		fmt.Printf("Stream sent event %d\n", i+1)
	}

	select {
	case <-received:
		fmt.Println("Stream send demo: received event")
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for stream event")
	}
	return nil
}

// demoConsumerGroup demonstrates load-balanced consumption with a consumer group.
func demoConsumerGroup(ctx context.Context, client *kubemq.Client) error {
	ch := "demo-group-ch"
	group := "demo-group"
	received := make(chan struct{})

	sub, err := client.SubscribeToEvents(ctx, ch, group,
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("Consumer group received: channel=%s body=%s\n", event.Channel, event.Body)
			close(received)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Subscription error:", err)
		}),
	)
	if err != nil {
		return fmt.Errorf("SubscribeToEvents: %w", err)
	}
	defer sub.Unsubscribe()

	err = client.SendEvent(ctx, kubemq.NewEvent().
		SetChannel(ch).
		SetBody([]byte("hello consumer group")).
		SetMetadata("group-demo"))
	if err != nil {
		return fmt.Errorf("SendEvent: %w", err)
	}
	fmt.Println("Event published to consumer group channel")

	select {
	case <-received:
		fmt.Println("Consumer group demo: received event")
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for consumer group event")
	}
	return nil
}

// demoWildcardSubscribe demonstrates subscribing with a wildcard pattern (demo.*).
func demoWildcardSubscribe(ctx context.Context, client *kubemq.Client) error {
	received := make(chan struct{}, 2)

	sub, err := client.SubscribeToEvents(ctx, "demo.*", "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("Wildcard received: channel=%s body=%s\n", event.Channel, event.Body)
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
		return fmt.Errorf("SubscribeToEvents: %w", err)
	}
	defer sub.Unsubscribe()

	// Publish to demo.a and demo.b; both should match demo.*
	for _, ch := range []string{"demo.a", "demo.b"} {
		err = client.SendEvent(ctx, kubemq.NewEvent().
			SetChannel(ch).
			SetBody([]byte("wildcard-msg")).
			SetMetadata("wildcard-demo"))
		if err != nil {
			return fmt.Errorf("SendEvent to %s: %w", ch, err)
		}
		fmt.Printf("Published to %s\n", ch)
	}

	select {
	case <-received:
		fmt.Println("Wildcard demo: received event(s)")
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for wildcard event")
	}
	return nil
}
