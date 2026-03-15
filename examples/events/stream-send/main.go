// Example: events/stream-send
//
// Demonstrates high-throughput event publishing using SendEventStream.
// A bidirectional stream is opened for sending multiple events efficiently.
//
// Channel: go-events.stream-send
// Client ID: go-events-stream-send-client
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
		kubemq.WithClientId("go-events-stream-send-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events.stream-send"
	received := make(chan struct{})

	// Subscribe to verify events arrive.
	sub, err := client.SubscribeToEvents(ctx, channel, "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("Stream received: channel=%s body=%s\n", event.Channel, event.Body)
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

	// Open a stream for high-throughput publishing.
	handle, err := client.SendEventStream(ctx)
	if err != nil {
		log.Fatalf("SendEventStream: %v", err)
	}
	defer handle.Close()

	// Drain errors in background.
	go func() {
		for err := range handle.Errors {
			log.Println("Stream send error:", err)
		}
	}()

	// Send multiple events via the stream.
	for i := range 5 {
		ev := kubemq.NewEvent().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "stream-msg-%d", i)).
			SetMetadata("stream-demo")
		if err := handle.Send(ev); err != nil {
			log.Fatalf("handle.Send: %v", err)
		}
		fmt.Printf("Stream sent event %d\n", i+1)
	}

	// Wait for at least one event to be received.
	select {
	case <-received:
		fmt.Println("Stream send demo complete")
	case <-ctx.Done():
		log.Fatal("Timed out waiting for stream event")
	}
}
