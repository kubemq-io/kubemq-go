// Example: events-store/stream-send
//
// Demonstrates high-throughput event store publishing using SendEventStoreStream.
// Each sent event receives a confirmation result via the Results channel.
//
// Channel: go-events-store.stream-send
// Client ID: go-events-store-stream-send-client
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
		kubemq.WithClientId("go-events-store-stream-send-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-events-store.stream-send"

	// Open a bidirectional stream for event store publishing.
	handle, err := client.SendEventStoreStream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer handle.Close()

	// Read confirmation results in the background.
	go func() {
		for r := range handle.Results {
			fmt.Printf("Stream result: eventId=%s sent=%v err=%s\n",
				r.EventID, r.Sent, r.Error)
		}
	}()

	// Send multiple events via the stream.
	for i := range 5 {
		ev := kubemq.NewEventStore().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "stream-data-%d", i)).
			SetMetadata("stream-meta")
		if err := handle.Send(ev); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Stream sent event %d\n", i+1)
	}

	// Allow time for results to arrive.
	time.Sleep(2 * time.Second)
	fmt.Println("Event store stream send demo complete")
}
