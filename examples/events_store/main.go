// Package main demonstrates Events Store features with the KubeMQ Go SDK v2.
//
// This example shows unary send, stream send, and subscription with different
// start positions. Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kubemq-io/kubemq-go/v2"
)

// All 6 Events Store start positions:
//
// 1. StartFromNewEvents()       — only new events published after subscription
// 2. StartFromFirstEvent()      — replay all stored events from the beginning
// 3. StartFromLastEvent()      — replay last event, then continue with new ones
// 4. StartFromSequence(n)      — replay starting at sequence number n (must be > 0)
// 5. StartFromTime(t)          — replay from specific point in time (Unix nanos)
// 6. StartFromTimeDelta(d)     — replay from now minus duration (e.g. 1*time.Hour)
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

	channel := "demo-es"
	errHandler := func(err error) { log.Println("Subscription error:", err) }

	// 1. Subscribe with StartFromFirstEvent — receive all stored + new events
	sub, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromFirstEvent(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromFirstEvent] seq=%d body=%s\n", e.Sequence, string(e.Body))
		}),
		kubemq.WithOnError(errHandler),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	// 4. Subscribe with StartFromSequence(1) — receive from sequence 1 onward
	subSeq, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromSequence(1),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromSequence(1)] seq=%d body=%s\n", e.Sequence, string(e.Body))
		}),
		kubemq.WithOnError(errHandler),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer subSeq.Unsubscribe()

	// 5. Subscribe with StartFromNewEvents() — only events published after subscribe
	subNew, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromNewEvents(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromNewEvents] seq=%d body=%s\n", e.Sequence, string(e.Body))
		}),
		kubemq.WithOnError(errHandler),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer subNew.Unsubscribe()

	// 2. Send event store (unary) using NewEventStore builder
	eventStore := kubemq.NewEventStore().
		SetChannel(channel).
		SetBody([]byte("data")).
		SetMetadata("meta")
	result, err := client.SendEventStore(ctx, eventStore)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Unary send: id=%s sent=%v\n", result.Id, result.Sent)
	if result.Err != nil {
		log.Printf("Unary send error: %v\n", result.Err)
	}

	// 3. Stream send using SendEventStoreStream
	// Returns (*EventStoreStreamHandle, error) with Send, Results, Done, Close
	streamHandle, err := client.SendEventStoreStream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer streamHandle.Close()

	// Drain Results and Done in background
	go func() {
		for r := range streamHandle.Results {
			fmt.Printf("Stream result: eventId=%s sent=%v err=%s\n", r.EventID, r.Sent, r.Error)
		}
	}()

	// Send events on the stream
	for i := 0; i < 2; i++ {
		ev := kubemq.NewEventStore().
			SetChannel(channel).
			SetBody([]byte(fmt.Sprintf("stream-data-%d", i))).
			SetMetadata("stream-meta")
		if err := streamHandle.Send(ev); err != nil {
			log.Fatal(err)
		}
	}

	// Give subscribers time to receive; stream Results are drained in goroutine above
	time.Sleep(2 * time.Second)

	fmt.Println("Events Store example complete.")
}
