// Package main demonstrates Events Store features with the KubeMQ Go SDK v2.
//
// This example shows unary send, stream send, and subscription with all 6
// start positions plus consumer groups. Run with a KubeMQ server on
// localhost:50000 (e.g., docker run -d -p 50000:50000 kubemq/kubemq).
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
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "demo-es"
	errHandler := func(err error) { log.Println("Subscription error:", err) }

	// -------------------------------------------------------------------------
	// 1. StartFromFirstEvent — replay all stored events from the beginning
	// -------------------------------------------------------------------------
	sub1, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromFirstEvent(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromFirst] seq=%d body=%s\n", e.Sequence, string(e.Body))
		}),
		kubemq.WithOnError(errHandler),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub1.Unsubscribe()

	// -------------------------------------------------------------------------
	// 2. StartFromLastEvent — start from the last stored event, then new ones
	// -------------------------------------------------------------------------
	sub2, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromLastEvent(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromLast] seq=%d body=%s\n", e.Sequence, string(e.Body))
		}),
		kubemq.WithOnError(errHandler),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub2.Unsubscribe()

	// -------------------------------------------------------------------------
	// 3. StartFromNewEvents — only events published after subscription
	// -------------------------------------------------------------------------
	sub3, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromNewEvents(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromNew] seq=%d body=%s\n", e.Sequence, string(e.Body))
		}),
		kubemq.WithOnError(errHandler),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub3.Unsubscribe()

	// -------------------------------------------------------------------------
	// 4. StartFromSequence — replay starting at sequence number 1
	// -------------------------------------------------------------------------
	sub4, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromSequence(1),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromSeq(1)] seq=%d body=%s\n", e.Sequence, string(e.Body))
		}),
		kubemq.WithOnError(errHandler),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub4.Unsubscribe()

	// -------------------------------------------------------------------------
	// 5. StartFromTime — replay from a specific point in time
	// -------------------------------------------------------------------------
	since := time.Now().Add(-1 * time.Hour)
	sub5, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromTime(since),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromTime] seq=%d body=%s\n", e.Sequence, string(e.Body))
		}),
		kubemq.WithOnError(errHandler),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub5.Unsubscribe()

	// -------------------------------------------------------------------------
	// 6. StartFromTimeDelta — replay from 30 minutes ago
	// -------------------------------------------------------------------------
	sub6, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromTimeDelta(30*time.Minute),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[StartFromTimeDelta] seq=%d body=%s\n", e.Sequence, string(e.Body))
		}),
		kubemq.WithOnError(errHandler),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub6.Unsubscribe()

	// -------------------------------------------------------------------------
	// 7. Subscribe with consumer group — load-balanced across group members
	// -------------------------------------------------------------------------
	subGroup, err := client.SubscribeToEventsStore(ctx, channel, "my-consumer-group",
		kubemq.StartFromFirstEvent(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[ConsumerGroup] seq=%d body=%s\n", e.Sequence, string(e.Body))
		}),
		kubemq.WithOnError(errHandler),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer subGroup.Unsubscribe()

	// -------------------------------------------------------------------------
	// Send events (unary)
	// -------------------------------------------------------------------------
	eventStore := kubemq.NewEventStore().
		SetChannel(channel).
		SetBody([]byte("hello-store")).
		SetMetadata("meta")
	result, err := client.SendEventStore(ctx, eventStore)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Unary send: id=%s sent=%v\n", result.Id, result.Sent)

	// -------------------------------------------------------------------------
	// Send events (stream)
	// -------------------------------------------------------------------------
	streamHandle, err := client.SendEventStoreStream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer streamHandle.Close()

	go func() {
		for r := range streamHandle.Results {
			fmt.Printf("Stream result: eventId=%s sent=%v err=%s\n", r.EventID, r.Sent, r.Error)
		}
	}()

	for i := 0; i < 3; i++ {
		ev := kubemq.NewEventStore().
			SetChannel(channel).
			SetBody([]byte(fmt.Sprintf("stream-data-%d", i))).
			SetMetadata("stream-meta")
		if err := streamHandle.Send(ev); err != nil {
			log.Fatal(err)
		}
	}

	time.Sleep(2 * time.Second)
	fmt.Println("Events Store example complete.")
}
