// Example: patterns/work-queue
//
// Demonstrates the work queue pattern using queues.
// Multiple messages are sent to a queue and consumed by workers.
// Each message is processed by exactly one worker (competing consumers).
//
// Channel: go-patterns.work-queue
// Client ID: go-patterns-work-queue-client
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
		kubemq.WithClientId("go-patterns-work-queue-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-patterns.work-queue"

	// Producer: send multiple work items to the queue.
	for i := 1; i <= 5; i++ {
		msg := kubemq.NewQueueMessage().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "task-%d", i)).
			SetMetadata(fmt.Sprintf("priority-%d", i))

		result, err := client.SendQueueMessage(ctx, msg)
		if err != nil {
			log.Fatal(err)
		}
		if result.IsError {
			log.Printf("Send error: %s", result.Error)
		} else {
			fmt.Printf("Enqueued: task-%d (id=%s)\n", i, result.MessageID)
		}
	}

	// Worker: consume and process work items.
	resp, err := client.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
		Channel:             channel,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     5,
		IsPeak:              false,
	})
	if err != nil {
		log.Fatal(err)
	}
	if resp.IsError {
		log.Fatalf("Receive failed: %s", resp.Error)
	}

	fmt.Printf("\nWorker processed %d tasks:\n", resp.MessagesReceived)
	for _, m := range resp.Messages {
		fmt.Printf("  - body=%s metadata=%s\n", m.Body, m.Metadata)
	}
}
