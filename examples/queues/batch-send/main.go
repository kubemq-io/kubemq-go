// Example: queues/batch-send
//
// Demonstrates sending multiple queue messages in a single batch operation.
// Batch send reduces round trips for high-throughput scenarios.
//
// Channel: go-queues.batch-send
// Client ID: go-queues-batch-send-client
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
		kubemq.WithClientId("go-queues-batch-send-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues.batch-send"

	// Create a batch of queue messages.
	batch := []*kubemq.QueueMessage{
		kubemq.NewQueueMessage().SetChannel(channel).SetBody([]byte("batch-msg-1")),
		kubemq.NewQueueMessage().SetChannel(channel).SetBody([]byte("batch-msg-2")),
		kubemq.NewQueueMessage().SetChannel(channel).SetBody([]byte("batch-msg-3")),
	}

	// Send all messages in a single batch operation.
	results, err := client.SendQueueMessages(ctx, batch)
	if err != nil {
		log.Fatal(err)
	}
	for i, r := range results {
		fmt.Printf("Batch[%d]: id=%s error=%v\n", i, r.MessageID, r.IsError)
	}
	fmt.Println("Batch send complete")
}
