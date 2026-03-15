// Example: management/purge-queue
//
// Demonstrates purging all messages from a queue using AckAllQueueMessages.
// This effectively removes all pending messages from the queue.
//
// Channel: go-management.purge-queue
// Client ID: go-management-purge-queue-client
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
		kubemq.WithClientId("go-management-purge-queue-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-management.purge-queue"

	// Send some messages to the queue.
	for i := 1; i <= 5; i++ {
		_, err := client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "to-purge-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Sent 5 messages to queue")

	// Purge the queue by acknowledging all messages.
	purgeResp, err := client.AckAllQueueMessages(ctx, &kubemq.AckAllQueueMessagesRequest{
		Channel:         channel,
		WaitTimeSeconds: 5,
	})
	if err != nil {
		log.Fatal(err)
	}
	if purgeResp.IsError {
		log.Printf("Purge warning: %s", purgeResp.Error)
	} else {
		fmt.Printf("Purged %d messages from queue\n", purgeResp.AffectedMessages)
	}
}
