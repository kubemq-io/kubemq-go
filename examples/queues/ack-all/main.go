// Example: queues/ack-all
//
// Demonstrates acknowledging all messages in a queue at once.
// This is useful for purging or bulk-acknowledging queue messages.
//
// Channel: go-queues.ack-all
// Client ID: go-queues-ack-all-client
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
		kubemq.WithClientId("go-queues-ack-all-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues.ack-all"

	// Send some messages to the queue.
	for i := 1; i <= 3; i++ {
		_, err := client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "msg-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Sent 3 messages")

	// Acknowledge all messages in the queue.
	ackResp, err := client.AckAllQueueMessages(ctx, &kubemq.AckAllQueueMessagesRequest{
		Channel:         channel,
		WaitTimeSeconds: 5,
	})
	if err != nil {
		log.Fatal(err)
	}
	if ackResp.IsError {
		log.Printf("Ack warning: %s", ackResp.Error)
	}
	fmt.Printf("Acknowledged %d messages\n", ackResp.AffectedMessages)
}
