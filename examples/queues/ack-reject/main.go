// Example: queues/ack-reject
//
// Demonstrates individual message acknowledgment and rejection using
// the queue downstream stream. Messages can be individually acked
// (confirmed) or rejected (nacked) back to the queue.
//
// Channel: go-queues.ack-reject
// Client ID: go-queues-ack-reject-client
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
		kubemq.WithClientId("go-queues-ack-reject-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues.ack-reject"

	// Send two messages.
	for i := 1; i <= 2; i++ {
		_, err := client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "msg-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Sent 2 messages")

	// Poll messages with manual acknowledgment (AutoAck=false).
	pollResp, err := client.PollQueue(ctx, &kubemq.QueuePollRequest{
		Channel:     channel,
		MaxItems:    10,
		WaitTimeout: 5000,
		AutoAck:     false,
	})
	if err != nil {
		log.Fatal(err)
	}
	if pollResp.IsError {
		log.Fatalf("Poll failed: %s", pollResp.Error)
	}

	fmt.Printf("Polled %d messages (txID=%s)\n",
		len(pollResp.Messages), pollResp.TransactionID)
	for _, m := range pollResp.Messages {
		fmt.Printf("  body=%s\n", m.Body)
	}
}
