// Example: queues-stream/auto-ack
//
// Demonstrates receiving queue messages with automatic acknowledgment.
// When AutoAck is true, messages are automatically acknowledged upon receipt.
//
// Channel: go-queues-stream.auto-ack
// Client ID: go-queues-stream-auto-ack-client
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
		kubemq.WithClientId("go-queues-stream-auto-ack-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues-stream.auto-ack"

	// Send a message.
	_, err = client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("auto-ack message")))
	if err != nil {
		log.Fatal(err)
	}

	// Poll with AutoAck=true — messages are acknowledged automatically.
	pollResp, err := client.PollQueue(ctx, &kubemq.QueuePollRequest{
		Channel:     channel,
		MaxItems:    10,
		WaitTimeout: 5000,
		AutoAck:     true,
	})
	if err != nil {
		log.Fatalf("PollQueue: %v", err)
	}
	if pollResp.IsError {
		log.Printf("Poll error: %s", pollResp.Error)
	} else {
		fmt.Printf("Auto-ack: received %d messages (automatically acknowledged)\n",
			len(pollResp.Messages))
		for _, m := range pollResp.Messages {
			fmt.Printf("  body=%s\n", m.Body)
		}
	}
}
