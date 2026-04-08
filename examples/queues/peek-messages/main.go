// Example: queues/peek-messages
//
// Demonstrates peeking at queue messages without consuming them.
// Messages are read (peeked) but not removed from the queue —
// they remain available for normal consumption afterwards.
//
// Channel: go-queues.peek-messages
// Client ID: go-queues-peek-messages-client
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
		kubemq.WithClientId("go-queues-peek-messages-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues.peek-messages"

	// Send 3 messages to the queue.
	for i := 1; i <= 3; i++ {
		_, err = client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "message-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Sent 3 messages")

	// Peek: poll with AutoAck=false so messages stay in the queue.
	resp, err := client.PollQueue(ctx, &kubemq.PollRequest{
		Channel:            channel,
		MaxItems:           10,
		WaitTimeoutSeconds: 5,
		AutoAck:            false,
	})
	if err != nil {
		log.Fatal(err)
	}
	if resp.IsError {
		log.Fatalf("Poll failed: %s", resp.Error)
	}
	fmt.Printf("Peeked: %d messages (not consumed)\n", len(resp.Messages))
	for _, dsMsg := range resp.Messages {
		fmt.Printf("  body=%s\n", dsMsg.Message.Body)
	}

	// Messages were only peeked, not permanently consumed. Clean up by
	// bulk-acknowledging them so the queue is empty after the demo.
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
