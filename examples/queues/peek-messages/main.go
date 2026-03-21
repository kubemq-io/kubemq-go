// Example: queues/peek-messages
//
// Demonstrates polling queue messages without auto-ack (transactional).
// Messages are received and can be individually acknowledged or rejected.
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

	// Send a message to receive.
	_, err = client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("transactional message")))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Message sent")

	// Poll without auto-ack (transactional mode).
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
	fmt.Printf("Polled: %d messages (pending ack)\n", len(resp.Messages))
	for _, dsMsg := range resp.Messages {
		fmt.Printf("  body=%s\n", dsMsg.Message.Body)
	}

	// Acknowledge all messages in the transaction.
	if err := resp.AckAll(); err != nil {
		log.Fatalf("AckAll failed: %s", err)
	}
	fmt.Println("All messages acknowledged")
}
