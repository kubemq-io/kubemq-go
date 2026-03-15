// Example: queues/peek-messages
//
// Demonstrates peeking at queue messages without consuming them.
// Peek allows you to view messages that remain in the queue.
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

	// Send a message to peek at.
	_, err = client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("peek at me")))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Message sent")

	// Peek at messages (IsPeak=true means view without consuming).
	peekResp, err := client.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
		Channel:             channel,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     5,
		IsPeak:              true,
	})
	if err != nil {
		log.Fatal(err)
	}
	if peekResp.IsError {
		log.Fatalf("Peek failed: %s", peekResp.Error)
	}
	fmt.Printf("Peek: %d messages (still in queue)\n", peekResp.MessagesReceived)
	for _, m := range peekResp.Messages {
		fmt.Printf("  peek: body=%s\n", m.Body)
	}
}
