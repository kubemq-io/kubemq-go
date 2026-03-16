// Example: queues/send-receive
//
// Demonstrates basic queue send and receive operations.
// A message is sent to a queue and then consumed (pulled) from it.
//
// Channel: go-queues.send-receive
// Client ID: go-queues-send-receive-client
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
		kubemq.WithAddress("localhost", 50000), // TODO: Replace with your KubeMQ server address
		kubemq.WithClientId("go-queues-send-receive-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues.send-receive"

	// Send a single queue message.
	msg := kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("hello queue")).
		SetMetadata("greeting")

	result, err := client.SendQueueMessage(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}
	if result.IsError {
		log.Fatalf("Send failed: %s", result.Error)
	}
	fmt.Printf("Sent: id=%s\n", result.MessageID)

	// Receive (consume) messages from the queue.
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
	fmt.Printf("Received: %d messages\n", resp.MessagesReceived)
	for _, m := range resp.Messages {
		fmt.Printf("  body=%s metadata=%s\n", m.Body, m.Metadata)
	}
}

// Expected output:
// Sent: id=<message-id>
// Received: 1 messages
//   body=hello queue metadata=greeting
