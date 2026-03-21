// Example: queues-stream/stream-receive
//
// Demonstrates receiving queue messages using NewQueueDownstreamReceiver + Poll.
// The receiver manages a persistent downstream stream with automatic reconnection.
//
// Channel: go-queues-stream.stream-receive
// Client ID: go-queues-stream-stream-receive-client
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
		kubemq.WithClientId("go-queues-stream-stream-receive-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues-stream.stream-receive"

	// Send a message to receive.
	_, err = client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("message to receive")))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Message sent")

	// Allow message to be committed to the queue.
	time.Sleep(time.Second)

	// Open a downstream receiver.
	receiver, err := client.NewQueueDownstreamReceiver(ctx)
	if err != nil {
		log.Fatalf("NewQueueDownstreamReceiver: %v", err)
	}
	defer receiver.Close()

	// Poll for messages (manual ack).
	resp, err := receiver.Poll(ctx, &kubemq.PollRequest{
		Channel:            channel,
		MaxItems:           10,
		WaitTimeoutSeconds: 5,
		AutoAck:            false,
	})
	if err != nil {
		log.Fatalf("Poll: %v", err)
	}

	for _, dm := range resp.Messages {
		if dm.Message != nil {
			fmt.Printf("Received: body=%s tx=%s\n", dm.Message.Body, dm.TransactionID)
		}
	}

	// Ack all received messages.
	if len(resp.Messages) > 0 {
		if err := resp.AckAll(); err != nil {
			log.Printf("AckAll: %v", err)
		}
		fmt.Println("Messages acknowledged")
	}
}
