// Example: queues-stream/stream-receive
//
// Demonstrates receiving queue messages using QueueDownstream.
// The downstream stream allows receiving messages with transaction control.
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

	// Open a downstream stream to receive messages.
	downstream, err := client.QueueDownstream(ctx)
	if err != nil {
		log.Fatalf("QueueDownstream: %v", err)
	}
	defer downstream.Close()

	// Send a Get request to pull messages.
	err = downstream.Send(&kubemq.QueueDownstreamRequest{
		RequestID:   fmt.Sprintf("req-%d", time.Now().UnixNano()),
		ClientID:    "go-queues-stream-stream-receive-client",
		RequestType: kubemq.QueueDownstreamGet,
		Channel:     channel,
		MaxItems:    10,
		WaitTimeout: 5000,
		AutoAck:     false,
	})
	if err != nil {
		log.Fatalf("Downstream Send: %v", err)
	}

	// Read received messages.
	var txID string
	timeout := time.After(5 * time.Second)
	for {
		select {
		case msg, ok := <-downstream.Messages:
			if !ok {
				goto done
			}
			if msg != nil && msg.Message != nil {
				txID = msg.TransactionID
				fmt.Printf("Received: body=%s tx=%s\n", msg.Message.Body, msg.TransactionID)
			}
		case <-timeout:
			goto done
		case <-ctx.Done():
			goto done
		}
	}

done:
	// Ack all received messages.
	if txID != "" {
		_ = downstream.Send(&kubemq.QueueDownstreamRequest{
			RequestID:        fmt.Sprintf("req-ack-%d", time.Now().UnixNano()),
			RequestType:      kubemq.QueueDownstreamAckAll,
			RefTransactionID: txID,
		})
		fmt.Println("Messages acknowledged")
	}
}
