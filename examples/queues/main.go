// Package main demonstrates Queue send and receive with the KubeMQ Go SDK v2.
//
// This example sends a queue message, receives it, and acknowledges. Run with a
// KubeMQ server on localhost:50000 (e.g., docker run -d -p 50000:50000 kubemq/kubemq).
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

	// Create a KubeMQ client
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "demo-queue"

	// Send a queue message
	msg := kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("hello from v2 queue")).
		SetMetadata("greeting")

	result, err := client.SendQueueMessage(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}
	if result != nil && result.IsError {
		log.Fatalf("Send failed: %s", result.Error)
	}
	fmt.Printf("Message sent: id=%s\n", result.MessageID)

	// Receive queue messages
	resp, err := client.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
		Channel:             channel,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     10,
		IsPeak:              false,
	})
	if err != nil {
		log.Fatal(err)
	}
	if resp.IsError {
		log.Fatalf("Receive failed: %s", resp.Error)
	}
	if len(resp.Messages) == 0 {
		log.Fatal("No messages received (timeout or empty queue)")
	}

	// Process and print the received message
	for _, m := range resp.Messages {
		fmt.Printf("Received: channel=%s body=%s\n", m.Channel, m.Body)
	}

	// Acknowledge all received messages
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
