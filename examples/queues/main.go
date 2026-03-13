// Package main demonstrates Queue Simple API with the KubeMQ Go SDK v2.
//
// This example covers: single send, batch send, receive (pull), peek, and ack all.
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
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "demo-queue"

	// -------------------------------------------------------------------------
	// 1. Send a single queue message
	// -------------------------------------------------------------------------
	msg := kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("single message")).
		SetMetadata("greeting")

	result, err := client.SendQueueMessage(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}
	if result.IsError {
		log.Fatalf("Send failed: %s", result.Error)
	}
	fmt.Printf("Single send: id=%s\n", result.MessageID)

	// -------------------------------------------------------------------------
	// 2. Send batch queue messages
	// -------------------------------------------------------------------------
	batch := []*kubemq.QueueMessage{
		kubemq.NewQueueMessage().SetChannel(channel).SetBody([]byte("batch-msg-1")),
		kubemq.NewQueueMessage().SetChannel(channel).SetBody([]byte("batch-msg-2")),
		kubemq.NewQueueMessage().SetChannel(channel).SetBody([]byte("batch-msg-3")),
	}
	batchResults, err := client.SendQueueMessages(ctx, batch)
	if err != nil {
		log.Fatal(err)
	}
	for i, r := range batchResults {
		fmt.Printf("Batch[%d]: id=%s error=%v\n", i, r.MessageID, r.IsError)
	}

	// -------------------------------------------------------------------------
	// 3. Peek queue messages (view without consuming)
	// -------------------------------------------------------------------------
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

	// -------------------------------------------------------------------------
	// 4. Receive queue messages (consume)
	// -------------------------------------------------------------------------
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
		fmt.Printf("  body=%s\n", m.Body)
	}

	// -------------------------------------------------------------------------
	// 5. Ack all queue messages
	// -------------------------------------------------------------------------
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
