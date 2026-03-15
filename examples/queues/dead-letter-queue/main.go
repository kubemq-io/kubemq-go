// Example: queues/dead-letter-queue
//
// Demonstrates configuring a dead-letter queue (DLQ) for messages that
// exceed the maximum receive count. After the max attempts, messages
// are moved to the specified DLQ channel.
//
// Channel: go-queues.dead-letter-queue
// Client ID: go-queues-dead-letter-queue-client
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
		kubemq.WithClientId("go-queues-dead-letter-queue-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues.dead-letter-queue"
	dlqChannel := channel + ".dlq"

	// Send a message with dead-letter queue configuration.
	// After 3 failed receive attempts, the message is moved to the DLQ.
	dlqMsg := kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("message with DLQ")).
		SetMaxReceiveCount(3).
		SetMaxReceiveQueue(dlqChannel)

	result, err := client.SendQueueMessage(ctx, dlqMsg)
	if err != nil {
		log.Fatal(err)
	}
	if result.IsError {
		log.Fatalf("Send failed: %s", result.Error)
	}
	fmt.Printf("DLQ message sent: id=%s (max receives=3, dlq=%s)\n",
		result.MessageID, dlqChannel)
}
