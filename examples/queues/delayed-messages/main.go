// Example: queues/delayed-messages
//
// Demonstrates sending a delayed queue message. The message becomes
// available for consumption only after the specified delay period.
//
// Channel: go-queues.delayed-messages
// Client ID: go-queues-delayed-messages-client
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
		kubemq.WithClientId("go-queues-delayed-messages-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues.delayed-messages"

	// Send a message with a 3-second delay.
	delayedMsg := kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("delayed message")).
		SetDelaySeconds(3)

	result, err := client.SendQueueMessage(ctx, delayedMsg)
	if err != nil {
		log.Fatal(err)
	}
	if result.IsError {
		log.Fatalf("Send failed: %s", result.Error)
	}
	fmt.Printf("Delayed message sent: id=%s delayedTo=%d\n",
		result.MessageID, result.DelayedTo)
	fmt.Println("Message will be available after 3 seconds")
}
