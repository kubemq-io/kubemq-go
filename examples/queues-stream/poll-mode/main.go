// Example: queues-stream/poll-mode
//
// Demonstrates PollQueue for simple single-shot queue polling.
// PollQueue is a high-level abstraction that handles the receiver lifecycle
// automatically with auto-ack.
//
// Channel: go-queues-stream.poll-mode
// Client ID: go-queues-stream-poll-mode-client
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
		kubemq.WithClientId("go-queues-stream-poll-mode-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues-stream.poll-mode"

	// Send some messages to poll.
	for i := 1; i <= 3; i++ {
		_, err := client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "poll-msg-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Sent 3 messages")

	// PollQueue: single-shot poll with auto-ack.
	pollResp, err := client.PollQueue(ctx, &kubemq.PollRequest{
		Channel:            channel,
		MaxItems:           10,
		WaitTimeoutSeconds: 3,
	})
	if err != nil {
		log.Fatalf("PollQueue: %v", err)
	}
	fmt.Printf("PollQueue: %d messages\n", len(pollResp.Messages))
	for _, dm := range pollResp.Messages {
		if dm.Message != nil {
			fmt.Printf("  body=%s\n", dm.Message.Body)
		}
	}
}
