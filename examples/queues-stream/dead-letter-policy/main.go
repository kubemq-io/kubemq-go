// Example: queues-stream/dead-letter-policy
//
// Demonstrates sending queue messages with a dead-letter queue policy
// via the upstream stream. After exceeding the max receive count,
// messages are moved to the specified dead-letter queue.
//
// Channel: go-queues-stream.dead-letter-policy
// Client ID: go-queues-stream-dead-letter-policy-client
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
		kubemq.WithClientId("go-queues-stream-dead-letter-policy-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues-stream.dead-letter-policy"
	dlqChannel := "go-queues-stream.dead-letter-policy.dlq"

	// Send a message with dead-letter queue policy via upstream stream.
	upstream, err := client.QueueUpstream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer upstream.Close()

	dlqMsg := kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("max 3 receives, then DLQ")).
		SetMaxReceiveCount(3).
		SetMaxReceiveQueue(dlqChannel)

	if err := upstream.Send("req-dlq", []*kubemq.QueueMessage{dlqMsg}); err != nil {
		log.Fatal(err)
	}

	select {
	case res := <-upstream.Results:
		if res != nil && res.IsError {
			log.Printf("Error: %s", res.Error)
		} else {
			fmt.Printf("Sent message with DLQ policy (max 3 receives, dlq=%s)\n", dlqChannel)
		}
	case <-time.After(3 * time.Second):
		fmt.Println("Sent message (no result confirmation within timeout)")
	}
}
