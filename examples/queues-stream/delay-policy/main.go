// Example: queues-stream/delay-policy
//
// Demonstrates sending queue messages with a delay policy via stream.
// Delayed messages are not available for consumption until the delay expires.
//
// Channel: go-queues-stream.delay-policy
// Client ID: go-queues-stream-delay-policy-client
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
		kubemq.WithClientId("go-queues-stream-delay-policy-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues-stream.delay-policy"

	// Send a message with 10-second delay via upstream stream.
	upstream, err := client.QueueUpstream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer upstream.Close()

	delayedMsg := kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("delayed by 10s")).
		SetDelaySeconds(10)

	if err := upstream.Send("req-delay", []*kubemq.QueueMessage{delayedMsg}); err != nil {
		log.Fatal(err)
	}

	select {
	case res := <-upstream.Results:
		if res != nil && res.IsError {
			log.Printf("Error: %s", res.Error)
		} else {
			fmt.Println("Sent message with 10s delay policy")
		}
	case <-time.After(3 * time.Second):
		fmt.Println("Sent message (no result confirmation within timeout)")
	}
}
