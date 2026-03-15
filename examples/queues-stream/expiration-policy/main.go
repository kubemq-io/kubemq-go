// Example: queues-stream/expiration-policy
//
// Demonstrates sending queue messages with an expiration (TTL) policy.
// Messages that are not consumed before the expiration time are automatically
// removed from the queue.
//
// Channel: go-queues-stream.expiration-policy
// Client ID: go-queues-stream-expiration-policy-client
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
		kubemq.WithClientId("go-queues-stream-expiration-policy-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues-stream.expiration-policy"

	// Send a message with 60-second expiration via upstream stream.
	upstream, err := client.QueueUpstream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer upstream.Close()

	expiringMsg := kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("expires in 60s")).
		SetExpirationSeconds(60)

	if err := upstream.Send("req-expiration", []*kubemq.QueueMessage{expiringMsg}); err != nil {
		log.Fatal(err)
	}

	// Drain the result.
	select {
	case res := <-upstream.Results:
		if res != nil && res.IsError {
			log.Printf("Error: %s", res.Error)
		} else {
			fmt.Println("Sent message with 60s expiration policy")
		}
	case <-time.After(3 * time.Second):
		fmt.Println("Sent message (no result confirmation within timeout)")
	}
}
