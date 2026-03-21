// Example: queues/ack-reject
//
// Demonstrates individual message acknowledgment and rejection using
// the queue downstream receiver. Messages can be individually acked
// (confirmed) or rejected (nacked) back to the queue.
//
// Channel: go-queues.ack-reject
// Client ID: go-queues-ack-reject-client
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
		kubemq.WithClientId("go-queues-ack-reject-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues.ack-reject"

	// Send two messages.
	for i := 1; i <= 2; i++ {
		_, err := client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "msg-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Sent 2 messages")

	// Poll messages with manual acknowledgment (AutoAck=false).
	receiver, err := client.NewQueueDownstreamReceiver(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer receiver.Close()

	resp, err := receiver.Poll(ctx, &kubemq.PollRequest{
		Channel:            channel,
		MaxItems:           10,
		WaitTimeoutSeconds: 5,
		AutoAck:            false,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Polled %d messages\n", len(resp.Messages))
	for _, dm := range resp.Messages {
		if dm.Message != nil {
			fmt.Printf("  body=%s\n", dm.Message.Body)
		}
	}
}
