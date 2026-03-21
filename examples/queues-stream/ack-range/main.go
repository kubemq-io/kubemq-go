// Example: queues-stream/ack-range
//
// Demonstrates selectively acknowledging specific messages using individual
// message Ack/Nack methods. This allows fine-grained control over which
// messages in a transaction are acknowledged.
//
// Channel: go-queues-stream.ack-range
// Client ID: go-queues-stream-ack-range-client
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
		kubemq.WithClientId("go-queues-stream-ack-range-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues-stream.ack-range"

	// Send multiple messages via upstream stream.
	upstream, err := client.QueueUpstream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer upstream.Close()

	for i := range 3 {
		msg := kubemq.NewQueueMessage().
			SetChannel(channel).
			SetBody(fmt.Appendf(nil, "msg-%d", i))
		if err := upstream.Send(fmt.Sprintf("req-%d", i), []*kubemq.QueueMessage{msg}); err != nil {
			log.Fatal(err)
		}
		// Drain result
		select {
		case <-upstream.Results:
		case <-time.After(3 * time.Second):
		}
	}
	fmt.Println("Sent 3 messages")

	// Allow messages to be committed to the queue.
	time.Sleep(time.Second)

	// Receive messages via downstream receiver.
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

	for _, dm := range resp.Messages {
		if dm.Message != nil {
			fmt.Printf("Received: body=%s seq=%d\n", dm.Message.Body, dm.Sequence)
		}
	}

	// Selectively ack only the first message.
	if len(resp.Messages) > 0 {
		dm := resp.Messages[0]
		fmt.Printf("Ack: acking sequence %d from tx=%s\n", dm.Sequence, dm.TransactionID)
		if err := dm.Ack(); err != nil {
			log.Printf("Ack failed: %v", err)
		}
		fmt.Println("Selective ack complete")
	}
}
