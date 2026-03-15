// Example: queues-stream/visibility-timeout
//
// Demonstrates the visibility timeout pattern with queue streams.
// Messages are received without auto-ack, giving a window to process
// and explicitly ack or reject them before they become visible again.
//
// Channel: go-queues-stream.visibility-timeout
// Client ID: go-queues-stream-visibility-timeout-client
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
		kubemq.WithClientId("go-queues-stream-visibility-timeout-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues-stream.visibility-timeout"

	// Send a message.
	_, err = client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("process me within timeout")))
	if err != nil {
		log.Fatal(err)
	}

	// Receive with manual ack (AutoAck=false) to control visibility.
	downstream, err := client.QueueDownstream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer downstream.Close()

	err = downstream.Send(&kubemq.QueueDownstreamRequest{
		RequestID:   fmt.Sprintf("req-%d", time.Now().UnixNano()),
		RequestType: kubemq.QueueDownstreamGet,
		Channel:     channel,
		MaxItems:    1,
		WaitTimeout: 5000,
		AutoAck:     false, // Manual ack — message is invisible to other consumers
	})
	if err != nil {
		log.Fatal(err)
	}

	// Process the message within the visibility window.
	select {
	case msg, ok := <-downstream.Messages:
		if ok && msg != nil && msg.Message != nil {
			fmt.Printf("Processing: body=%s (tx=%s)\n", msg.Message.Body, msg.TransactionID)

			// Simulate processing, then ack the message.
			time.Sleep(500 * time.Millisecond)

			_ = downstream.Send(&kubemq.QueueDownstreamRequest{
				RequestID:        "req-ack-visibility",
				RequestType:      kubemq.QueueDownstreamAckAll,
				RefTransactionID: msg.TransactionID,
			})
			fmt.Println("Message acknowledged within visibility timeout")
		}
	case <-time.After(5 * time.Second):
		fmt.Println("No messages received")
	}
}
