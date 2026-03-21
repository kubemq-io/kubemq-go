// Example: queues-stream/requeue-all
//
// Demonstrates moving all messages from one queue to another using ReQueueAll.
// This is useful for routing messages to different processing pipelines.
//
// Channel: go-queues-stream.requeue-all
// Client ID: go-queues-stream-requeue-all-client
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
		kubemq.WithClientId("go-queues-stream-requeue-all-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	srcChannel := "go-queues-stream.requeue-all"
	dstChannel := "go-queues-stream.requeue-all.dest"

	// Send a message to the source queue with a receive policy.
	_, err = client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
		SetChannel(srcChannel).
		SetBody([]byte("will be requeued")).
		SetMaxReceiveCount(3))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Message sent to source queue")

	// Receive from source queue via downstream receiver.
	receiver, err := client.NewQueueDownstreamReceiver(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer receiver.Close()

	resp, err := receiver.Poll(ctx, &kubemq.PollRequest{
		Channel:            srcChannel,
		MaxItems:           10,
		WaitTimeoutSeconds: 5,
		AutoAck:            false,
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, dm := range resp.Messages {
		if dm.Message != nil {
			fmt.Printf("Received: body=%s tx=%s\n", dm.Message.Body, dm.TransactionID)
		}
	}

	// ReQueueAll: move all messages to the destination queue.
	if len(resp.Messages) > 0 {
		fmt.Printf("ReQueueAll: moving messages to %s\n", dstChannel)
		if err := resp.ReQueueAll(dstChannel); err != nil {
			log.Printf("ReQueueAll: %v", err)
		}
		fmt.Println("Messages requeued to destination")
	}
}
