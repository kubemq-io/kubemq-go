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

	// Receive from source queue via downstream stream.
	downstream, err := client.QueueDownstream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer downstream.Close()

	err = downstream.Send(&kubemq.QueueDownstreamRequest{
		RequestID:   fmt.Sprintf("req-get-%d", time.Now().UnixNano()),
		ClientID:    "go-queues-stream-requeue-all-client",
		RequestType: kubemq.QueueDownstreamGet,
		Channel:     srcChannel,
		MaxItems:    10,
		WaitTimeout: 5000,
		AutoAck:     false,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Wait longer than WaitTimeout so the server has time to respond.
	var txID string
	select {
	case msg, ok := <-downstream.Messages:
		if ok && msg != nil {
			txID = msg.TransactionID
			fmt.Printf("Received: body=%s tx=%s\n", msg.Message.Body, msg.TransactionID)
		}
	case <-time.After(10 * time.Second):
		log.Fatal("No messages received")
	}

	// ReQueueAll: move all messages to the destination queue.
	if txID != "" {
		fmt.Printf("ReQueueAll: moving messages from tx=%s to %s\n", txID, dstChannel)
		_ = downstream.Send(&kubemq.QueueDownstreamRequest{
			RequestID:        "req-requeue-all",
			RequestType:      kubemq.QueueDownstreamReQueueAll,
			RefTransactionID: txID,
			ReQueueChannel:   dstChannel,
		})
		fmt.Println("Messages requeued to destination")
	}
}
