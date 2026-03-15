// Example: queues-stream/nack-all
//
// Demonstrates rejecting all messages in a transaction using NAckAll.
// Rejected messages are returned to the queue for reprocessing.
//
// Channel: go-queues-stream.nack-all
// Client ID: go-queues-stream-nack-all-client
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
		kubemq.WithClientId("go-queues-stream-nack-all-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues-stream.nack-all"

	// Send a message.
	_, err = client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("will be nacked")))
	if err != nil {
		log.Fatal(err)
	}

	// Receive the message.
	downstream, err := client.QueueDownstream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer downstream.Close()

	err = downstream.Send(&kubemq.QueueDownstreamRequest{
		RequestID:   fmt.Sprintf("req-get-%d", time.Now().UnixNano()),
		RequestType: kubemq.QueueDownstreamGet,
		Channel:     channel,
		MaxItems:    10,
		WaitTimeout: 5000,
		AutoAck:     false,
	})
	if err != nil {
		log.Fatal(err)
	}

	var txID string
	select {
	case msg, ok := <-downstream.Messages:
		if ok && msg != nil {
			txID = msg.TransactionID
			fmt.Printf("Received: body=%s tx=%s\n", msg.Message.Body, msg.TransactionID)
		}
	case <-time.After(5 * time.Second):
		log.Fatal("No messages received")
	}

	// NAckAll: reject all messages in the transaction (return to queue).
	if txID != "" {
		fmt.Printf("NAckAll: rejecting all messages from tx=%s\n", txID)
		_ = downstream.Send(&kubemq.QueueDownstreamRequest{
			RequestID:        "req-nack-all",
			RequestType:      kubemq.QueueDownstreamNAckAll,
			RefTransactionID: txID,
		})
		fmt.Println("All messages rejected (returned to queue)")
	}
}
