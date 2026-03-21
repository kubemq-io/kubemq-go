// Example: queues-stream/nack-all
//
// Demonstrates rejecting all messages in a transaction using NackAll.
// Rejected messages are returned to the queue for reprocessing (up to
// the MaxReceiveCount limit, after which they are discarded).
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

	// Send a message with a receive policy so the server knows how to
	// handle it after rejection (re-deliver up to 3 times).
	_, err = client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("will be nacked")).
		SetMaxReceiveCount(3))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Message sent")

	// Receive the message via downstream receiver.
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
			fmt.Printf("Received: body=%s tx=%s\n", dm.Message.Body, dm.TransactionID)
		}
	}

	// NAckAll: reject all messages in the transaction (return to queue).
	if len(resp.Messages) > 0 {
		fmt.Printf("NAckAll: rejecting all messages from tx=%s\n", resp.Messages[0].TransactionID)
		if err := resp.NackAll(); err != nil {
			log.Printf("NackAll: %v", err)
		}
		fmt.Println("All messages rejected (returned to queue)")
	}
}
