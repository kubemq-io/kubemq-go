// Example: queues-stream/ack-range
//
// Demonstrates selectively acknowledging specific messages by sequence
// number using AckRange. This allows fine-grained control over which
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

	// Receive messages via downstream.
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
	var seqs []int64
	timeout := time.After(10 * time.Second)
	for {
		select {
		case msg, ok := <-downstream.Messages:
			if !ok {
				goto ack
			}
			if msg != nil {
				txID = msg.TransactionID
				if msg.Message != nil && msg.Message.Attributes != nil {
					seqs = append(seqs, int64(msg.Message.Attributes.Sequence))
				}
				fmt.Printf("Received: body=%s seq=%d\n", msg.Message.Body,
					func() uint64 {
						if msg.Message.Attributes != nil {
							return msg.Message.Attributes.Sequence
						}
						return 0
					}())
			}
		case <-timeout:
			goto ack
		}
	}

ack:
	// Selectively ack only the first message's sequence.
	if txID != "" && len(seqs) > 0 {
		fmt.Printf("AckRange: acking sequence %v from tx=%s\n", seqs[:1], txID)
		_ = downstream.Send(&kubemq.QueueDownstreamRequest{
			RequestID:        "req-ack-range",
			RequestType:      kubemq.QueueDownstreamAckRange,
			RefTransactionID: txID,
			SequenceRange:    seqs[:1],
		})
		fmt.Println("Selective ack complete")
	}
}
