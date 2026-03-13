// Package main demonstrates Queue Stream features (QueueUpstream, QueueDownstream, PollQueue)
// with the KubeMQ Go SDK v2.
//
// Run with a KubeMQ server on localhost:50000 (e.g. docker run -d -p 50000:50000 kubemq/kubemq).
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
		kubemq.WithClientId("example-queues-stream"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "demo-queue-stream"

	// -------------------------------------------------------------------------
	// 1. QueueUpstream: bidirectional stream for high-throughput publishing
	// -------------------------------------------------------------------------
	upstream, err := client.QueueUpstream(ctx)
	if err != nil {
		log.Fatalf("QueueUpstream: %v", err)
	}
	defer upstream.Close()

	// Send messages via Send(requestID, msgs)
	msgs := []*kubemq.QueueMessage{
		kubemq.NewQueueMessage().SetChannel(channel).SetBody([]byte("stream-msg-1")),
		kubemq.NewQueueMessage().SetChannel(channel).SetBody([]byte("stream-msg-2")),
	}
	if err := upstream.Send("req-up-1", msgs); err != nil {
		log.Fatalf("QueueUpstream Send: %v", err)
	}

	// Read results from Results channel
	select {
	case res := <-upstream.Results:
		if res != nil {
			if res.IsError {
				log.Printf("QueueUpstream result error: %s", res.Error)
			} else {
				fmt.Printf("QueueUpstream: sent %d messages, RefRequestID=%s\n", len(res.Results), res.RefRequestID)
			}
		}
	case <-upstream.Done:
		fmt.Println("QueueUpstream stream done")
	case <-ctx.Done():
		log.Printf("QueueUpstream: %v", ctx.Err())
	}

	// -------------------------------------------------------------------------
	// 2. QueueDownstream: bidirectional stream for receiving and managing messages
	// -------------------------------------------------------------------------
	downstream, err := client.QueueDownstream(ctx)
	if err != nil {
		log.Fatalf("QueueDownstream: %v", err)
	}
	defer downstream.Close()

	// Send a Get request to receive messages
	reqID := fmt.Sprintf("req-down-%d", time.Now().UnixNano())
	err = downstream.Send(&kubemq.QueueDownstreamRequest{
		RequestID:   reqID,
		ClientID:    "example-queues-stream",
		RequestType: kubemq.QueueDownstreamGet,
		Channel:     channel,
		MaxItems:    10,
		WaitTimeout: 5,
		AutoAck:     false,
	})
	if err != nil {
		log.Fatalf("QueueDownstream Send: %v", err)
	}

	// Read from Messages channel (QueueTransactionMessage per message)
	var txID string
	var received []*kubemq.QueueMessage
	done := false
	for !done {
		select {
		case tx, ok := <-downstream.Messages:
			if !ok {
				done = true
				break
			}
			if tx != nil && tx.Message != nil {
				txID = tx.TransactionID
				received = append(received, tx.Message)
				fmt.Printf("QueueDownstream received: channel=%s body=%s tx=%s\n", tx.Message.Channel, tx.Message.Body, tx.TransactionID)
			}
		case err, ok := <-downstream.Errors:
			if ok && err != nil {
				log.Printf("QueueDownstream error: %v", err)
			}
			done = true
		case <-ctx.Done():
			done = true
		default:
			// Non-blocking: if we got at least one message, allow a short drain then exit
			if len(received) > 0 {
				time.Sleep(50 * time.Millisecond)
				done = true
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	// Ack received messages if we have a transaction
	if txID != "" && len(received) > 0 {
		_ = downstream.Send(&kubemq.QueueDownstreamRequest{
			RequestID:        fmt.Sprintf("req-ack-%d", time.Now().UnixNano()),
			RequestType:      kubemq.QueueDownstreamAckAll,
			RefTransactionID: txID,
		})
	}
	fmt.Printf("QueueDownstream: received %d messages\n", len(received))

	// -------------------------------------------------------------------------
	// 3. PollQueue: single-shot poll (high-level abstraction over downstream)
	// -------------------------------------------------------------------------
	pollResp, err := client.PollQueue(ctx, &kubemq.QueuePollRequest{
		Channel:     channel,
		MaxItems:    10,
		WaitTimeout: 3,
		AutoAck:     true,
	})
	if err != nil {
		log.Fatalf("PollQueue: %v", err)
	}
	if pollResp.IsError {
		log.Printf("PollQueue error: %s", pollResp.Error)
	} else {
		fmt.Printf("PollQueue: TransactionID=%s, %d messages\n", pollResp.TransactionID, len(pollResp.Messages))
		for _, m := range pollResp.Messages {
			fmt.Printf("  - body=%s\n", m.Body)
		}
	}
}
