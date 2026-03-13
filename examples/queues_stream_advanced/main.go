// Package main demonstrates advanced Queue Stream features with the KubeMQ Go SDK v2.
//
// This example covers: message policies (expiration, delay, dead-letter queue),
// and advanced downstream operations (AckRange, NAckAll, ReQueueAll).
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
		kubemq.WithClientId("example-qs-advanced"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// -------------------------------------------------------------------------
	// 1. Send messages with expiration policy (TTL)
	// -------------------------------------------------------------------------
	upstream, err := client.QueueUpstream(ctx)
	if err != nil {
		log.Fatal("QueueUpstream:", err)
	}
	defer upstream.Close()

	expiringMsg := kubemq.NewQueueMessage().
		SetChannel("demo-expiring").
		SetBody([]byte("expires in 60s")).
		SetExpirationSeconds(60)

	if err := upstream.Send("req-expiration", []*kubemq.QueueMessage{expiringMsg}); err != nil {
		log.Fatal("Send expiring:", err)
	}
	drainUpstreamResult(upstream)
	fmt.Println("Sent message with 60s expiration")

	// -------------------------------------------------------------------------
	// 2. Send messages with delay policy
	// -------------------------------------------------------------------------
	delayedMsg := kubemq.NewQueueMessage().
		SetChannel("demo-delayed").
		SetBody([]byte("delayed by 10s")).
		SetDelaySeconds(10)

	if err := upstream.Send("req-delay", []*kubemq.QueueMessage{delayedMsg}); err != nil {
		log.Fatal("Send delayed:", err)
	}
	drainUpstreamResult(upstream)
	fmt.Println("Sent message with 10s delay")

	// -------------------------------------------------------------------------
	// 3. Send messages with dead-letter queue policy
	// -------------------------------------------------------------------------
	dlqMsg := kubemq.NewQueueMessage().
		SetChannel("demo-dlq-source").
		SetBody([]byte("max 3 receives, then DLQ")).
		SetMaxReceiveCount(3).
		SetMaxReceiveQueue("demo-dlq-target")

	if err := upstream.Send("req-dlq", []*kubemq.QueueMessage{dlqMsg}); err != nil {
		log.Fatal("Send DLQ:", err)
	}
	drainUpstreamResult(upstream)
	fmt.Println("Sent message with dead-letter queue policy (max 3 receives)")

	// -------------------------------------------------------------------------
	// 4. AckRange — selectively acknowledge specific messages by sequence
	// -------------------------------------------------------------------------
	channel := "demo-ack-range"
	for i := 0; i < 3; i++ {
		msg := kubemq.NewQueueMessage().
			SetChannel(channel).
			SetBody([]byte(fmt.Sprintf("msg-%d", i)))
		if err := upstream.Send(fmt.Sprintf("req-send-%d", i), []*kubemq.QueueMessage{msg}); err != nil {
			log.Fatal(err)
		}
		drainUpstreamResult(upstream)
	}

	downstream, err := client.QueueDownstream(ctx)
	if err != nil {
		log.Fatal("QueueDownstream:", err)
	}
	defer downstream.Close()

	txID, seqs := getMessages(ctx, downstream, channel)
	if txID != "" && len(seqs) > 0 {
		fmt.Printf("AckRange: acking sequences %v from tx=%s\n", seqs[:1], txID)
		_ = downstream.Send(&kubemq.QueueDownstreamRequest{
			RequestID:        "req-ack-range",
			RequestType:      kubemq.QueueDownstreamAckRange,
			RefTransactionID: txID,
			SequenceRange:    seqs[:1],
		})
	}

	// -------------------------------------------------------------------------
	// 5. NAckAll — reject all messages (return to queue)
	// -------------------------------------------------------------------------
	nackChannel := "demo-nack"
	msg := kubemq.NewQueueMessage().SetChannel(nackChannel).SetBody([]byte("will be nacked"))
	if err := upstream.Send("req-nack-send", []*kubemq.QueueMessage{msg}); err != nil {
		log.Fatal(err)
	}
	drainUpstreamResult(upstream)

	downstream2, err := client.QueueDownstream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer downstream2.Close()

	txID2, _ := getMessages(ctx, downstream2, nackChannel)
	if txID2 != "" {
		fmt.Printf("NAckAll: rejecting all messages from tx=%s\n", txID2)
		_ = downstream2.Send(&kubemq.QueueDownstreamRequest{
			RequestID:        "req-nack-all",
			RequestType:      kubemq.QueueDownstreamNAckAll,
			RefTransactionID: txID2,
		})
	}

	// -------------------------------------------------------------------------
	// 6. ReQueueAll — move all messages to another queue
	// -------------------------------------------------------------------------
	requeueChannel := "demo-requeue-src"
	msg2 := kubemq.NewQueueMessage().SetChannel(requeueChannel).SetBody([]byte("will be requeued"))
	if err := upstream.Send("req-requeue-send", []*kubemq.QueueMessage{msg2}); err != nil {
		log.Fatal(err)
	}
	drainUpstreamResult(upstream)

	downstream3, err := client.QueueDownstream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer downstream3.Close()

	txID3, _ := getMessages(ctx, downstream3, requeueChannel)
	if txID3 != "" {
		fmt.Printf("ReQueueAll: moving messages from tx=%s to demo-requeue-dst\n", txID3)
		_ = downstream3.Send(&kubemq.QueueDownstreamRequest{
			RequestID:        "req-requeue-all",
			RequestType:      kubemq.QueueDownstreamReQueueAll,
			RefTransactionID: txID3,
			ReQueueChannel:   "demo-requeue-dst",
		})
	}

	fmt.Println("Advanced queue stream example complete.")
}

func drainUpstreamResult(up *kubemq.QueueUpstreamHandle) {
	select {
	case res := <-up.Results:
		if res != nil && res.IsError {
			log.Printf("Upstream error: %s", res.Error)
		}
	case <-time.After(3 * time.Second):
	}
}

func getMessages(ctx context.Context, ds *kubemq.QueueDownstreamHandle, channel string) (string, []int64) {
	reqID := fmt.Sprintf("req-get-%d", time.Now().UnixNano())
	err := ds.Send(&kubemq.QueueDownstreamRequest{
		RequestID:   reqID,
		RequestType: kubemq.QueueDownstreamGet,
		Channel:     channel,
		MaxItems:    10,
		WaitTimeout: 3000,
		AutoAck:     false,
	})
	if err != nil {
		log.Printf("Downstream Send: %v", err)
		return "", nil
	}

	var txID string
	var seqs []int64
	timeout := time.After(5 * time.Second)
	for {
		select {
		case msg, ok := <-ds.Messages:
			if !ok {
				return txID, seqs
			}
			if msg != nil {
				txID = msg.TransactionID
				if msg.Message != nil && msg.Message.Attributes != nil {
					seqs = append(seqs, int64(msg.Message.Attributes.Sequence))
				}
				fmt.Printf("  downstream: body=%s tx=%s\n", msg.Message.Body, msg.TransactionID)
			}
		case <-timeout:
			return txID, seqs
		case <-ctx.Done():
			return txID, seqs
		}
	}
}
