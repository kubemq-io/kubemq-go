// Example: queues-stream/stream-send
//
// Demonstrates high-throughput queue message publishing using QueueUpstream.
// The bidirectional stream allows sending multiple messages efficiently
// with per-batch result confirmations.
//
// Channel: go-queues-stream.stream-send
// Client ID: go-queues-stream-stream-send-client
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
		kubemq.WithClientId("go-queues-stream-stream-send-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queues-stream.stream-send"

	// Open a bidirectional upstream stream for publishing.
	upstream, err := client.QueueUpstream(ctx)
	if err != nil {
		log.Fatalf("QueueUpstream: %v", err)
	}
	defer upstream.Close()

	// Send a batch of messages via the stream.
	msgs := []*kubemq.QueueMessage{
		kubemq.NewQueueMessage().SetChannel(channel).SetBody([]byte("stream-msg-1")),
		kubemq.NewQueueMessage().SetChannel(channel).SetBody([]byte("stream-msg-2")),
		kubemq.NewQueueMessage().SetChannel(channel).SetBody([]byte("stream-msg-3")),
	}
	if err := upstream.Send("req-batch-1", msgs); err != nil {
		log.Fatalf("Send: %v", err)
	}

	// Read the batch result.
	select {
	case res := <-upstream.Results:
		if res != nil {
			if res.IsError {
				log.Printf("Upstream error: %s", res.Error)
			} else {
				fmt.Printf("Stream sent %d messages, RefRequestID=%s\n",
					len(res.Results), res.RefRequestID)
			}
		}
	case <-upstream.Done:
		fmt.Println("Upstream stream done")
	case <-ctx.Done():
		log.Printf("Timed out: %v", ctx.Err())
	}
}
