# Building a Task Queue

This tutorial shows you how to build a reliable task queue with KubeMQ. Unlike fire-and-forget events, queues guarantee that every message is delivered exactly once and processed with explicit acknowledgment. By the end, you'll know how to send tasks, consume them, handle failures, and route poison messages to a dead-letter queue.

## What You'll Build

A task processing pipeline where:
1. A producer enqueues work items
2. A consumer pulls and processes them with manual acknowledgment
3. Failed messages are rejected back to the queue for retry
4. Messages that fail too many times land in a dead-letter queue for investigation

## Prerequisites

- **Go 1.21 or later** ([download](https://go.dev/dl/))
- **KubeMQ server** running on `localhost:50000`:
  ```bash
  docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq
  ```
- A new Go module:
  ```bash
  mkdir kubemq-queue-tutorial && cd kubemq-queue-tutorial
  go mod init kubemq-queue-tutorial
  go get github.com/kubemq-io/kubemq-go/v2
  ```

## Step 1: Send Messages to a Queue

Queues differ from events in a fundamental way: messages persist until a consumer explicitly acknowledges them. This makes queues the right choice for tasks that *must* be processed — order fulfillment, payment processing, email dispatch, and so on.

Create `main.go`:

```go
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
		kubemq.WithClientId("task-queue-tutorial"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "tutorial.tasks"

	// Enqueue three tasks.
	tasks := []string{"resize-image-001", "send-email-042", "generate-report-7"}
	for _, task := range tasks {
		result, err := client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
			SetChannel(channel).
			SetBody([]byte(task)).
			SetMetadata("task"))
		if err != nil {
			log.Fatal(err)
		}
		if result.IsError {
			log.Fatalf("Send failed: %s", result.Error)
		}
		fmt.Printf("Enqueued: %s (id=%s)\n", task, result.MessageID)
	}
}
```

**What's happening:**

- `NewQueueMessage()` creates a message builder. Every queue message needs a channel and a body. Metadata and tags are optional but useful for routing and debugging.
- `SendQueueMessage` delivers the message to the server and returns a `SendResult` containing the assigned message ID. The `IsError` flag tells you whether the server accepted the message.
- Unlike events, this message now sits in the queue waiting for a consumer. It won't disappear until someone acknowledges it.

## Step 2: Receive and Process Messages

Now let's consume those tasks. Replace the contents of `main.go` with the following complete program that sends *and* receives:

```go
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
		kubemq.WithClientId("task-queue-tutorial"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "tutorial.tasks"

	// --- Producer: enqueue tasks ---
	tasks := []string{"resize-image-001", "send-email-042", "generate-report-7"}
	for _, task := range tasks {
		result, err := client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
			SetChannel(channel).
			SetBody([]byte(task)).
			SetMetadata("task"))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("[Producer] Enqueued: %s (id=%s)\n", task, result.MessageID)
	}

	// --- Consumer: pull and process ---
	resp, err := client.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
		Channel:             channel,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     5,
	})
	if err != nil {
		log.Fatal(err)
	}
	if resp.IsError {
		log.Fatalf("Receive failed: %s", resp.Error)
	}

	fmt.Printf("\n[Consumer] Received %d messages:\n", resp.MessagesReceived)
	for _, msg := range resp.Messages {
		fmt.Printf("  Task: %s (metadata=%s)\n", msg.Body, msg.Metadata)
	}
}
```

**Why this matters:**

- `ReceiveQueueMessages` is a pull-based API. You ask for up to N messages and wait up to M seconds. This gives you control over batch size and processing pace — critical for backpressure management.
- The `WaitTimeSeconds` parameter implements long polling: the server holds the connection open until messages arrive or the timeout expires. This is more efficient than repeatedly polling.

### Expected Output

```
[Producer] Enqueued: resize-image-001 (id=...)
[Producer] Enqueued: send-email-042 (id=...)
[Producer] Enqueued: generate-report-7 (id=...)

[Consumer] Received 3 messages:
  Task: resize-image-001 (metadata=task)
  Task: send-email-042 (metadata=task)
  Task: generate-report-7 (metadata=task)
```

## Step 3: Handle Acknowledgment and Rejection

In production, you need to tell the queue whether processing succeeded (ack) or failed (reject/nack). Without explicit acknowledgment, the message stays invisible but isn't removed — it will eventually reappear for another attempt.

Use `PollQueue` with `AutoAck: false` for manual control:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("task-queue-ack-tutorial"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "tutorial.tasks.ack"

	// Enqueue tasks — some will "fail" processing.
	tasks := []string{"valid-task-1", "bad-task-crash", "valid-task-2"}
	for _, task := range tasks {
		_, err := client.SendQueueMessage(ctx, kubemq.NewQueueMessage().
			SetChannel(channel).
			SetBody([]byte(task)))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("[Producer] Sent 3 tasks\n")

	// Poll with manual ack.
	pollResp, err := client.PollQueue(ctx, &kubemq.QueuePollRequest{
		Channel:     channel,
		MaxItems:    10,
		WaitTimeout: 5000,
		AutoAck:     false,
	})
	if err != nil {
		log.Fatal(err)
	}
	if pollResp.IsError {
		log.Fatalf("Poll failed: %s", pollResp.Error)
	}

	fmt.Printf("[Consumer] Polled %d messages\n", len(pollResp.Messages))
	for _, msg := range pollResp.Messages {
		task := string(msg.Body)

		if strings.Contains(task, "bad") {
			// Simulate a processing failure — reject the message.
			fmt.Printf("  REJECT: %s (will return to queue)\n", task)
		} else {
			// Processing succeeded.
			fmt.Printf("  ACK:    %s (removed from queue)\n", task)
		}
	}
}
```

**The ack/reject decision:**

- **Ack** (acknowledge): "I processed this successfully." The message is permanently removed from the queue.
- **Reject** (negative acknowledge): "Processing failed." The message goes back to the queue so another consumer (or the same one later) can retry it.

This is the foundation of reliable message processing. Every message gets exactly one chance per pull, and your code decides the outcome.

## Step 4: Add Dead-Letter Queue Handling

Rejecting messages endlessly creates an infinite retry loop. Dead-letter queues (DLQs) solve this by catching messages that have been rejected too many times. You set a max receive count, and KubeMQ automatically routes the message to a DLQ channel after that threshold is crossed.

```go
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
		kubemq.WithClientId("task-queue-dlq-tutorial"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "tutorial.tasks.dlq"
	dlqChannel := "tutorial.tasks.dlq.dead-letters"

	// Send a message with DLQ policy: after 3 failed receives, move to DLQ.
	msg := kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("fragile-task-99")).
		SetMaxReceiveCount(3).
		SetMaxReceiveQueue(dlqChannel)

	result, err := client.SendQueueMessage(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[Producer] Sent task with DLQ policy (id=%s)\n", result.MessageID)
	fmt.Printf("  Max receives: 3\n")
	fmt.Printf("  DLQ channel:  %s\n\n", dlqChannel)

	// Simulate 3 failed processing attempts by receiving and rejecting.
	for attempt := 1; attempt <= 3; attempt++ {
		resp, err := client.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
			Channel:             channel,
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     3,
		})
		if err != nil {
			log.Fatal(err)
		}
		if resp.MessagesReceived == 0 {
			fmt.Printf("[Consumer] Attempt %d: No message (moved to DLQ)\n", attempt)
			break
		}
		fmt.Printf("[Consumer] Attempt %d: Received '%s' — rejecting\n",
			attempt, resp.Messages[0].Body)
	}

	// Check the DLQ — the message should be there now.
	fmt.Println("\n[DLQ Monitor] Checking dead-letter queue...")
	dlqResp, err := client.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
		Channel:             dlqChannel,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     3,
	})
	if err != nil {
		log.Fatal(err)
	}
	if dlqResp.MessagesReceived > 0 {
		for _, m := range dlqResp.Messages {
			fmt.Printf("  Dead letter: %s\n", m.Body)
		}
	} else {
		fmt.Println("  No messages in DLQ yet")
	}
}
```

**Why dead-letter queues matter:**

- Without a DLQ, a poison message (one that always fails processing) blocks your entire pipeline. Every consumer that pulls it will fail, reject it, and the cycle repeats.
- `SetMaxReceiveCount(3)` tells KubeMQ: "If this message is received 3 times without being acknowledged, stop retrying."
- `SetMaxReceiveQueue(dlqChannel)` tells KubeMQ where to send the message after the limit is reached. You can then monitor the DLQ channel separately for manual investigation or alerting.

### Expected Output

```
[Producer] Sent task with DLQ policy (id=...)
  Max receives: 3
  DLQ channel:  tutorial.tasks.dlq.dead-letters

[Consumer] Attempt 1: Received 'fragile-task-99' — rejecting
[Consumer] Attempt 2: Received 'fragile-task-99' — rejecting
[Consumer] Attempt 3: No message (moved to DLQ)

[DLQ Monitor] Checking dead-letter queue...
  Dead letter: fragile-task-99
```

## Key Concepts

| Concept | What It Means |
|---|---|
| **At-least-once delivery** | Every message is delivered at least once. Ack removes it; reject returns it for retry. |
| **Pull-based consumption** | Consumers control the pace — no messages are pushed until requested. |
| **Long polling** | `WaitTimeSeconds` avoids busy-waiting by holding the connection open until messages arrive. |
| **Dead-letter queue** | A safety net for messages that repeatedly fail processing. Prevents poison messages from blocking the pipeline. |
| **Batch operations** | `SendQueueMessages` (plural) sends multiple messages in a single round trip for higher throughput. |

## Next Steps

- **Batch sending**: Send many messages in one call for high-throughput producers. See [`examples/queues/batch-send/`](../../examples/queues/batch-send/).
- **Delayed messages**: Schedule messages to become visible after a delay. See [`examples/queues/delayed-messages/`](../../examples/queues/delayed-messages/).
- **Stream mode**: Use `QueueUpstream` for continuous high-throughput sending. See [`examples/queues-stream/stream-send/`](../../examples/queues-stream/stream-send/).
- **Request-reply**: If your tasks need to send results back, see [Implementing Request-Reply with Commands](request-reply-with-commands.md).
