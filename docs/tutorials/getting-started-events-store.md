# Getting Started with Events Store

This tutorial walks you through building a persistent pub/sub system with KubeMQ events store in Go. By the end, you'll have a working publisher and subscriber exchanging durable messages — and the ability to replay events from any point in the stream.

## What You'll Build

A persistent pub/sub system where messages are stored on the server and can be replayed. Unlike regular events (fire-and-forget), events store messages survive restarts and support multiple start positions: from the beginning, from the last event, from a specific sequence number, or only new events. This makes them ideal for audit logs, event sourcing, and any scenario where you need durability and replay.

## Prerequisites

- **Go 1.21 or later** installed ([download](https://go.dev/dl/))
- **KubeMQ server** running on `localhost:50000`. The quickest way:
  ```bash
  docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq
  ```
- A new Go module for your project:
  ```bash
  mkdir kubemq-events-store-tutorial && cd kubemq-events-store-tutorial
  go mod init kubemq-events-store-tutorial
  ```

## Step 1: Install the SDK

Pull the KubeMQ Go SDK into your module:

```bash
go get github.com/kubemq-io/kubemq-go/v2
```

This single dependency gives you access to events, events store, queues, commands, and queries — all over a single gRPC connection.

## Step 2: Create a Subscriber with Start Position

Create a file called `main.go`. We'll start with the subscriber because it needs to be listening *before* events are published — otherwise you'll miss them unless you use a replay position.

With events store, you choose *where* to start receiving:

- **`kubemq.StartFromNewEvents()`** — only events published after you subscribe (like regular events, but stored)
- **`kubemq.StartFromFirstEvent()`** — replay all stored events from the beginning, then continue with new ones
- **`kubemq.StartFromLastEvent()`** — replay the last stored event, then continue with new ones
- **`kubemq.StartFromSequence(n)`** — replay from sequence number `n` onward (`n` must be > 0)
- **`kubemq.StartFromTime(t)`** — replay events stored at or after time `t`
- **`kubemq.StartFromTimeDelta(d)`** — replay events from now minus duration `d`

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

	// Connect to the KubeMQ server.
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-store-tutorial-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "tutorial.events-store"
	received := make(chan struct{})

	// Subscribe to events store with StartFromNewEvents — only new events.
	sub, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromNewEvents(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[Subscriber] Received event:\n")
			fmt.Printf("  Sequence: %d\n", e.Sequence)
			fmt.Printf("  Channel:  %s\n", e.Channel)
			fmt.Printf("  Metadata: %s\n", e.Metadata)
			fmt.Printf("  Body:     %s\n", e.Body)
			close(received)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("[Subscriber] Error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	fmt.Println("[Subscriber] Listening on channel:", channel)

	// ... publisher code will go here (Step 3) ...
}
```

Let's break down the key parts:

- **`SubscribeToEventsStore`** takes the channel name, an optional consumer group (empty string means no group), a start position option, and callbacks. The event store callback receives `*kubemq.EventStoreReceive`, which includes a `Sequence` field for ordering and replay.
- **`kubemq.StartFromNewEvents()`** means we only receive events published after we subscribe. Use `StartFromFirstEvent()` to replay everything, or `StartFromSequence(n)` to resume from a known position.
- **`sub.Unsubscribe()`** cleans up the subscription when you're done. Pairing it with `defer` ensures it always runs.

## Step 3: Publish Persistent Events

Now add the publishing code right after the subscription block. Use `SendEventStore` instead of `SendEvent` — events are stored on the server and can be replayed:

```go
	// Allow the subscription to register with the server.
	time.Sleep(time.Second)

	// Publish a persistent event to the store.
	result, err := client.SendEventStore(ctx, kubemq.NewEventStore().
		SetChannel(channel).
		SetBody([]byte("Hello from the KubeMQ Events Store!")).
		SetMetadata("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[Publisher] Event stored: id=%s sent=%v\n", result.Id, result.Sent)

	// Wait for the subscriber to receive the event.
	select {
	case <-received:
		fmt.Println("\nSuccess! Event was stored and received.")
	case <-ctx.Done():
		log.Fatal("Timed out waiting for event")
	}
```

A few things to notice:

- **`kubemq.NewEventStore()`** creates an event store builder. You set the channel, body (as `[]byte`), and optional metadata — same pattern as `NewEvent()`.
- **`SendEventStore`** returns a result with `Id` and `Sent`. The event is persisted on the server and assigned a sequence number.
- The `select` block waits either for the subscriber to close the `received` channel (meaning it got the event) or for the context timeout to expire.

## Step 4: Replay from Sequence

To demonstrate replay, you can subscribe with `StartFromSequence(n)` to receive events starting at a specific sequence number. This is useful when you've processed up to sequence 42 and want to resume from 43:

```go
	// Subscribe starting at sequence 3 — replays events from seq 3 onward.
	sub, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromSequence(3),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[Replay] seq=%d body=%s\n", e.Sequence, string(e.Body))
			// ...
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("[Subscriber] Error:", err)
		}),
	)
```

The sequence number is per channel. Each stored event gets an incrementing sequence. Use `StartFromSequence(lastSeq + 1)` to resume processing after a restart.

## Complete Program

Here's the complete program assembled:

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
		kubemq.WithClientId("events-store-tutorial-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "tutorial.events-store"
	received := make(chan struct{})

	sub, err := client.SubscribeToEventsStore(ctx, channel, "",
		kubemq.StartFromNewEvents(),
		kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
			fmt.Printf("[Subscriber] Received event:\n")
			fmt.Printf("  Sequence: %d\n", e.Sequence)
			fmt.Printf("  Channel:  %s\n", e.Channel)
			fmt.Printf("  Metadata: %s\n", e.Metadata)
			fmt.Printf("  Body:     %s\n", e.Body)
			close(received)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("[Subscriber] Error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	fmt.Println("[Subscriber] Listening on channel:", channel)

	time.Sleep(time.Second)

	result, err := client.SendEventStore(ctx, kubemq.NewEventStore().
		SetChannel(channel).
		SetBody([]byte("Hello from the KubeMQ Events Store!")).
		SetMetadata("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[Publisher] Event stored: id=%s sent=%v\n", result.Id, result.Sent)

	select {
	case <-received:
		fmt.Println("\nSuccess! Event was stored and received.")
	case <-ctx.Done():
		log.Fatal("Timed out waiting for event")
	}
}
```

Run it:

```bash
go run main.go
```

### Expected Output

```
[Subscriber] Listening on channel: tutorial.events-store
[Publisher] Event stored: id=<message-id> sent=true
[Subscriber] Received event:
  Sequence: 1
  Channel:  tutorial.events-store
  Metadata: greeting
  Body:     Hello from the KubeMQ Events Store!

Success! Event was stored and received.
```

## Key Concepts

| Concept | What It Means |
|---|---|
| **Persistence** | Events are stored on the server. They survive restarts and can be replayed. |
| **Replay** | You choose where to start: from the first event, last event, a sequence number, or only new events. |
| **Sequence numbers** | Each stored event gets an incrementing sequence per channel. Use them to resume processing. |
| **Start positions** | `StartFromNewEvents`, `StartFromFirstEvent`, `StartFromLastEvent`, `StartFromSequence(n)`, `StartFromTime(t)`, `StartFromTimeDelta(d)` control replay behavior. |

## Next Steps

- **Replay from sequence**: Use `StartFromSequence(lastSeq + 1)` to resume after a crash. See [`examples/events-store/replay-from-sequence/`](../../examples/events-store/replay-from-sequence/).
- **Replay from the beginning**: Use `StartFromFirstEvent()` to process all historical events. See [`examples/events-store/start-from-first/`](../../examples/events-store/start-from-first/).
- **Consumer groups**: Add consumer groups for load-balanced processing of stored events. See [`examples/events-store/consumer-group/`](../../examples/events-store/consumer-group/).
- **Regular events**: If you don't need persistence, see [Getting Started with Events](getting-started-events.md).
