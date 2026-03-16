# Getting Started with Events

This tutorial walks you through building your first real-time pub/sub system with KubeMQ events in Go. By the end, you'll have a working publisher and subscriber exchanging messages — and a solid understanding of how each piece fits together.

## What You'll Build

A simple event-driven system where one goroutine subscribes to a channel and another publishes a message to it. Events in KubeMQ are **fire-and-forget**: the publisher sends a message and moves on. This makes them ideal for telemetry, logs, notifications, and any scenario where delivery acknowledgment isn't required.

## Prerequisites

- **Go 1.21 or later** installed ([download](https://go.dev/dl/))
- **KubeMQ server** running on `localhost:50000`. The quickest way:
  ```bash
  docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq
  ```
- A new Go module for your project:
  ```bash
  mkdir kubemq-events-tutorial && cd kubemq-events-tutorial
  go mod init kubemq-events-tutorial
  ```

## Step 1: Install the SDK

Pull the KubeMQ Go SDK into your module:

```bash
go get github.com/kubemq-io/kubemq-go/v2
```

This single dependency gives you access to events, queues, commands, and queries — all over a single gRPC connection.

## Step 2: Create a Subscriber

Create a file called `main.go`. We'll start with the subscriber because it needs to be listening *before* events are published — otherwise the event has no one to deliver to.

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
		kubemq.WithClientId("events-tutorial-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "tutorial.events"
	received := make(chan struct{})

	// Subscribe to events on the channel.
	sub, err := client.SubscribeToEvents(ctx, channel, "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("[Subscriber] Received event:\n")
			fmt.Printf("  Channel:  %s\n", event.Channel)
			fmt.Printf("  Metadata: %s\n", event.Metadata)
			fmt.Printf("  Body:     %s\n", event.Body)
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

- **`kubemq.NewClient`** establishes a gRPC connection to the server. The `ClientId` identifies this client in the KubeMQ dashboard and logs — pick something meaningful.
- **`SubscribeToEvents`** takes the channel name, an optional consumer group (empty string means no group), and two callbacks. The event callback fires for every incoming event; the error callback fires if the subscription encounters a problem.
- **`sub.Unsubscribe()`** cleans up the subscription when you're done. Pairing it with `defer` ensures it always runs.
- The `received` channel is a simple synchronization mechanism so our program waits for the event before exiting.

## Step 3: Create a Publisher

Now add the publishing code right after the subscription block. The subscriber needs a moment to register with the server, so we add a brief sleep before publishing:

```go
	// Give the subscription time to register with the server.
	time.Sleep(time.Second)

	// Publish an event to the channel.
	err = client.SendEvent(ctx, kubemq.NewEvent().
		SetChannel(channel).
		SetBody([]byte("Hello from the KubeMQ Go SDK!")).
		SetMetadata("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("[Publisher] Event sent to channel:", channel)

	// Wait for the subscriber to receive the event.
	select {
	case <-received:
		fmt.Println("\nSuccess! Event was published and received.")
	case <-ctx.Done():
		log.Fatal("Timed out waiting for event")
	}
```

A few things to notice:

- **`kubemq.NewEvent()`** creates an event builder. You set the channel, body (as `[]byte`), and optional metadata. This fluent API makes the intent clear.
- **`SendEvent`** is non-blocking from the publisher's perspective — it fires the event to the server and returns. There's no delivery receipt because events are fire-and-forget by design.
- The `select` block waits either for the subscriber to close the `received` channel (meaning it got the event) or for the context timeout to expire.

## Step 4: Run and See Output

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
		kubemq.WithClientId("events-tutorial-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "tutorial.events"
	received := make(chan struct{})

	sub, err := client.SubscribeToEvents(ctx, channel, "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("[Subscriber] Received event:\n")
			fmt.Printf("  Channel:  %s\n", event.Channel)
			fmt.Printf("  Metadata: %s\n", event.Metadata)
			fmt.Printf("  Body:     %s\n", event.Body)
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

	err = client.SendEvent(ctx, kubemq.NewEvent().
		SetChannel(channel).
		SetBody([]byte("Hello from the KubeMQ Go SDK!")).
		SetMetadata("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("[Publisher] Event sent to channel:", channel)

	select {
	case <-received:
		fmt.Println("\nSuccess! Event was published and received.")
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
[Subscriber] Listening on channel: tutorial.events
[Publisher] Event sent to channel: tutorial.events
[Subscriber] Received event:
  Channel:  tutorial.events
  Metadata: greeting
  Body:     Hello from the KubeMQ Go SDK!

Success! Event was published and received.
```

## Key Concepts

| Concept | What It Means |
|---|---|
| **Fire-and-forget** | The publisher doesn't wait for subscribers to confirm receipt. Fast, but no delivery guarantee. |
| **Channel** | A named topic that publishers send to and subscribers listen on. Any string works. |
| **Consumer group** | When multiple subscribers use the same group name, each event goes to only one of them (load balancing). Leave empty for broadcast. |
| **Client ID** | Identifies this connection in server logs and the dashboard. Use descriptive names. |

## Next Steps

- **Persistent events**: Switch to `SubscribeToEventsStore` / `SendEventStore` to get durable, replayable events that survive restarts. See [`examples/events-store/`](../../examples/events-store/).
- **Multiple subscribers**: Add consumer groups for load-balanced processing. See [`examples/events/consumer-group/`](../../examples/events/consumer-group/).
- **Wildcard subscriptions**: Subscribe to `tutorial.*` to catch events on any sub-channel. See [`examples/events/wildcard-subscription/`](../../examples/events/wildcard-subscription/).
- **Queues**: If you need guaranteed delivery with acknowledgment, see [Building a Task Queue](building-a-task-queue.md).
