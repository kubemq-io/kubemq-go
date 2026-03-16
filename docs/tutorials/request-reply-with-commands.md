# Implementing Request-Reply with Commands

This tutorial shows you how to build synchronous request-reply interactions using KubeMQ commands. Unlike events (fire-and-forget) and queues (async processing), commands give you a direct response from the handler — similar to an RPC call. By the end, you'll have a working command handler, a client that sends commands, and robust timeout and error handling.

## What You'll Build

A request-reply system where:
1. A responder subscribes to a command channel and processes incoming requests
2. A client sends a command and blocks until the response arrives
3. Timeouts prevent the client from hanging if the responder is unavailable
4. Errors are handled gracefully on both sides

## Prerequisites

- **Go 1.21 or later** ([download](https://go.dev/dl/))
- **KubeMQ server** running on `localhost:50000`:
  ```bash
  docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq
  ```
- A new Go module:
  ```bash
  mkdir kubemq-commands-tutorial && cd kubemq-commands-tutorial
  go mod init kubemq-commands-tutorial
  go get github.com/kubemq-io/kubemq-go/v2
  ```

## When to Use Commands vs. Events vs. Queues

Choosing the right messaging pattern matters:

| Pattern | Use When |
|---|---|
| **Events** | You want to broadcast information and don't need confirmation (logs, metrics, notifications). |
| **Queues** | Work must be processed reliably but the producer doesn't need an immediate answer (background jobs). |
| **Commands** | You need a synchronous response — the caller waits for the result before continuing (API gateways, orchestration, validation). |

Commands are blocking by nature. The sender waits until the handler responds or the timeout expires. This makes them perfect for operations where the caller needs to know the outcome right away.

## Step 1: Create a Command Handler (Responder)

The responder listens on a channel and processes incoming commands. Think of it as a remote function that other services can call.

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
		kubemq.WithClientId("commands-tutorial-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "tutorial.commands"
	done := make(chan struct{})

	// Subscribe to commands on the channel.
	sub, err := client.SubscribeToCommands(ctx, channel, "",
		kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
			fmt.Printf("[Handler] Received command:\n")
			fmt.Printf("  ID:       %s\n", cmd.Id)
			fmt.Printf("  Channel:  %s\n", cmd.Channel)
			fmt.Printf("  Body:     %s\n", cmd.Body)
			fmt.Printf("  Metadata: %s\n", cmd.Metadata)

			// Process the command and send a response.
			resp := kubemq.NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetBody([]byte("order confirmed")).
				SetExecutedAt(time.Now())

			if err := client.SendResponse(ctx, resp); err != nil {
				log.Printf("[Handler] Failed to send response: %v", err)
			} else {
				fmt.Println("[Handler] Response sent")
			}
			close(done)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("[Handler] Subscription error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	fmt.Println("[Handler] Listening for commands on:", channel)

	// ... sender code will go here (Step 2) ...
}
```

**How the handler works:**

- `SubscribeToCommands` registers a callback that fires every time a command arrives on the channel. The second argument is an optional consumer group — leave it empty for a single handler.
- Each `CommandReceive` contains the command's `Id`, `ResponseTo` address, `Body`, and `Metadata`. The `Id` and `ResponseTo` fields are critical — they tell KubeMQ where to route the response.
- `NewResponse()` builds the reply. You *must* set `RequestId` and `ResponseTo` from the received command, or the sender will never get the response. `SetExecutedAt` records when the handler finished processing.
- The response body can contain any data you want to send back to the caller.

## Step 2: Send Commands from Client

Add the sender code after the subscription block:

```go
	// Give the subscription time to establish.
	time.Sleep(time.Second)

	// Send a command and wait for the response.
	fmt.Println("\n[Client] Sending command...")
	cmdResp, err := client.SendCommand(ctx, kubemq.NewCommand().
		SetChannel(channel).
		SetBody([]byte("process-order-123")).
		SetMetadata("priority:high").
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatalf("[Client] Command failed: %v", err)
	}

	fmt.Printf("[Client] Response received:\n")
	fmt.Printf("  Executed: %v\n", cmdResp.Executed)
	fmt.Printf("  At:       %v\n", cmdResp.ExecutedAt)

	<-done
```

**What's happening:**

- `NewCommand()` builds the command with a channel, body, metadata, and a **timeout**. The timeout is essential — it defines how long the client will wait for a response before giving up.
- `SendCommand` is a **blocking call**. It sends the command to the server, which routes it to a handler, and then waits for the response. This is the "request-reply" pattern in action.
- The returned `CommandResponse` contains `Executed` (bool indicating success) and `ExecutedAt` (timestamp from the handler).

## Step 3: Handle Timeouts

What happens when no handler is available? The command times out. Your code should handle this gracefully rather than crashing.

Here's a standalone example that demonstrates timeout behavior:

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
		kubemq.WithClientId("commands-timeout-tutorial"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Send a command to a channel with NO handler — this will time out.
	fmt.Println("[Client] Sending command with 3-second timeout (no handler exists)...")
	start := time.Now()

	_, err = client.SendCommand(ctx, kubemq.NewCommand().
		SetChannel("tutorial.commands.nobody-home").
		SetBody([]byte("hello?")).
		SetTimeout(3*time.Second))

	elapsed := time.Since(start)

	if err != nil {
		fmt.Printf("[Client] Command timed out after %v: %v\n", elapsed.Round(time.Millisecond), err)
	} else {
		fmt.Println("[Client] Unexpected success — a handler must exist")
	}
}
```

**Why timeouts matter:**

- Without a timeout, a missing handler would cause the sender to block indefinitely (or until the context expires). Explicit timeouts make your system predictable.
- Choose timeout values based on your expected processing time. A fast lookup might use 2 seconds; a complex computation might need 30.
- The error returned on timeout is a standard Go `error` that you can inspect, log, or use to trigger fallback logic.

### Expected Output

```
[Client] Sending command with 3-second timeout (no handler exists)...
[Client] Command timed out after 3.012s: ...
```

## Step 4: Add Error Handling

Let's put it all together in a production-ready example with comprehensive error handling on both the handler and sender sides:

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
		kubemq.WithClientId("commands-error-tutorial"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "tutorial.commands.robust"
	done := make(chan struct{})

	// Handler with error-aware processing logic.
	sub, err := client.SubscribeToCommands(ctx, channel, "",
		kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
			body := string(cmd.Body)
			fmt.Printf("[Handler] Processing: %s\n", body)

			resp := kubemq.NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo)

			// Simulate business logic that can fail.
			if strings.Contains(body, "invalid") {
				resp.SetBody([]byte("validation failed: invalid order ID"))
			} else {
				resp.SetExecutedAt(time.Now()).
					SetBody([]byte("order processed successfully"))
			}

			if err := client.SendResponse(ctx, resp); err != nil {
				log.Printf("[Handler] Response send failed: %v", err)
			}
		}),
		kubemq.WithOnError(func(err error) {
			log.Printf("[Handler] Subscription error: %v", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()
	time.Sleep(time.Second)

	// Send multiple commands to exercise both success and failure paths.
	commands := []struct {
		body    string
		timeout time.Duration
	}{
		{"process-order-100", 10 * time.Second},
		{"invalid-order-xyz", 10 * time.Second},
	}

	for _, c := range commands {
		fmt.Printf("\n[Client] Sending: %s\n", c.body)
		resp, err := client.SendCommand(ctx, kubemq.NewCommand().
			SetChannel(channel).
			SetBody([]byte(c.body)).
			SetTimeout(c.timeout))
		if err != nil {
			fmt.Printf("[Client] Error: %v\n", err)
			continue
		}
		fmt.Printf("[Client] Executed: %v, Body: %s\n",
			resp.Executed, resp.Body)
	}

	close(done)
	<-done
}
```

### Expected Output

```
[Handler] Processing: process-order-100

[Client] Sending: process-order-100
[Client] Executed: true, Body: order processed successfully

[Client] Sending: invalid-order-xyz
[Handler] Processing: invalid-order-xyz
[Client] Executed: false, Body: validation failed: invalid order ID
```

## Key Concepts

| Concept | What It Means |
|---|---|
| **Request-reply** | The sender blocks until the handler responds, like a function call across services. |
| **Timeout** | A hard limit on how long the sender waits. Always set one. |
| **RequestId / ResponseTo** | These fields link the response back to the original command. Always copy them from the received command. |
| **ExecutedAt** | A handler-set timestamp indicating when processing completed. Useful for latency tracking. |
| **Consumer group** | Multiple handlers on the same group name: each command goes to only one handler (load balancing). |

## Next Steps

- **Consumer groups**: Distribute command handling across multiple instances for load balancing. See [`examples/commands/consumer-group/`](../../examples/commands/consumer-group/).
- **Queries**: If you need to return data (not just success/failure), use queries instead. See [`examples/queries/send-query/`](../../examples/queries/send-query/).
- **Events**: For one-way notifications that don't need a response, see [Getting Started with Events](getting-started-events.md).
- **Queues**: For reliable async task processing, see [Building a Task Queue](building-a-task-queue.md).
