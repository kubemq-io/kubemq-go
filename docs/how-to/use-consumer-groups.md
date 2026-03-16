# How to Use Consumer Groups

Distribute message processing across multiple subscribers using consumer groups for load-balanced delivery.

## What Consumer Groups Do

When multiple subscribers on the same channel share a **group name**, each message is delivered to exactly **one** member of the group. This provides:

- **Load balancing** — work is distributed across group members
- **Horizontal scaling** — add members to increase throughput
- **Fault tolerance** — if a member goes down, others continue receiving

Without a group, every subscriber receives every message (fan-out). With a group, each message goes to one subscriber (competing consumers).

## Events — Load-Balanced Pub/Sub

Pass a non-empty `group` parameter to `SubscribeToEvents`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-group-demo"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "orders.process"
	group := "order-workers"
	var count atomic.Int32

	// Worker 1
	sub1, err := client.SubscribeToEvents(ctx, channel, group,
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("[Worker-1] %s\n", event.Body)
			count.Add(1)
		}),
		kubemq.WithOnError(func(err error) { log.Println("W1 error:", err) }),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub1.Unsubscribe()

	// Worker 2
	sub2, err := client.SubscribeToEvents(ctx, channel, group,
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("[Worker-2] %s\n", event.Body)
			count.Add(1)
		}),
		kubemq.WithOnError(func(err error) { log.Println("W2 error:", err) }),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub2.Unsubscribe()

	time.Sleep(500 * time.Millisecond)

	for i := 0; i < 6; i++ {
		_ = client.SendEvent(ctx, kubemq.NewEvent().
			SetChannel(channel).
			SetBody([]byte(fmt.Sprintf("order-%d", i+1))))
	}

	time.Sleep(2 * time.Second)
	fmt.Printf("Total processed: %d (each by exactly one worker)\n", count.Load())
}
```

## Events Store — Persistent Consumer Groups

Consumer groups also work with `SubscribeToEventsStore`. Provide a `group` name alongside a start position:

```go
sub, err := client.SubscribeToEventsStore(ctx, "audit.logs", "log-processors",
	kubemq.StartFromFirstEvent(),
	kubemq.WithOnEventStoreReceive(func(e *kubemq.EventStoreReceive) {
		fmt.Printf("[Processor] seq=%d body=%s\n", e.Sequence, string(e.Body))
	}),
	kubemq.WithOnError(func(err error) { log.Println("Error:", err) }),
)
```

## Commands — Load-Balanced RPC Handlers

For commands, each incoming command is routed to exactly one handler in the group:

```go
sub, err := client.SubscribeToCommands(ctx, "tasks.execute", "task-handlers",
	kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
		fmt.Printf("Handling command: %s\n", cmd.Body)
		resp := kubemq.NewResponse().
			SetRequestId(cmd.Id).
			SetResponseTo(cmd.ResponseTo).
			SetExecutedAt(time.Now())
		_ = client.SendResponse(ctx, resp)
	}),
	kubemq.WithOnError(func(err error) { log.Println("Error:", err) }),
)
```

## Queries — Load-Balanced Request/Reply

Queries follow the same pattern. Each query goes to one handler:

```go
sub, err := client.SubscribeToQueries(ctx, "inventory.lookup", "inventory-readers",
	kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
		result := lookupItem(string(q.Body))
		resp := kubemq.NewResponse().
			SetRequestId(q.Id).
			SetResponseTo(q.ResponseTo).
			SetBody(result).
			SetExecutedAt(time.Now())
		_ = client.SendResponse(ctx, resp)
	}),
	kubemq.WithOnError(func(err error) { log.Println("Error:", err) }),
)
```

## Group Naming Conventions

| Pattern | Example | When to use |
|---------|---------|-------------|
| `{service}-workers` | `order-workers` | General processing pools |
| `{service}-{region}` | `order-us-east` | Regional processing |
| `{service}-{priority}` | `order-high` | Priority-based routing |

Group names are scoped per channel. Two groups with the same name on different channels are independent.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Every subscriber gets every message | Group name is empty string `""` | Pass a non-empty group name |
| Messages go to only one subscriber | Intended behavior for consumer groups | This is correct — use `""` group for fan-out |
| Uneven message distribution | Normal with small message counts | Distribution evens out over larger volumes |
| Subscriber not receiving messages | It joined a different group name | Verify all members use the exact same group string |
