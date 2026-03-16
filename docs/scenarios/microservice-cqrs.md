# Implementing CQRS with KubeMQ

This guide demonstrates a Command Query Responsibility Segregation (CQRS) architecture using KubeMQ's native messaging primitives: commands for writes, queries for reads, and events for state synchronization.

## Architecture

```
┌────────┐  command   ┌──────────────┐  event   ┌──────────────┐
│ Client │───────────▶│ Write Service │────────▶│ Read Service │
│        │            │ (cmd handler) │          │ (projection) │
│        │◀───────────│              │          │              │
│        │  query     └──────────────┘          └──────────────┘
│        │────────────────────────────────────▶│              │
│        │◀───────────────────────────────────│              │
└────────┘                                     └──────────────┘
```

1. **Commands** carry write intent — "create order", "update status". The write service processes commands and emits domain events.
2. **Events** propagate state changes to read-side projections asynchronously.
3. **Queries** retrieve data from the read-optimized projection, independent of the write model.

## Prerequisites

- KubeMQ server on `localhost:50000`
- `go get github.com/kubemq-io/kubemq-go/v2`

## Write Service — Command Handler

The write service subscribes to commands, validates them, applies business logic, and publishes domain events.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "sync"
    "time"

    "github.com/kubemq-io/kubemq-go/v2"
)

var (
    orders = make(map[string]string) // write-side store
    mu     sync.RWMutex
)

func startWriteService(ctx context.Context, client *kubemq.Client) {
    sub, err := client.SubscribeToCommands(ctx, "cqrs.commands", "",
        kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
            fmt.Printf("[Write] Command received: %s\n", cmd.Body)

            mu.Lock()
            orders[cmd.Metadata] = string(cmd.Body)
            mu.Unlock()

            // Acknowledge the command
            _ = client.SendResponse(ctx, kubemq.NewResponse().
                SetRequestId(cmd.Id).
                SetResponseTo(cmd.ResponseTo).
                SetExecutedAt(time.Now()))

            // Publish domain event for read-side sync
            _ = client.SendEvent(ctx, kubemq.NewEvent().
                SetChannel("cqrs.events").
                SetBody(cmd.Body).
                SetMetadata(cmd.Metadata))

            fmt.Printf("[Write] Order %s persisted and event emitted\n", cmd.Metadata)
        }),
        kubemq.WithOnError(func(err error) {
            log.Printf("[Write] Error: %v", err)
        }),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer sub.Unsubscribe()
    <-ctx.Done()
}
```

## Read Service — Query Handler with Event Projection

The read service maintains a denormalized projection updated by domain events, and serves queries against it.

```go
var (
    projection = make(map[string]string) // read-side projection
    projMu     sync.RWMutex
)

func startReadService(ctx context.Context, client *kubemq.Client) {
    // Subscribe to domain events to keep projection up to date
    eventSub, err := client.SubscribeToEvents(ctx, "cqrs.events", "",
        kubemq.WithOnEvent(func(event *kubemq.Event) {
            projMu.Lock()
            projection[event.Metadata] = string(event.Body)
            projMu.Unlock()
            fmt.Printf("[Read] Projection updated: key=%s\n", event.Metadata)
        }),
        kubemq.WithOnError(func(err error) {
            log.Printf("[Read] Event error: %v", err)
        }),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer eventSub.Unsubscribe()

    // Subscribe to queries to serve read requests
    querySub, err := client.SubscribeToQueries(ctx, "cqrs.queries", "",
        kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
            key := string(q.Body)
            projMu.RLock()
            val, ok := projection[key]
            projMu.RUnlock()

            body := []byte(fmt.Sprintf(`{"found":%t,"data":"%s"}`, ok, val))

            _ = client.SendResponse(ctx, kubemq.NewResponse().
                SetRequestId(q.Id).
                SetResponseTo(q.ResponseTo).
                SetBody(body).
                SetExecutedAt(time.Now()))

            fmt.Printf("[Read] Query served: key=%s found=%t\n", key, ok)
        }),
        kubemq.WithOnError(func(err error) {
            log.Printf("[Read] Query error: %v", err)
        }),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer querySub.Unsubscribe()
    <-ctx.Done()
}
```

## Client — Sending Commands and Queries

```go
func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    client, err := kubemq.NewClient(ctx,
        kubemq.WithAddress("localhost", 50000),
        kubemq.WithClientId("cqrs-demo"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    go startWriteService(ctx, client)
    go startReadService(ctx, client)
    time.Sleep(time.Second)

    // Write via command
    _, err = client.SendCommand(ctx, kubemq.NewCommand().
        SetChannel("cqrs.commands").
        SetBody([]byte(`{"item":"widget","qty":5}`)).
        SetMetadata("ORD-001").
        SetTimeout(10*time.Second))
    if err != nil {
        log.Fatal(err)
    }

    time.Sleep(500 * time.Millisecond)

    // Read via query
    resp, err := client.SendQuery(ctx, kubemq.NewQuery().
        SetChannel("cqrs.queries").
        SetBody([]byte("ORD-001")).
        SetTimeout(10*time.Second))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("[Client] Query result: %s\n", resp.Body)
}
```

## Design Considerations

| Concern | Approach |
|---|---|
| **Consistency** | Eventually consistent — events propagate asynchronously to the read model |
| **Ordering** | Use events-store with sequence replay if strict ordering matters |
| **Durability** | Commands are request-reply; the write service persists before acking |
| **Scaling** | Read and write services scale independently via consumer groups |
| **Failure** | If the read service misses events, replay from events-store |

## When to Use This Pattern

- Systems where read and write workloads have different scaling requirements
- Domain models that benefit from separate write validation and read optimization
- Microservices that need event-driven state synchronization across bounded contexts
