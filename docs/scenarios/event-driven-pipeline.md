# Building an Event-Driven Processing Pipeline

This guide walks through a multi-stage processing pipeline that combines KubeMQ events for real-time fan-out with queues for reliable, exactly-once delivery.

## Architecture

```
┌──────────┐    events     ┌─────────────┐    queue     ┌──────────┐
│ Producer │──────────────▶│  Processor  │─────────────▶│  Output  │
│ (ingest) │  (fan-out)    │  (transform)│  (reliable)  │ (persist)│
└──────────┘               └─────────────┘              └──────────┘
```

1. **Producer** publishes raw data as events on a pub/sub channel.
2. **Processor** subscribes to events, transforms each payload, and enqueues results into a queue.
3. **Output** worker pulls from the queue with ack/nack semantics to guarantee delivery.

This separation lets you scale each stage independently. Events handle real-time fan-out while queues provide backpressure and delivery guarantees.

## Prerequisites

- KubeMQ server on `localhost:50000`
- `go get github.com/kubemq-io/kubemq-go/v2`

## Stage 1 — Event Producer

The producer ingests raw data and publishes it as events. Multiple subscribers can receive each event.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/kubemq-io/kubemq-go/v2"
)

func runProducer(ctx context.Context, client *kubemq.Client) {
    orders := []string{
        `{"id":"ORD-1","item":"widget","qty":5}`,
        `{"id":"ORD-2","item":"gadget","qty":2}`,
        `{"id":"ORD-3","item":"gizmo","qty":10}`,
    }
    for _, order := range orders {
        err := client.SendEvent(ctx, kubemq.NewEvent().
            SetChannel("pipeline.ingest").
            SetBody([]byte(order)).
            SetMetadata("source:api"))
        if err != nil {
            log.Printf("send error: %v", err)
            continue
        }
        fmt.Printf("[Producer] Published: %s\n", order)
    }
}
```

## Stage 2 — Event Processor

The processor subscribes to events, transforms payloads, and enqueues enriched results for reliable downstream consumption.

```go
func runProcessor(ctx context.Context, client *kubemq.Client) {
    sub, err := client.SubscribeToEvents(ctx, "pipeline.ingest", "",
        kubemq.WithOnEvent(func(event *kubemq.Event) {
            fmt.Printf("[Processor] Received event: %s\n", event.Body)

            enriched := fmt.Sprintf(`{"original":%s,"processed_at":"%s"}`,
                event.Body, time.Now().Format(time.RFC3339))

            msg := kubemq.NewQueueMessage().
                SetChannel("pipeline.output").
                SetBody([]byte(enriched))

            result, qErr := client.SendQueueMessage(ctx, msg)
            if qErr != nil || result.IsError {
                log.Printf("[Processor] Queue send failed: %v %s", qErr, result.Error)
                return
            }
            fmt.Printf("[Processor] Enqueued for output: %s\n", result.MessageID)
        }),
        kubemq.WithOnError(func(err error) {
            log.Printf("[Processor] Subscription error: %v", err)
        }),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer sub.Unsubscribe()

    <-ctx.Done()
}
```

## Stage 3 — Output Worker

The output worker pulls from the queue with exactly-once semantics. Failed messages remain on the queue for retry.

```go
func runOutputWorker(ctx context.Context, client *kubemq.Client) {
    resp, err := client.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
        Channel:             "pipeline.output",
        MaxNumberOfMessages: 10,
        WaitTimeSeconds:     5,
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("[Output] Received %d messages:\n", resp.MessagesReceived)
    for _, m := range resp.Messages {
        fmt.Printf("  → %s\n", m.Body)
    }
}
```

## Putting It Together

```go
func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    client, err := kubemq.NewClient(ctx,
        kubemq.WithAddress("localhost", 50000),
        kubemq.WithClientId("pipeline-demo"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    go runProcessor(ctx, client)
    time.Sleep(500 * time.Millisecond)

    runProducer(ctx, client)
    time.Sleep(2 * time.Second)

    runOutputWorker(ctx, client)
}
```

## Error Handling

- **Producer failures**: Log and skip; events are fire-and-forget by design.
- **Processor failures**: If the queue send fails, the event is lost. Use events-store instead of events if you need replay capability.
- **Output failures**: Messages stay in the queue. Use dead-letter queues for messages that fail repeatedly.

## When to Use This Pattern

- Stream processing with decoupled stages
- Ingestion pipelines where throughput matters more than ordering
- Systems that need both real-time notifications and guaranteed delivery
