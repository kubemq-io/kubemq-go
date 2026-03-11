# Migrating from v1 to v2

This guide covers all breaking changes in the KubeMQ Go SDK v2 and provides
step-by-step instructions for upgrading your application.

## Overview of Changes

v2 is a major release with comprehensive improvements to error handling,
connection management, security, observability, and code architecture.
Key themes:

- **Structured errors** — All errors are now `*kubemq.KubeMQError` with
  machine-readable codes and retryable classification
- **Production-safe defaults** — Auto-reconnect with exponential backoff
  enabled by default
- **Modern dependencies** — OpenTelemetry replaces OpenCensus;
  `google.golang.org/protobuf` replaces `gogo/protobuf`
- **Clean architecture** — Internal packages hide implementation details;
  no proto type leaks

## Step-by-Step Upgrade

### Step 1: Update the Module Path

```bash
# Remove v1
go mod edit -droprequire github.com/kubemq-io/kubemq-go

# Add v2
go get github.com/kubemq-io/kubemq-go/v2
```

Update all import statements:

```go
// v1
import "github.com/kubemq-io/kubemq-go"

// v2
import "github.com/kubemq-io/kubemq-go/v2"
```

### Step 2: Update Client Construction

Use the unified `kubemq.NewClient` with functional options. See the breaking
changes table below for each specific change.

### Step 3: Update Error Handling

Replace string-based error checks with `errors.As` and `KubeMQError` inspection.
See BC-2 and BC-3 below.

### Step 4: Update Subscriptions

Replace channel-based error delivery with `WithOnError` callback. See BC-12 below.

### Step 5: Verify and Test

```bash
go build ./...
go test ./...
```

## Breaking Changes Reference

### BC-1: Module Path Change

| Aspect | v1 | v2 |
|--------|----|----|
| Import path | `github.com/kubemq-io/kubemq-go` | `github.com/kubemq-io/kubemq-go/v2` |

```go
// v1
import "github.com/kubemq-io/kubemq-go"

// v2
import "github.com/kubemq-io/kubemq-go/v2"
```

### BC-2: Error Types Change

| Aspect | v1 | v2 |
|--------|----|----|
| Error type | Plain `error` (string) | `*kubemq.KubeMQError` with structured fields |
| Error matching | String comparison | `errors.As` / `errors.Is` |

```go
// v1
err := client.SendEvent(ctx, event)
if err != nil {
    if strings.Contains(err.Error(), "timeout") {
        // handle timeout
    }
}

// v2
err := client.SendEvent(ctx, event)
if err != nil {
    var ke *kubemq.KubeMQError
    if errors.As(err, &ke) {
        switch ke.Code {
        case kubemq.ErrCodeTimeout:
            // handle timeout — ke.IsRetryable tells you if retry is safe
        case kubemq.ErrCodeAuthentication:
            // handle auth failure
        }
    }
}
```

### BC-3: Sentinel Errors Removed

| Aspect | v1 | v2 |
|--------|----|----|
| `ErrNoTransportDefined` | Sentinel var | Removed — replaced by `*KubeMQError` with `ErrCodeValidation` |
| `ErrNoTransportConnection` | Sentinel var | Removed — replaced by `*KubeMQError` with `ErrCodeTransient` |

```go
// v1
if err == kubemq.ErrNoTransportConnection {
    // handle
}

// v2
var ke *kubemq.KubeMQError
if errors.As(err, &ke) && ke.Code == kubemq.ErrCodeTransient {
    // handle connection failure
}
```

### BC-4: Default Send Timeout Added

| Aspect | v1 | v2 |
|--------|----|----|
| Events publish timeout | None (blocks indefinitely) | 5s default |
| RPC timeout | 5s (via `Timeout` field) | 10s (via context) |

```go
// v1 — no timeout, could block forever
err := client.Send(ctx, event)

// v2 — 5s default timeout applied automatically
// To override, use context:
ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
defer cancel()
err := client.SendEvent(ctx, event)
```

### BC-5: Auto-Reconnect Default Changed

| Aspect | v1 | v2 |
|--------|----|----|
| `autoReconnect` default | `false` | Always enabled (controlled by `ReconnectPolicy`) |
| Reconnection backoff | Fixed interval | Exponential backoff with jitter |

```go
// v1 — must opt-in to reconnection
client, _ := kubemq.NewClient(ctx,
    kubemq.WithAddress("localhost", 50000),
    kubemq.WithAutoReconnect(true),
    kubemq.WithReconnectInterval(5 * time.Second),
    kubemq.WithMaxReconnects(10),
)

// v2 — reconnection is always on with sensible defaults
// To customize:
client, _ := kubemq.NewClient(ctx,
    kubemq.WithAddress("localhost", 50000),
    kubemq.WithReconnectPolicy(kubemq.ReconnectPolicy{
        MaxAttempts:  10,
        InitialDelay: 1 * time.Second,
        MaxDelay:     30 * time.Second,
        Multiplier:   2.0,
        JitterMode:   kubemq.JitterFull,
        BufferSize:   1000,
    }),
)
```

**Removed options:** `WithAutoReconnect()`, `WithReconnectInterval()`, `WithMaxReconnects()` — all replaced by `WithReconnectPolicy()`.

### BC-6: REST/WebSocket Transport Removed

| Aspect | v1 | v2 |
|--------|----|----|
| Transports available | gRPC, REST, WebSocket | gRPC only |
| `TransportType` enum | `TransportTypeGRPC`, `TransportTypeRest` | Removed |

```go
// v1
client, _ := kubemq.NewClient(ctx,
    kubemq.WithAddress("localhost", 50000),
    kubemq.WithTransportType(kubemq.TransportTypeGRPC),
    kubemq.WithUri("http://localhost:9090"),
)

// v2 — gRPC is the only transport, no selection needed
client, _ := kubemq.NewClient(ctx,
    kubemq.WithAddress("localhost", 50000),
)
```

### BC-7: gogo/protobuf Replaced

| Aspect | v1 | v2 |
|--------|----|----|
| Protobuf runtime | `github.com/gogo/protobuf` | `google.golang.org/protobuf` |
| Proto type exposure | `QueueMessage` embeds `*pb.QueueMessage` | SDK types only; no proto leaks |

```go
// v1 — accessing proto fields directly
msg := client.NewQueueMessage()
msg.MessageID = "123"         // proto field
msg.Policy = &pb.QueueMessagePolicy{...}  // proto type

// v2 — SDK-provided builder methods
msg := kubemq.NewQueueMessage().
    SetChannel("tasks").
    SetBody([]byte("payload")).
    SetPolicyDelaySeconds(30).
    SetPolicyMaxReceiveCount(3)
```

### BC-8: QueueMessage No Longer Embeds Proto

| Aspect | v1 | v2 |
|--------|----|----|
| `QueueMessage` type | Embeds `*pb.QueueMessage` | Pure SDK struct with accessor methods |

```go
// v1 — accessing embedded proto directly
msg.QueueMessage.Attributes.Sequence = 42

// v2 — use accessor methods
seq := msg.Attributes.Sequence
channel := msg.Channel
body := msg.Body
```

### BC-9: Internal Package Restructure

| Aspect | v1 | v2 |
|--------|----|----|
| `Transport` interface | Public (in root package) | Internal (`internal/transport/`) |
| `GetGRPCRawClient()` | Available | Removed |

```go
// v1 — accessing transport internals
rawClient := client.GetGRPCRawClient()

// v2 — use public Client API only
// There is no escape hatch to the raw gRPC client.
```

### BC-10: E(), C(), Q() Shortcuts Removed

| Aspect | v1 | v2 |
|--------|----|----|
| Event shortcut | `client.E()` | `kubemq.NewEvent()` |
| Command shortcut | `client.C()` | `kubemq.NewCommand()` |
| Query shortcut | `client.Q()` | `kubemq.NewQuery()` |

```go
// v1
err := client.E().
    SetChannel("demo").
    SetBody([]byte("hello")).
    Send(ctx)

// v2
err := client.SendEvent(ctx, kubemq.NewEvent().
    SetChannel("demo").
    SetBody([]byte("hello")))
```

### BC-11: ClientId Enforcement

| Aspect | v1 | v2 |
|--------|----|----|
| Default `clientId` | `"ClientId"` (shared literal) | Auto-generated UUID |
| Empty `clientId` | Allowed | Allowed (UUID generated) |

```go
// v1 — shared default
client, _ := kubemq.NewClient(ctx,
    kubemq.WithAddress("localhost", 50000),
    // clientId defaults to "ClientId" — shared across all clients!
)

// v2 — unique by default
client, _ := kubemq.NewClient(ctx,
    kubemq.WithAddress("localhost", 50000),
    // clientId auto-generated as UUID — unique per client
)
```

### BC-12: Subscription Error Channel Removed

| Aspect | v1 | v2 |
|--------|----|----|
| Error delivery | Separate `chan error` parameter | `WithOnError(func(error))` option |

```go
// v1
errCh := make(chan error)
eventsCh, err := client.SubscribeToEvents(ctx, "demo", "", errCh)

// v2
sub, err := client.SubscribeToEvents(ctx, "demo", "",
    kubemq.WithOnEvent(func(event *kubemq.Event) {
        fmt.Println(event.Body)
    }),
    kubemq.WithOnError(func(err error) {
        log.Println("Subscription error:", err)
    }),
)
defer sub.Unsubscribe()
```

### BC-13: OpenCensus Tracing Removed

| Aspect | v1 | v2 |
|--------|----|----|
| Tracing library | OpenCensus (`go.opencensus.io`) | OpenTelemetry (`go.opentelemetry.io/otel`) |
| `AddTrace()` | Manual trace attachment | Automatic span creation via `WithTracerProvider()` |

```go
// v1 — manual OpenCensus tracing
import "go.opencensus.io/trace"
ctx, span := trace.StartSpan(ctx, "my-operation")
defer span.End()
event.AddTrace(span)
err := event.Send(ctx)

// v2 — automatic OpenTelemetry instrumentation
import "go.opentelemetry.io/otel"
tp := otel.GetTracerProvider()
client, _ := kubemq.NewClient(ctx,
    kubemq.WithAddress("localhost", 50000),
    kubemq.WithTracerProvider(tp),
)
err := client.SendEvent(ctx, event)
```

### BC-14: queues_stream Package Removed

| Aspect | v1 | v2 |
|--------|----|----|
| Queue stream client | Separate `queues_stream.QueuesStreamClient` | Unified `kubemq.Client` with queue methods |
| Constructor | `kubemq.NewQueuesStreamClient(ctx, opts...)` | `kubemq.NewClient(ctx, opts...)` |

```go
// v1 — separate client for queue streams
import "github.com/kubemq-io/kubemq-go/queues_stream"
streamClient, _ := queues_stream.NewQueuesStreamClient(ctx,
    kubemq.WithAddress("localhost", 50000),
)
result, _ := streamClient.Send(ctx, msg)

// v2 — unified client
client, _ := kubemq.NewClient(ctx,
    kubemq.WithAddress("localhost", 50000),
)
result, _ := client.SendQueueMessage(ctx, msg)
```

## Changelog

For a complete list of all changes (including non-breaking), see the
[Changelog](https://github.com/kubemq-io/kubemq-go/blob/master/CHANGELOG.md).
