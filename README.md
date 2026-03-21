# KubeMQ Go SDK

[![CI](https://github.com/kubemq-io/kubemq-go/actions/workflows/ci.yml/badge.svg)](https://github.com/kubemq-io/kubemq-go/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/kubemq-io/kubemq-go/branch/master/graph/badge.svg)](https://codecov.io/gh/kubemq-io/kubemq-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/kubemq-io/kubemq-go/v2.svg)](https://pkg.go.dev/github.com/kubemq-io/kubemq-go/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/kubemq-io/kubemq-go/v2)](https://goreportcard.com/report/github.com/kubemq-io/kubemq-go/v2)
[![License](https://img.shields.io/github/license/kubemq-io/kubemq-go)](LICENSE)

The **KubeMQ SDK for Go** enables Go developers to communicate with [KubeMQ](https://kubemq.io/) message broker servers, supporting Events, Events Store, Queues, Commands, and Queries messaging patterns.

> **v1 users:** v1.x receives security patches for 12 months after v2.0.0 GA. See [COMPATIBILITY.md](https://github.com/kubemq-io/kubemq-go/blob/master/COMPATIBILITY.md) for details and [MIGRATION.md](https://github.com/kubemq-io/kubemq-go/blob/master/MIGRATION.md) for upgrade instructions.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Messaging Patterns](#messaging-patterns)
- [Configuration](#configuration)
- [Error Handling](#error-handling)
- [Troubleshooting](#troubleshooting)
- [Examples](#examples)
- [Migration from v1](#migration-from-v1)
- [Deprecation Policy](#deprecation-policy)
- [Performance](#performance)
- [Security](#security)
- [Additional Resources](#additional-resources)
- [Contributing](#contributing)
- [License](#license)

## Installation

```bash
go get github.com/kubemq-io/kubemq-go/v2
```

Requires Go 1.23 or later. Compatible with KubeMQ server v2.2 or later (see [COMPATIBILITY.md](https://github.com/kubemq-io/kubemq-go/blob/master/COMPATIBILITY.md)).

## Quick Start

### Quick Start: Events

#### Prerequisites

- Go 1.23+
- KubeMQ server running on `localhost:50000` (`docker run -d -p 50000:50000 kubemq/kubemq`)
- Install: `go get github.com/kubemq-io/kubemq-go/v2`

#### Send an Event

```go
package main

import (
    "context"
    "log"

    "github.com/kubemq-io/kubemq-go/v2"
)

func main() {
    ctx := context.Background()
    client, err := kubemq.NewClient(ctx,
        kubemq.WithAddress("localhost", 50000),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    err = client.SendEvent(ctx, kubemq.NewEvent().
        SetChannel("notifications").
        SetBody([]byte("hello kubemq")),
    )
    if err != nil {
        log.Fatal(err)
    }
    log.Println("Event sent successfully")
}
```

#### Receive Events

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/kubemq-io/kubemq-go/v2"
)

func main() {
    ctx := context.Background()
    client, err := kubemq.NewClient(ctx,
        kubemq.WithAddress("localhost", 50000),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    sub, err := client.SubscribeToEvents(ctx, "notifications", "",
        kubemq.WithOnEvent(func(event *kubemq.Event) {
            fmt.Printf("Received event: %s\n", event.Body)
        }),
        kubemq.WithOnError(func(err error) {
            log.Println("Subscription error:", err)
        }),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer sub.Unsubscribe()

    <-ctx.Done()
}
```

### Quick Start: Queues

```go
// Send
msg := kubemq.NewQueueMessage().SetChannel("tasks").SetBody([]byte("hello"))
result, err := client.SendQueueMessage(ctx, msg)

// Receive (poll-based)
resp, err := client.PollQueue(ctx, &kubemq.PollRequest{
    Channel:            "tasks",
    MaxItems:           1,
    WaitTimeoutSeconds: 10,
})
// Process resp.Messages
```

### Quick Start: Commands (RPC)

```go
// Send command
cmdResp, err := client.SendCommand(ctx, kubemq.NewCommand().
    SetChannel("orders").
    SetBody([]byte("create order")).
    SetTimeout(10*time.Second))

// Handle commands (subscribe)
sub, err := client.SubscribeToCommands(ctx, "orders", "",
    kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
        resp := kubemq.NewCommandReply().
            SetRequestId(cmd.Id).
            SetResponseTo(cmd.ResponseTo).
            SetBody([]byte("done")).
            SetExecutedAt(time.Now())
        _ = client.SendCommandResponse(ctx, resp)
    }),
)
```

## Messaging Patterns

| Pattern | Delivery Guarantee | Use When | Example Use Case |
|---------|--------------------|----------|------------------|
| Events | At-most-once | Fire-and-forget broadcasting to multiple subscribers | Real-time notifications, log streaming |
| Events Store | At-least-once (persistent) | Subscribers must not miss messages, even if offline | Audit trails, event sourcing, replay |
| Queues | At-least-once (with ack) | Work must be processed by exactly one consumer with acknowledgment | Job processing, task distribution |
| Commands | At-most-once (request/reply) | You need a response confirming an action was executed | Device control, configuration changes |
| Queries | At-most-once (request/reply) | You need to retrieve data from a responder | Data lookups, service-to-service reads |

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `WithAddress(host, port)` | `localhost:50000` | KubeMQ server address |
| `WithClientId(id)` | Auto-generated UUID | Unique client identifier |
| `WithCredentialProvider(p)` | None (insecure) | Authentication credential provider |
| `WithTLS(caCertFile)` | None (plaintext) | TLS with CA cert file; use `WithTLSConfig(cfg)` for full config |
| `WithReconnectPolicy(p)` | Infinite retries, 1s–30s backoff | Reconnection behavior on disconnect |
| `WithConnectionTimeout(d)` | 10s | Maximum time to establish initial connection |
| `WithRetryPolicy(p)` | 3 retries, 100ms–10s backoff | Retry policy for transient operation failures |
| `WithLogger(l)` | None (silent) | Structured logger for SDK diagnostics |
| `WithTracerProvider(tp)` | None (no tracing) | OpenTelemetry tracer provider |
| `WithMeterProvider(mp)` | None (no metrics) | OpenTelemetry meter provider |

## Error Handling

All SDK operations return errors as `*kubemq.KubeMQError`, which provides structured error information:

```go
err := client.SendEvent(ctx, event)
if err != nil {
    var ke *kubemq.KubeMQError
    if errors.As(err, &ke) {
        fmt.Printf("Code: %s, Retryable: %v\n", ke.Code, ke.IsRetryable)

        switch ke.Code {
        case kubemq.ErrCodeTimeout:
            // Retry with longer timeout
        case kubemq.ErrCodeAuthentication:
            // Check credentials
        case kubemq.ErrCodeTransient:
            // Automatic retry was exhausted; consider manual retry
        }
    }
}
```

The SDK automatically retries transient errors using exponential backoff with jitter. Configure retry behavior with `WithRetryPolicy`:

```go
client, _ := kubemq.NewClient(ctx,
    kubemq.WithAddress("localhost", 50000),
    kubemq.WithRetryPolicy(kubemq.RetryPolicy{
        MaxRetries:     5,
        InitialBackoff: 200 * time.Millisecond,
        MaxBackoff:     15 * time.Second,
        Multiplier:     2.0,
        JitterMode:     kubemq.JitterFull,
    }),
)
```

For the full error code reference, see the [API documentation](https://pkg.go.dev/github.com/kubemq-io/kubemq-go/v2#ErrorCode).

## Troubleshooting

| Problem | Quick Fix |
|---------|-----------|
| Connection refused | Verify KubeMQ server is running: `kubectl get pods -l app=kubemq` |
| Authentication failed | Check token validity; see [Troubleshooting Guide](https://github.com/kubemq-io/kubemq-go/blob/master/TROUBLESHOOTING.md#authentication-failed) |
| Timeout / deadline exceeded | Increase timeout via `context.WithTimeout` or `WithRetryPolicy` |
| No messages received | Verify channel name matches publisher; check subscriber group |
| TLS handshake failure | Verify certificate paths and CA trust chain |

See [TROUBLESHOOTING.md](https://github.com/kubemq-io/kubemq-go/blob/master/TROUBLESHOOTING.md) for the complete guide with error messages and step-by-step solutions.

## Examples

Runnable examples are in the [examples](https://github.com/kubemq-io/kubemq-go/tree/master/examples) directory:

- [examples/pubsub](https://github.com/kubemq-io/kubemq-go/blob/master/examples/pubsub/main.go) — Events pub/sub
- [examples/queues](https://github.com/kubemq-io/kubemq-go/blob/master/examples/queues/main.go) — Queue send/receive with ack
- [examples/cq](https://github.com/kubemq-io/kubemq-go/blob/master/examples/cq/main.go) — Commands and Queries (RPC)

Run an example:

```bash
go run ./examples/pubsub/
```

## Migration from v1

For users upgrading from v1, see the [Migration Guide](https://github.com/kubemq-io/kubemq-go/blob/master/MIGRATION.md).

For a complete list of changes by version, see the [Changelog](https://github.com/kubemq-io/kubemq-go/blob/master/CHANGELOG.md).

## Deprecation Policy

- Deprecated APIs are annotated with `// Deprecated: Use X instead.` (recognized by `go vet`).
- Deprecated APIs remain functional for at least 2 minor versions or 6 months, whichever is longer.
- Deprecation notices name the replacement API.
- Removed APIs are listed in [CHANGELOG.md](https://github.com/kubemq-io/kubemq-go/blob/master/CHANGELOG.md) and [MIGRATION.md](https://github.com/kubemq-io/kubemq-go/blob/master/MIGRATION.md).

## Performance

### Characteristics

| Parameter | Value | Notes |
|-----------|-------|-------|
| Transport | gRPC (HTTP/2) | Single multiplexed connection |
| Max message size | 100 MB (send and receive) | Configurable via `WithMaxSendMessageSize` / `WithMaxReceiveMessageSize` |
| Max concurrent streams | Unlimited (gRPC default) | Limited by server configuration |
| Connection setup | ~50-200ms typical | Includes TLS handshake if secured |
| Keepalive interval | 10s (configurable) | Dead connection detected within 15s |

### Performance Tips

1. **Reuse the client instance.** Create one `kubemq.Client` and share it across all goroutines.
2. **Use batching for high-throughput queue sends.** Aim for 10–100 messages per batch with `SendQueueMessages`.
3. **Do not block subscription callbacks.** Offload slow work to worker pools.
4. **Close streams when done.** Call `Unsubscribe()` when no longer needed.

See [BENCHMARKS.md](https://github.com/kubemq-io/kubemq-go/blob/master/BENCHMARKS.md) for reproducible benchmark results.

## Security

See [SECURITY.md](SECURITY.md) for vulnerability reporting. The SDK supports TLS and mTLS connections — for configuration details, see [How to Connect with TLS](docs/how-to/connect-with-tls.md).

## Additional Resources

- [KubeMQ Documentation](https://docs.kubemq.io/) — Official KubeMQ documentation and guides
- [Full Documentation Index](docs/INDEX.md) — Complete SDK documentation index
- [KubeMQ Concepts](docs/CONCEPTS.md) — Core KubeMQ messaging concepts
- [SDK Feature Parity Matrix](../sdk-feature-parity-matrix.md) — Cross-SDK feature comparison
- [CHANGELOG.md](https://github.com/kubemq-io/kubemq-go/blob/master/CHANGELOG.md) — Release history
- [MIGRATION.md](https://github.com/kubemq-io/kubemq-go/blob/master/MIGRATION.md) — v1 to v2 migration guide
- [TROUBLESHOOTING.md](https://github.com/kubemq-io/kubemq-go/blob/master/TROUBLESHOOTING.md) — Common issues and solutions
- [Examples](https://github.com/kubemq-io/kubemq-go/tree/master/examples) — Runnable code examples for all patterns

## Contributing

See [CONTRIBUTING.md](https://github.com/kubemq-io/kubemq-go/blob/master/CONTRIBUTING.md) for development setup, coding standards, and pull request process.

## License

Apache License 2.0 — see [LICENSE](https://github.com/kubemq-io/kubemq-go/blob/master/LICENSE) for details.
