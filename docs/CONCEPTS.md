# KubeMQ Concepts

A comprehensive guide to KubeMQ messaging patterns, architecture, and operational best practices.

---

## Table of Contents

- [Overview](#overview)
- [Messaging Patterns](#messaging-patterns)
  - [Events (Pub/Sub)](#events-pubsub)
  - [Events Store (Persistent Pub/Sub)](#events-store-persistent-pubsub)
  - [Queues](#queues)
  - [Commands (RPC)](#commands-rpc)
  - [Queries (RPC)](#queries-rpc)
- [Channels](#channels)
- [Consumer Groups](#consumer-groups)
- [Message Lifecycle](#message-lifecycle)
- [Connection Lifecycle](#connection-lifecycle)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)

---

## Overview

KubeMQ is a Kubernetes-native message broker and message queue engine designed for
cloud-native applications. It provides multiple messaging patterns through a unified
gRPC-based API, enabling developers to choose the right pattern for each use case
without deploying multiple messaging systems.

KubeMQ runs as a StatefulSet inside Kubernetes and supports five messaging patterns:

| Pattern       | Delivery Guarantee | Persistence | Direction          |
|---------------|--------------------|-------------|--------------------|
| Events        | At-most-once       | No          | One-to-many        |
| Events Store  | At-least-once      | Yes         | One-to-many        |
| Queues        | At-least-once      | Yes         | One-to-one (FIFO)  |
| Commands      | Exactly-once       | No          | One-to-one (RPC)   |
| Queries       | Exactly-once       | No          | One-to-one (RPC)   |

All SDKs (Go, Python, Java, C#, JavaScript/TypeScript) expose the same patterns and
semantics, so concepts learned here apply uniformly across languages.

---

## Messaging Patterns

### Events (Pub/Sub)

Events implement a fire-and-forget publish/subscribe model with **at-most-once**
delivery semantics.

A publisher sends a message to a channel. All active subscribers receive it
immediately. If no subscriber is listening, the message is **lost**. The publisher
gets confirmation that the broker accepted the message, but not that any subscriber
received it.

**Key characteristics:**

- No persistence — messages exist only in transit
- No replay — once delivered (or missed), a message is gone
- Supports wildcard channel subscriptions
- Lowest latency of all patterns

**Use cases:** real-time notifications, telemetry streaming, live dashboards,
log forwarding, heartbeat broadcasts.

---

### Events Store (Persistent Pub/Sub)

Events Store extends the Events pattern by adding **persistence** and **replay**
capabilities with **at-least-once** delivery semantics.

A publisher sends a message to an Events Store channel. The broker persists it and
assigns a monotonically increasing **sequence number**. Active subscribers receive
the message immediately; new or reconnecting subscribers can replay from any point.

**Subscription start positions:**

| Position            | Description                                         |
|---------------------|-----------------------------------------------------|
| Start from new      | Only receive messages published after subscribing   |
| Start from first    | Replay all messages from the beginning of the store |
| Start from last     | Start from the most recent message                  |
| Start from sequence | Replay from a specific sequence number              |
| Start from time     | Replay from a specific timestamp                    |
| Start from delta    | Replay messages from a relative time offset (e.g., last 5 minutes) |

**Key characteristics:**

- Messages are persisted to disk (configurable retention)
- Each message is assigned a unique, ordered sequence number
- Subscribers can join at any time and catch up from any point

**Use cases:** event sourcing, audit trails, data synchronization,
replay-based debugging, stream processing pipelines.

---

### Queues

Queues provide a **point-to-point** messaging pattern with guaranteed **at-least-once**
delivery. Messages persist until explicitly acknowledged by a consumer.

A producer sends a message to a queue channel; the broker persists it. A consumer
polls or receives the message, processes it, and sends an **acknowledgment** (ack).
On ack, the message is removed. If the consumer fails to ack within the **visibility
timeout**, the message becomes available to other consumers.

**Advanced features:**

- **Visibility timeout** — Time a message is hidden from other consumers while being
  processed. If the consumer crashes or doesn't ack in time, the message reappears.
- **Dead-letter queue** — Messages exceeding the maximum receive count are moved to a
  dead-letter channel for inspection.
- **Delayed delivery** — Messages can be scheduled for future delivery by specifying
  a delay duration.
- **Batch operations** — Send and receive multiple messages in a single call.
- **Message expiration** — Messages can carry a TTL; expired messages are discarded
  automatically.

**Use cases:** task distribution, order processing, background jobs,
reliable inter-service communication, work distribution.

---

### Commands (RPC)

Commands implement a synchronous **request-response** pattern for operations that
produce a side effect but return only a success/failure result.

**How it works:**

1. The sender publishes a command to a channel with a **timeout**.
2. The broker routes the command to exactly one responder (or one member of a
   consumer group).
3. The responder executes the operation and sends back a boolean result.
4. The sender blocks until the response arrives or the timeout expires.

**Key characteristics:**

- Synchronous — the sender blocks until a response is received
- Boolean result with an optional error message
- Timeout-based — no response within the deadline means failure
- Exactly one responder handles each command

**Use cases:** remote procedure calls, service orchestration,
configuration updates, administrative operations.

---

### Queries (RPC)

Queries implement a synchronous **request-response** pattern for operations that
**return data**. They are the read counterpart to Commands.

**How it works:**

1. The sender publishes a query to a channel with a **timeout**.
2. The broker routes the query to exactly one responder (or one member of a
   consumer group).
3. The responder assembles a response payload and sends it back.
4. The sender blocks until the response arrives or the timeout expires.

**Key characteristics:**

- Synchronous request-response with arbitrary data return
- Timeout-based — the caller blocks until a response or timeout
- Supports **response caching** — the broker can cache responses for a configurable
  duration, serving repeated queries without hitting the responder

**Use cases:** data retrieval, aggregation queries, health-check/status queries,
configuration fetching, search and lookup operations.

---

## Channels

Channels are **named logical endpoints** that route messages between publishers and
subscribers (or senders and responders). Every message targets a channel, and every
subscription listens on a channel.

### Channel Types

Each channel is associated with exactly one messaging pattern:

| Channel Type   | Pattern      | Example Channel Name         |
|----------------|--------------|------------------------------|
| events         | Events       | `notifications.user.signup`  |
| events_store   | Events Store | `audit.orders.created`       |
| queues         | Queues       | `tasks.image.resize`         |
| commands       | Commands     | `service.payments.charge`    |
| queries        | Queries      | `service.inventory.check`    |

### Naming Conventions

- Use **dot-delimited** hierarchical names: `domain.entity.action`
- Keep names lowercase with alphanumeric characters, dots, hyphens, and underscores
- Be descriptive: `orders.payment.processed` is better than `ch1`
- Group related channels under a common prefix for organizational clarity

### Wildcard Subscriptions (Events only)

Events and Events Store channels support wildcard subscriptions:

| Wildcard | Meaning                    | Example                                        |
|----------|----------------------------|------------------------------------------------|
| `*`      | Matches one level          | `notifications.*` matches `notifications.email` but not `notifications.email.sent` |
| `>`      | Matches one or more levels | `notifications.>` matches `notifications.email` and `notifications.email.sent` |

Queues, Commands, and Queries require exact channel names.

### Implicit vs. Explicit Creation

- **Implicit**: Channels are created automatically on first use (first message sent
  or first subscription registered). No upfront configuration is needed.
- **Explicit**: Channels can also be managed through the management API for enforcing
  naming policies or pre-provisioning infrastructure.

---

## Consumer Groups

Consumer groups enable **load balancing** of messages across multiple subscribers.
Instead of every subscriber receiving every message, each message is delivered to
**exactly one member** of the group.

### How It Works

1. Multiple subscribers connect to the same channel with the **same group name**.
2. The broker treats them as a single logical consumer.
3. Each incoming message is routed to exactly one member (round-robin by default).
4. If a member disconnects, its share is redistributed to remaining members.

### Availability

Consumer groups are available for all messaging patterns:

| Pattern       | Group Behavior                                      |
|---------------|-----------------------------------------------------|
| Events        | One subscriber in the group receives each event     |
| Events Store  | One subscriber in the group receives each event     |
| Queues        | One consumer in the group receives each message     |
| Commands      | One responder in the group handles each command     |
| Queries       | One responder in the group handles each query       |

### When to Use

- **Horizontal scaling** — Distribute load across multiple service instances
- **Work distribution** — Spread CPU-intensive processing across workers
- **High availability** — If one member fails, others continue processing

Without consumer groups: Events/Events Store fan out to all subscribers;
Commands/Queries pick one available responder; Queues deliver to polling consumers.

---

## Message Lifecycle

### Events

`Publisher → Send → Broker → Deliver → All Subscribers`

If no subscribers are connected, the message is silently discarded.
No retry or replay mechanism exists for plain Events.

### Events Store

`Publisher → Send → Broker → Persist (seq #) → Deliver → Subscribers`

Messages are retained according to the configured retention policy. New subscribers
can replay the entire history or start from any point in the stream.

### Queues

`Producer → Send → Broker → Persist → Consumer polls → Process → Ack → Remove`

**Failure scenarios:**

- **No ack within visibility timeout** — Message becomes visible again and is
  delivered to the next available consumer.
- **Explicit reject** — Message is returned to the queue immediately.
- **Max receive count exceeded** — Message is moved to the dead-letter queue.
- **Message expired (TTL)** — Message is removed from the queue automatically.

---

## Connection Lifecycle

KubeMQ SDKs communicate with the broker over **gRPC**. The connection lifecycle is
managed automatically by the SDK.

### Connection States

`DISCONNECTED → CONNECTING → CONNECTED → RECONNECTING → (back to CONNECTING)`

| State          | Description                                           |
|----------------|-------------------------------------------------------|
| DISCONNECTED   | No active connection to the broker                    |
| CONNECTING     | Initial connection attempt in progress                |
| CONNECTED      | Active, healthy connection                            |
| RECONNECTING   | Connection lost; SDK is attempting to re-establish it |

### Connection Parameters

| Parameter          | Description                                           |
|--------------------|-------------------------------------------------------|
| Address            | Broker host and gRPC port (e.g., `localhost:50000`)   |
| Client ID          | Unique identifier for this client instance            |
| Auth Token         | JWT or API key for authentication (optional)          |
| TLS                | Enable TLS encryption for the gRPC channel            |
| TLS Certificate    | Client certificate for mutual TLS (mTLS)             |
| Keep-alive         | Interval for gRPC keep-alive pings                    |
| Reconnect interval | Delay between reconnection attempts                   |
| Max reconnects     | Maximum reconnection attempts (0 = unlimited)         |

### Automatic Reconnection

When the connection is lost (network partition, broker restart, etc.), the SDK
automatically attempts to reconnect. It transitions to `RECONNECTING`, waits the
configured interval, and retries. On success, it re-registers all active
subscriptions. On failure, it retries until the max reconnect limit is reached.

### Security

- **TLS** — Encrypts all traffic between client and broker using TLS 1.2+.
- **mTLS** — Mutual TLS requires both client and server to present certificates.
- **Auth Token** — A bearer token (typically JWT) included in each request for
  identity verification and access control.

---

## Error Handling

KubeMQ SDKs categorize errors to help applications decide how to respond.

### Error Categories

| Category         | Description                                          | Retryable |
|------------------|------------------------------------------------------|-----------|
| CONNECTIVITY     | Network-level failures (timeout, DNS, TCP)           | Yes       |
| AUTHENTICATION   | Invalid or expired auth token                        | No*       |
| CONFIGURATION    | Invalid client settings or parameters                | No        |
| OPERATION        | Business-logic failures (e.g., no responder)         | Sometimes |
| DATA             | Malformed message, payload too large                 | No        |
| SYSTEM           | Internal broker errors                               | Yes       |

\* Authentication errors may become retryable after refreshing the token.

### Error Structure

Each error includes: a **code**, a human-readable **message**, the **category**,
an **isRetryable** flag, and an actionable **suggestion**.

### Retry Behavior

- **Transient errors** (connectivity, some system errors) are automatically retried
  with exponential backoff.
- **Non-retryable errors** (authentication, configuration, data) surface immediately.
- Retry parameters (max retries, backoff interval, jitter) are configurable at the
  client level.

---

## Best Practices

### Client Lifecycle

- **Reuse client instances** — SDK clients are safe for concurrent use. Create one
  client and share it across your application.
- **Always close clients** — Release gRPC connections and deregister subscriptions
  cleanly when shutting down.
- **One client per broker** — Avoid creating multiple clients for the same broker.

### Error Handling

- **Handle errors explicitly** — Never ignore errors from send or subscribe calls.
- **Distinguish retryable from fatal** — Use the error category and `isRetryable`
  flag to decide whether to retry or abort.
- **Implement circuit breakers** — For Commands/Queries, wrap calls in a circuit
  breaker to prevent cascading failures.

### Messaging Design

- **Choose the right pattern** — Events for fire-and-forget, Events Store for replay,
  Queues for reliable task processing, Commands/Queries for synchronous RPC.
- **Set appropriate timeouts** — For Commands and Queries, set timeouts that reflect
  expected processing time plus a margin.
- **Use consumer groups for scaling** — Distribute load across instances rather than
  having each instance process every message.
- **Leverage dead-letter queues** — Capture messages that repeatedly fail processing.
  Monitor and reprocess them rather than losing data.

### Production Readiness

- **Enable TLS in production** — Always encrypt client-to-broker traffic in
  production environments.
- **Use meaningful client IDs** — Include the service name and instance identifier
  (e.g., `order-service-pod-abc123`) for easier debugging.
- **Monitor with OpenTelemetry** — Enable tracing and metrics for visibility into
  message flows, latencies, and error rates.
- **Plan channel naming carefully** — Adopt a consistent naming convention early.
  Renaming channels in production requires coordinating all publishers and
  subscribers.
- **Test with realistic load** — Validate throughput, latency, and error handling
  under expected production load before going live.

---

*This document covers KubeMQ concepts applicable to all SDKs. For language-specific
API references and code examples, see the SDK README and examples directory.*
