# KubeMQ Go SDK Examples

Runnable examples demonstrating KubeMQ messaging patterns with the v2 SDK.

## Prerequisites

- Go 1.23+
- KubeMQ server running on `localhost:50000`

```bash
docker run -d -p 50000:50000 kubemq/kubemq
```

## Examples

| Example | Description | Source |
|---------|-------------|--------|
| Events pub/sub | Publish and subscribe to real-time events (basic, stream, wildcard, consumer group) | [pubsub/main.go](pubsub/main.go) |
| Events Store | Persistent events with all 6 start positions, stream send, consumer group | [events_store/main.go](events_store/main.go) |
| Queue send/receive | Simple queue API: single send, batch send, receive, peek, ack all | [queues/main.go](queues/main.go) |
| Queue stream | Stream queue API: upstream send, downstream receive, AckAll, AutoAck, poll mode | [queues_stream/main.go](queues_stream/main.go) |
| Queue stream advanced | Queue policies (expiration, delay, DLQ), AckRange, NAckAll, ReQueueAll | [queues_stream_advanced/main.go](queues_stream_advanced/main.go) |
| Commands & Queries | RPC: send/receive commands and queries, consumer groups, timeout, caching | [cq/main.go](cq/main.go) |
| Channel management | Create, list, delete channels, queue info, list with search, purge queue | [management/main.go](management/main.go) |
| Channels (basic) | Simple create, list, delete channel operations | [channels/main.go](channels/main.go) |
| TLS & mTLS | Connect with TLS, mutual TLS, and auth tokens | [tls/main.go](tls/main.go) |

## Running an Example

```bash
go run ./examples/pubsub/
go run ./examples/events_store/
go run ./examples/queues/
go run ./examples/queues_stream/
go run ./examples/queues_stream_advanced/
go run ./examples/cq/
go run ./examples/management/
go run ./examples/channels/
go run ./examples/tls/
```
