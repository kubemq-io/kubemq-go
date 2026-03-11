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
| Events pub/sub | Publish and subscribe to real-time events | [pubsub/main.go](pubsub/main.go) |
| Queue send/receive | Send, receive, and acknowledge queue messages | [queues/main.go](queues/main.go) |
| Commands & Queries | RPC patterns: send command/query, handle with response | [cq/main.go](cq/main.go) |

## Running an Example

```bash
go run ./examples/pubsub/
go run ./examples/queues/
go run ./examples/cq/
```
