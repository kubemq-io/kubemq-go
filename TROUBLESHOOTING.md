# Troubleshooting Guide

This guide covers the most common issues when using the KubeMQ Go SDK v2.
Each entry includes the exact error message, cause, and step-by-step solution.

## Table of Contents

- [Connection refused / timeout](#connection-refused--timeout)
- [Authentication failed](#authentication-failed)
- [Authorization denied](#authorization-denied)
- [Channel not found](#channel-not-found)
- [Message too large](#message-too-large)
- [Timeout / deadline exceeded](#timeout--deadline-exceeded)
- [Rate limiting / throttling](#rate-limiting--throttling)
- [Internal server error](#internal-server-error)
- [TLS handshake failure](#tls-handshake-failure)
- [No messages received](#no-messages-received)
- [Queue message not acknowledged](#queue-message-not-acknowledged)
- [Connection state: RECONNECTING](#connection-state-reconnecting)
- [Buffer full during reconnection](#buffer-full-during-reconnection)
- [How to Enable Debug Logging](#how-to-enable-debug-logging)
- [gRPC Status Code Mapping](#grpc-status-code-mapping)

---

## Connection refused / timeout

**Error message:**
```
kubemq TRANSIENT: failed to connect: dial tcp localhost:50000: connect: connection refused
```

**Cause:** The KubeMQ server is not running, not reachable at the configured
address, or a firewall is blocking the connection.

**Solution:**

1. Verify the KubeMQ server is running:
   ```bash
   kubectl get pods -l app=kubemq
   # or for Docker:
   docker ps | grep kubemq
   ```
2. Confirm the address matches your server deployment:
   ```go
   client, err := kubemq.NewClient(ctx,
       kubemq.WithAddress("kubemq-cluster.default.svc.cluster.local", 50000),
   )
   ```
3. For Kubernetes, ensure the service is exposed:
   ```bash
   kubectl port-forward svc/kubemq-cluster 50000:50000
   ```
4. Check network connectivity:
   ```bash
   nc -zv localhost 50000
   ```

**See also:** [Configuration options](https://github.com/kubemq-io/kubemq-go/blob/master/README.md#configuration)

---

## Authentication failed

**Error message:**
```
kubemq AUTHENTICATION: authentication failed: invalid token
```

**Cause:** The auth token is missing, expired, or invalid for the target
KubeMQ server.

**Solution:**

1. Verify your token is set:
   ```go
   client, err := kubemq.NewClient(ctx,
       kubemq.WithAddress("localhost", 50000),
       kubemq.WithCredentialProvider(
           kubemq.NewStaticTokenProvider("your-valid-token"),
       ),
   )
   ```
2. Check token expiration — tokens may have a TTL configured on the server.
3. Verify the token matches the server's configured auth key.
4. If using a custom `CredentialProvider`, ensure `GetToken()` returns a
   valid token and the correct `expiresAt` time.

**See also:** [TLS config options](https://github.com/kubemq-io/kubemq-go/blob/master/README.md#configuration)

---

## Authorization denied

**Error message:**
```
kubemq AUTHORIZATION: authorization denied: insufficient permissions for channel "orders"
```

**Cause:** The authenticated client does not have permission to perform the
requested operation on the specified channel.

**Solution:**

1. Verify the client's access control configuration on the KubeMQ server.
2. Check that the channel name matches the allowed channels in the ACL.
3. Ensure the client ID has the correct role (publisher, subscriber, or both).

---

## Channel not found

**Error message:**
```
kubemq NOT_FOUND: channel "orders" not found
```

**Cause:** The specified channel does not exist and auto-creation is disabled
on the server.

**Solution:**

1. Create the channel explicitly before publishing (if your server supports it).
2. Or enable auto-channel-creation on the KubeMQ server configuration.
3. Verify the channel name is spelled correctly (channel names are case-sensitive).

---

## Message too large

**Error message:**
```
kubemq VALIDATION: message size 104857601 exceeds maximum allowed size 104857600
```

**Cause:** The message body exceeds the server's maximum message size (default: 100MB).

**Solution:**

1. Reduce the message payload size.
2. Consider splitting large payloads into multiple messages.
3. If needed, configure the server's max message size.

---

## Timeout / deadline exceeded

**Error message:**
```
kubemq TIMEOUT: operation timed out after 5s: SendEvent on channel "orders"
```

**Cause:** The operation did not complete within the configured timeout.
This can happen due to network latency, server load, or an overloaded channel.

**Solution:**

1. Increase the operation timeout:
   ```go
   ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
   defer cancel()
   err := client.SendEvent(ctx, event)
   ```
2. Increase the retry policy timeouts:
   ```go
   client, _ := kubemq.NewClient(ctx,
       kubemq.WithRetryPolicy(kubemq.RetryPolicy{
           MaxRetries:     5,
           InitialBackoff: 500 * time.Millisecond,
           MaxBackoff:     30 * time.Second,
       }),
   )
   ```
3. Check server health and resource utilization.

**See also:** [Configuration options](https://github.com/kubemq-io/kubemq-go/blob/master/README.md#configuration)

---

## Rate limiting / throttling

**Error message:**
```
kubemq THROTTLING: rate limit exceeded: try again after 2s
```

**Cause:** The server is throttling requests from this client due to rate
limiting configuration.

**Solution:**

1. Reduce the publish/send rate from your application.
2. The SDK automatically retries throttled requests with backoff. If retries
   are exhausted, consider increasing `MaxRetries` in your `RetryPolicy`.
3. Check the server's rate limiting configuration.

---

## Internal server error

**Error message:**
```
kubemq FATAL: internal server error: {server-provided detail}
```

**Cause:** An unexpected error occurred on the KubeMQ server.

**Solution:**

1. Check the KubeMQ server logs for detailed error information.
2. Verify the server version is compatible with SDK v2 (see
   [Compatibility](https://github.com/kubemq-io/kubemq-go/blob/master/COMPATIBILITY.md)).
3. If the error persists, report it with the full error message and server logs.

---

## TLS handshake failure

**Error message:**
```
kubemq TRANSIENT: TLS handshake failed: x509: certificate signed by unknown authority
```

**Cause:** The client cannot verify the server's TLS certificate. The CA
certificate is missing, incorrect, or the server's certificate is self-signed.

**Solution:**

1. Provide the CA certificate:
   ```go
   client, _ := kubemq.NewClient(ctx,
       kubemq.WithAddress("kubemq.example.com", 50000),
       kubemq.WithTLS("/path/to/ca.pem"),
   )
   ```
   For full TLS configuration (server name override, PEM bytes), use `WithTLSConfig`:
   ```go
   kubemq.WithTLSConfig(&kubemq.TLSConfig{
       CACertFile: "/path/to/ca.pem",
       ServerName: "kubemq.example.com",
   })
   ```
2. For development with self-signed certificates, you can skip verification
   (NOT recommended for production):
   ```go
   kubemq.WithInsecureSkipVerify()
   ```
3. Verify the certificate chain: `openssl s_client -connect kubemq.example.com:50000`

**See also:** [Configuration options](https://github.com/kubemq-io/kubemq-go/blob/master/README.md#configuration)

---

## No messages received

**Error message:** No error — subscriber simply does not receive messages.

**Cause:** Multiple possible causes:
- Channel name mismatch between publisher and subscriber
- Subscriber started after publisher (Events are at-most-once)
- Group name conflict causing messages to route to a different subscriber
- Context cancelled before messages arrive

**Solution:**

1. Verify channel names match exactly (case-sensitive):
   ```go
   // Publisher
   event.SetChannel("orders.new")

   // Subscriber — must match
   sub, _ := client.SubscribeToEvents(ctx, "orders.new", "",
       kubemq.WithOnEvent(func(event *kubemq.Event) { /* ... */ }),
   )
   ```
2. For persistent delivery, use **Events Store** instead of Events:
   ```go
   sub, _ := client.SubscribeToEventsStore(ctx, "orders.new", "",
       kubemq.StartFromFirstEvent(),
       kubemq.WithOnEventStoreReceive(func(event *kubemq.EventStoreReceive) { /* ... */ }),
   )
   ```
3. Check that subscriber context is not cancelled.
4. Verify no other subscriber in the same group is consuming the messages.

---

## Queue message not acknowledged

**Error message:**
```
kubemq TIMEOUT: visibility timeout expired for message {id} on channel "tasks"
```

**Cause:** A queue message was received but not acknowledged (ack) within
the visibility timeout period.

**Solution:**

1. Acknowledge messages promptly after processing:
   ```go
   resp, _ := client.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
       Channel:             "tasks",
       MaxNumberOfMessages: 1,
       WaitTimeSeconds:     10,
   })
   // Process the messages
   client.AckAllQueueMessages(ctx, &kubemq.AckAllQueueMessagesRequest{
       Channel:         "tasks",
       WaitTimeSeconds: 5,
   })
   ```
2. If processing takes longer, reduce batch size or increase visibility timeout on the server.
3. Ensure `IsPeak: false` when you intend to consume (not peek) messages.

---

## Connection state: RECONNECTING

**Error message (in logs):**
```
kubemq: connection state changed to RECONNECTING (attempt 3/∞)
```

**Cause:** The client lost connection to the server and is attempting to
reconnect using the configured `ReconnectPolicy`.

**Solution:**

1. This is normal behavior during server restarts or network disruptions.
2. Monitor connection state changes:
   ```go
   client, _ := kubemq.NewClient(ctx,
       kubemq.WithOnReconnected(func() {
           log.Println("Connection restored")
       }),
   )
   ```
3. Messages published during reconnection are buffered (up to `BufferSize`).
4. If reconnection fails permanently, the SDK fires the error callback.

---

## Buffer full during reconnection

**Error message:**
```
kubemq: reconnection buffer full (capacity=1000, queued=1000)
```

**Cause:** The reconnection message buffer is full because the client has
been disconnected for too long and too many messages are waiting to be sent.

**Solution:**

1. Increase the buffer size:
   ```go
   client, _ := kubemq.NewClient(ctx,
       kubemq.WithReconnectPolicy(kubemq.ReconnectPolicy{
           BufferSize: 5000,
       }),
   )
   ```
2. Reduce the publish rate while disconnected.
3. Check why the server is unavailable for extended periods.

---

## How to Enable Debug Logging

By default, the SDK is silent — no log output is produced. To enable debug-level
logging for SDK diagnostics, attach a `slog.Logger` configured at `LevelDebug`
using the built-in `SlogAdapter`.

```go
package main

import (
    "context"
    "log/slog"
    "os"

    "github.com/kubemq-io/kubemq-go/v2"
)

func main() {
    // Create a slog.Logger at debug level
    debugLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
        Level: slog.LevelDebug,
    }))

    client, err := kubemq.NewClient(context.Background(),
        kubemq.WithAddress("localhost", 50000),
        kubemq.WithLogger(kubemq.NewSlogAdapter(debugLogger)),
    )
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // Use the client — debug logs will appear on stderr
}
```

**What to expect:** With debug logging enabled, the SDK emits structured
key-value log lines to stderr showing connection state transitions, retry
attempts, subscription lifecycle events, and gRPC-level diagnostics. Example:

```
time=2025-01-15T10:30:00.000Z level=DEBUG msg="connection state changed" state=READY
time=2025-01-15T10:30:00.001Z level=DEBUG msg="sending event" channel=notifications
```

**Tip:** Use `slog.LevelInfo` instead of `slog.LevelDebug` in production to
reduce noise while still capturing connection events and errors.

---

## gRPC Status Code Mapping

When the SDK receives a gRPC error from the server, it maps the gRPC status
code to an SDK error code and category. This table shows every mapping used
by `ClassifyGRPCCode` in the SDK:

| gRPC Status Code | SDK Error Code | Category | Retryable | Description |
|---|---|---|---|---|
| `OK` | `CANCELLATION` | Cancellation | No | Operation completed successfully |
| `Canceled` | `CANCELLATION` | Cancellation | No | Operation was cancelled by the caller |
| `Unknown` | `TRANSIENT` | Transient | Yes | Unknown server error; treated as transient |
| `InvalidArgument` | `VALIDATION` | Validation | No | Client sent an invalid request |
| `DeadlineExceeded` | `TIMEOUT` | Timeout | Yes | Operation timed out |
| `NotFound` | `NOT_FOUND` | NotFound | No | Requested resource (channel, etc.) not found |
| `AlreadyExists` | `VALIDATION` | Validation | No | Resource already exists |
| `PermissionDenied` | `AUTHORIZATION` | Authorization | No | Client lacks permission for the operation |
| `ResourceExhausted` | `THROTTLING` | Throttling | Yes | Rate limit or quota exceeded |
| `FailedPrecondition` | `VALIDATION` | Validation | No | Operation rejected due to system state |
| `Aborted` | `TRANSIENT` | Transient | Yes | Operation aborted; safe to retry |
| `OutOfRange` | `VALIDATION` | Validation | No | Value out of accepted range |
| `Unimplemented` | `FATAL` | Fatal | No | Operation not supported by the server |
| `Internal` | `FATAL` | Fatal | No | Internal server error |
| `Unavailable` | `TRANSIENT` | Transient | Yes | Server temporarily unavailable |
| `DataLoss` | `FATAL` | Fatal | No | Unrecoverable data loss or corruption |
| `Unauthenticated` | `AUTHENTICATION` | Authentication | No | Missing or invalid authentication credentials |

**Retryable** errors (`TRANSIENT`, `TIMEOUT`, `THROTTLING`) are automatically
retried by the SDK according to the configured `RetryPolicy`. Non-retryable
errors surface immediately to the caller.
