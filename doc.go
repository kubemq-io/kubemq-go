// Package kubemq provides the KubeMQ SDK for Go — a client library for
// communicating with [KubeMQ](https://kubemq.io/) message broker servers.
//
// The SDK supports all KubeMQ messaging patterns:
//   - Events — fire-and-forget pub/sub (at-most-once)
//   - Events Store — persistent pub/sub with replay (at-least-once)
//   - Queues — pull-based messaging with acknowledgment (at-least-once)
//   - Commands — request/reply for actions (at-most-once)
//   - Queries — request/reply for data retrieval (at-most-once)
//
// # Quick Start
//
//	client, err := kubemq.NewClient(ctx, kubemq.WithAddress("localhost", 50000))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	err = client.SendEvent(ctx, kubemq.NewEvent().
//	    SetChannel("notifications").
//	    SetBody([]byte("hello kubemq")))
//
// # Configuration
//
// Use functional options with [NewClient]:
//   - [WithAddress] — server host and port (default: localhost:50000)
//   - [WithCredentialProvider] — token authentication
//   - [WithTLSConfig] — TLS/mTLS
//   - [WithReconnectPolicy] — reconnection behavior
//   - [WithRetryPolicy] — retry for transient errors
//
// # Error Handling
//
// All operations return structured errors via [KubeMQError]. Use [errors.As]
// to inspect error codes and retryability.
//
// # Concurrency
//
// [Client] is safe for concurrent use by multiple goroutines. Create one
// client and share it across goroutines — do not create a client per operation.
package kubemq
