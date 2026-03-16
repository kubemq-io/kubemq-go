// Package kubemq provides the KubeMQ SDK for Go — a client library for
// communicating with [KubeMQ](https://kubemq.io/) message broker servers.
//
// KubeMQ is a Kubernetes-native message queue and message broker designed for
// microservices architectures. This SDK gives Go applications access to all
// KubeMQ messaging patterns over gRPC, making it suitable for building
// distributed systems, event-driven pipelines, and service-to-service
// communication on Kubernetes.
//
// # Key Features
//
//   - Publish/subscribe (pub/sub) with Events and Events Store
//   - Request/reply with Commands and Queries
//   - Pull-based message queue with acknowledgment
//   - Automatic reconnection and retry with exponential backoff
//   - OpenTelemetry tracing and metrics
//   - TLS/mTLS and token-based authentication
//
// # Messaging Patterns
//
// The SDK supports all KubeMQ messaging patterns:
//   - Events — fire-and-forget publish subscribe (at-most-once delivery)
//   - Events Store — persistent pub/sub with replay (at-least-once delivery)
//   - Queues — pull-based message queue with acknowledgment (at-least-once)
//   - Commands — request reply for executing actions (at-most-once)
//   - Queries — request reply for data retrieval (at-most-once)
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
//
// # Documentation
//
// For the full KubeMQ documentation, visit https://docs.kubemq.io/.
//
// Additional resources:
//   - Homepage: https://kubemq.io/
//   - pkg.go.dev: https://pkg.go.dev/github.com/kubemq-io/kubemq-go/v2
//   - Issues: https://github.com/kubemq-io/kubemq-go/issues
//   - Changelog: https://github.com/kubemq-io/kubemq-go/blob/main/CHANGELOG.md
//   - Troubleshooting: https://github.com/kubemq-io/kubemq-go/blob/main/TROUBLESHOOTING.md
//   - Examples: https://github.com/kubemq-io/kubemq-go/tree/main/examples
package kubemq
