# How to Handle Errors and Retries

Work with the SDK's structured error types to build resilient applications with automatic and custom retry logic.

## KubeMQError Structure

Every SDK operation returns a `*KubeMQError` with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `Code` | `ErrorCode` | Machine-readable code: `TRANSIENT`, `TIMEOUT`, `THROTTLING`, `AUTHENTICATION`, `AUTHORIZATION`, `VALIDATION`, `NOT_FOUND`, `FATAL`, `CANCELLATION`, `BACKPRESSURE` |
| `Message` | `string` | Human-readable description |
| `Operation` | `string` | The SDK operation that failed (e.g., `SendEvent`) |
| `Channel` | `string` | The channel involved, if any |
| `IsRetryable` | `bool` | Whether the error is safe to retry |
| `Cause` | `error` | The underlying error |
| `RequestID` | `string` | Correlation ID for log tracing |

## Inspecting Errors with errors.As

Use `errors.As` to extract the typed error and inspect its fields:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("error-handling-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	err = client.SendEvent(ctx, kubemq.NewEvent().
		SetChannel("errors.demo").
		SetBody([]byte("test")))

	if err != nil {
		var kErr *kubemq.KubeMQError
		if errors.As(err, &kErr) {
			fmt.Printf("Code:      %s\n", kErr.Code)
			fmt.Printf("Operation: %s\n", kErr.Operation)
			fmt.Printf("Channel:   %s\n", kErr.Channel)
			fmt.Printf("Retryable: %v\n", kErr.IsRetryable)

			switch kErr.Code {
			case kubemq.ErrCodeAuthentication:
				fmt.Println("Action: Check your auth token")
			case kubemq.ErrCodeValidation:
				fmt.Println("Action: Check request parameters")
			case kubemq.ErrCodeTransient:
				fmt.Println("Action: Retry the operation")
			}
		}
	}
}
```

## Matching by Error Code with errors.Is

`KubeMQError` supports `errors.Is` matching by `Code`:

```go
if errors.Is(err, &kubemq.KubeMQError{Code: kubemq.ErrCodeTimeout}) {
	fmt.Println("Operation timed out — consider increasing the timeout")
}

if errors.Is(err, &kubemq.KubeMQError{Code: kubemq.ErrCodeNotFound}) {
	fmt.Println("Channel does not exist — create it first")
}
```

## Checking IsRetryable

The `IsRetryable` field tells you whether an error is safe to retry. Retryable categories are `TRANSIENT`, `TIMEOUT`, and `THROTTLING`.

```go
var kErr *kubemq.KubeMQError
if errors.As(err, &kErr) && kErr.IsRetryable {
	// safe to retry
}
```

## Built-in Retry Policy

The SDK includes automatic retries via `WithRetryPolicy`. The default policy retries 3 times with exponential backoff:

```go
client, err := kubemq.NewClient(ctx,
	kubemq.WithAddress("localhost", 50000),
	kubemq.WithClientId("retry-client"),
	kubemq.WithRetryPolicy(kubemq.RetryPolicy{
		MaxRetries:     5,
		InitialBackoff: 200 * time.Millisecond,
		MaxBackoff:     15 * time.Second,
		Multiplier:     2.0,
		JitterMode:     kubemq.JitterFull,
	}),
	kubemq.WithMaxConcurrentRetries(10),
)
```

| Field | Default | Description |
|-------|---------|-------------|
| `MaxRetries` | 3 | Maximum retry attempts (0 disables retries) |
| `InitialBackoff` | 100ms | Delay before the first retry |
| `MaxBackoff` | 10s | Upper bound on backoff delay |
| `Multiplier` | 2.0 | Backoff multiplier per attempt |
| `JitterMode` | `JitterFull` | Randomization: `JitterFull`, `JitterEqual`, or `JitterNone` |

## Custom Retry Logic

For application-level control beyond the built-in policy:

```go
func sendWithRetry(ctx context.Context, client *kubemq.Client, event *kubemq.Event, maxAttempts int) error {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lastErr = client.SendEvent(ctx, event)
		if lastErr == nil {
			return nil
		}

		var kErr *kubemq.KubeMQError
		if !errors.As(lastErr, &kErr) || !kErr.IsRetryable {
			return lastErr // non-retryable — stop immediately
		}

		backoff := time.Duration(attempt*attempt) * 100 * time.Millisecond
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("exhausted %d retries: %w", maxAttempts, lastErr)
}
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `retries exhausted: 3/3 attempts` | All retry attempts failed | Increase `MaxRetries` or investigate the underlying error |
| `retry throttled: concurrent retry limit reached` | Too many concurrent retries | Increase `WithMaxConcurrentRetries` or reduce request rate |
| `AUTHENTICATION` error marked non-retryable | Auth tokens don't fix themselves | Refresh the token, then retry manually |
| `VALIDATION` error on send | Bad request parameters | Fix the channel name, body, or metadata before retrying |
| `BACKPRESSURE` during reconnect | Reconnection buffer full | Wait for reconnection or increase buffer size |
