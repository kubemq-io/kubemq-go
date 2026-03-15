package middleware

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type retryInterceptor struct {
	policy types.RetryPolicy
	logger types.Logger
	sem    chan struct{}
}

// NewRetryInterceptor creates a gRPC unary interceptor that retries transient
// and timeout errors with configurable exponential backoff + jitter.
func NewRetryInterceptor(policy types.RetryPolicy, logger types.Logger, maxConcurrent int) *retryInterceptor {
	ri := &retryInterceptor{
		policy: policy,
		logger: logger,
	}
	if maxConcurrent > 0 {
		ri.sem = make(chan struct{}, maxConcurrent)
	}
	return ri
}

func (ri *retryInterceptor) Name() string { return "retry" }

func (ri *retryInterceptor) UnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if ri.policy.MaxRetries == 0 {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		if ri.sem != nil {
			select {
			case ri.sem <- struct{}{}:
				defer func() { <-ri.sem }()
			default:
				return &types.KubeMQError{
					Code:        types.ErrCodeThrottling,
					Message:     "retry throttled: concurrent retry limit reached",
					Operation:   method,
					IsRetryable: false,
				}
			}
		}

		var lastErr error
		start := time.Now()

		for attempt := 0; attempt <= ri.policy.MaxRetries; attempt++ {
			if attempt > 0 {
				delay := ri.backoff(attempt)
				if ri.logger != nil {
					ri.logger.Debug("retrying operation",
						"method", method,
						"attempt", attempt,
						"delay", delay,
						"last_error", lastErr,
					)
				}

				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}

			lastErr = invoker(ctx, method, req, reply, cc, opts...)
			if lastErr == nil {
				return nil
			}

			st, ok := status.FromError(lastErr)
			if !ok {
				return lastErr
			}

			cat, _ := types.ClassifyGRPCCode(st.Code())
			if !types.IsRetryableCategory(cat) {
				return lastErr
			}

			if st.Code() == codes.DeadlineExceeded && !isIdempotent(method) {
				return lastErr
			}

			if st.Code() == codes.Unknown && attempt >= 1 {
				break
			}
		}

		return &types.KubeMQError{
			Code:        types.ErrCodeTransient,
			Message:     fmt.Sprintf("retries exhausted: %d/%d attempts over %s", ri.policy.MaxRetries, ri.policy.MaxRetries, time.Since(start).Truncate(time.Millisecond)),
			Operation:   method,
			IsRetryable: false,
			Cause:       lastErr,
		}
	}
}

func (ri *retryInterceptor) backoff(attempt int) time.Duration {
	delay := float64(ri.policy.InitialBackoff) * math.Pow(ri.policy.Multiplier, float64(attempt-1))
	if delay > float64(ri.policy.MaxBackoff) {
		delay = float64(ri.policy.MaxBackoff)
	}
	switch ri.policy.JitterMode {
	case types.JitterFull:
		delay = rand.Float64() * delay //nolint:gosec // G404: jitter for backoff does not need cryptographic randomness
	case types.JitterEqual:
		delay = delay/2 + rand.Float64()*(delay/2) //nolint:gosec // G404: jitter for backoff does not need cryptographic randomness
	case types.JitterNone:
		// deterministic
	}
	return time.Duration(delay)
}

func isIdempotent(method string) bool {
	switch method {
	case "/kubemq.Kubemq/SendEvent":
		return true
	case "/kubemq.Kubemq/SendRequest":
		return false
	case "/kubemq.Kubemq/SendQueueMessage",
		"/kubemq.Kubemq/SendQueueMessagesBatch":
		return false
	default:
		return true
	}
}
