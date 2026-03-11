package middleware

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetry_TransientSucceeds(t *testing.T) {
	attempts := 0
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		attempts++
		if attempts < 3 {
			return status.Error(codes.Unavailable, "server unavailable")
		}
		return nil
	}
	policy := types.DefaultRetryPolicy()
	policy.InitialBackoff = time.Millisecond
	policy.MaxBackoff = time.Millisecond
	policy.JitterMode = types.JitterNone
	ri := NewRetryInterceptor(policy, nil, 10)
	interceptor := ri.UnaryInterceptor()
	err := interceptor(context.Background(), "/kubemq.Kubemq/SendEvent", nil, nil, nil, invoker)
	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestRetry_NonRetryableImmediate(t *testing.T) {
	attempts := 0
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		attempts++
		return status.Error(codes.InvalidArgument, "bad request")
	}
	policy := types.DefaultRetryPolicy()
	policy.InitialBackoff = time.Millisecond
	ri := NewRetryInterceptor(policy, nil, 10)
	interceptor := ri.UnaryInterceptor()
	err := interceptor(context.Background(), "/kubemq.Kubemq/SendEvent", nil, nil, nil, invoker)
	assert.Error(t, err)
	assert.Equal(t, 1, attempts)
}

func TestRetry_DeadlineExceeded_NonIdempotent(t *testing.T) {
	attempts := 0
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		attempts++
		return status.Error(codes.DeadlineExceeded, "timeout")
	}
	policy := types.DefaultRetryPolicy()
	policy.InitialBackoff = time.Millisecond
	ri := NewRetryInterceptor(policy, nil, 10)
	interceptor := ri.UnaryInterceptor()
	err := interceptor(context.Background(), "/kubemq.Kubemq/SendQueueMessage", nil, nil, nil, invoker)
	assert.Error(t, err)
	assert.Equal(t, 1, attempts)
}

func TestRetry_Disabled(t *testing.T) {
	attempts := 0
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		attempts++
		return status.Error(codes.Unavailable, "server unavailable")
	}
	policy := types.RetryPolicy{MaxRetries: 0}
	ri := NewRetryInterceptor(policy, nil, 10)
	interceptor := ri.UnaryInterceptor()
	err := interceptor(context.Background(), "/kubemq.Kubemq/SendEvent", nil, nil, nil, invoker)
	assert.Error(t, err)
	assert.Equal(t, 1, attempts)
}

func TestRetry_UnknownMaxOnce(t *testing.T) {
	attempts := 0
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		attempts++
		return status.Error(codes.Unknown, "unknown error")
	}
	policy := types.DefaultRetryPolicy()
	policy.InitialBackoff = time.Millisecond
	policy.JitterMode = types.JitterNone
	ri := NewRetryInterceptor(policy, nil, 10)
	interceptor := ri.UnaryInterceptor()
	err := interceptor(context.Background(), "/kubemq.Kubemq/SendEvent", nil, nil, nil, invoker)
	assert.Error(t, err)
	assert.Equal(t, 2, attempts)
}

func TestRetry_ExhaustionMessage(t *testing.T) {
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		return status.Error(codes.Unavailable, "server unavailable")
	}
	policy := types.RetryPolicy{
		MaxRetries:     2,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
		Multiplier:     1.0,
		JitterMode:     types.JitterNone,
	}
	ri := NewRetryInterceptor(policy, nil, 10)
	interceptor := ri.UnaryInterceptor()
	err := interceptor(context.Background(), "/kubemq.Kubemq/SendEvent", nil, nil, nil, invoker)
	assert.Error(t, err)
	var kErr *types.KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "retries exhausted: 2/2")
}

func TestRetry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		attempts++
		if attempts == 1 {
			cancel()
		}
		return status.Error(codes.Unavailable, "server unavailable")
	}
	policy := types.RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2.0,
	}
	ri := NewRetryInterceptor(policy, nil, 10)
	interceptor := ri.UnaryInterceptor()
	err := interceptor(ctx, "/kubemq.Kubemq/SendEvent", nil, nil, nil, invoker)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestRetryThrottle_LimitReached(t *testing.T) {
	policy := types.RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     time.Second,
		Multiplier:     2.0,
	}
	ri := NewRetryInterceptor(policy, nil, 1)

	ri.sem <- struct{}{}

	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		return status.Error(codes.Unavailable, "unavailable")
	}
	interceptor := ri.UnaryInterceptor()
	err := interceptor(context.Background(), "/kubemq.Kubemq/SendEvent", nil, nil, nil, invoker)
	var kErr *types.KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, types.ErrCodeThrottling, kErr.Code)
	assert.Contains(t, kErr.Message, "retry throttled")

	<-ri.sem
}

func TestRetry_Name(t *testing.T) {
	ri := NewRetryInterceptor(types.DefaultRetryPolicy(), nil, 10)
	assert.Equal(t, "retry", ri.Name())
}
