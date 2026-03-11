package middleware

import (
	"context"
	"errors"
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrmapInterceptor_Name(t *testing.T) {
	m := NewErrmapInterceptor()
	assert.Equal(t, "errmap", m.Name())
}

func TestErrmapInterceptor_All17Codes(t *testing.T) {
	mapper := NewErrmapInterceptor()
	for code := codes.OK; code <= codes.Unauthenticated; code++ {
		if code == codes.OK {
			continue
		}
		t.Run(code.String(), func(t *testing.T) {
			grpcErr := status.Error(code, "test error")
			result := mapper.mapError(context.Background(), "TestMethod", grpcErr)
			var kErr *types.KubeMQError
			require.True(t, errors.As(result, &kErr))
			assert.NotEmpty(t, kErr.Code)
			assert.Equal(t, "TestMethod", kErr.Operation)
			assert.ErrorIs(t, kErr, grpcErr)
		})
	}
}

func TestErrmapInterceptor_CancelledClientInitiated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mapper := NewErrmapInterceptor()
	grpcErr := status.Error(codes.Canceled, "cancelled")
	result := mapper.mapError(ctx, "Subscribe", grpcErr)
	var kErr *types.KubeMQError
	require.True(t, errors.As(result, &kErr))
	assert.Equal(t, types.ErrCodeCancellation, kErr.Code)
	assert.False(t, kErr.IsRetryable)
}

func TestErrmapInterceptor_CancelledServerInitiated(t *testing.T) {
	ctx := context.Background()
	mapper := NewErrmapInterceptor()
	grpcErr := status.Error(codes.Canceled, "server reset")
	result := mapper.mapError(ctx, "Subscribe", grpcErr)
	var kErr *types.KubeMQError
	require.True(t, errors.As(result, &kErr))
	assert.Equal(t, types.ErrCodeTransient, kErr.Code)
	assert.True(t, kErr.IsRetryable)
}

func TestErrmapInterceptor_OriginalPreserved(t *testing.T) {
	mapper := NewErrmapInterceptor()
	grpcErr := status.Error(codes.Unavailable, "connection refused")
	result := mapper.mapError(context.Background(), "SendEvent", grpcErr)
	assert.ErrorIs(t, result, grpcErr)
}

func TestErrmapInterceptor_NonGRPCError(t *testing.T) {
	mapper := NewErrmapInterceptor()
	plainErr := errors.New("some random error")
	result := mapper.mapError(context.Background(), "SendEvent", plainErr)
	var kErr *types.KubeMQError
	require.True(t, errors.As(result, &kErr))
	assert.Equal(t, types.ErrCodeFatal, kErr.Code)
	assert.False(t, kErr.IsRetryable)
}

func TestErrmapInterceptor_PassthroughKubeMQError(t *testing.T) {
	mapper := NewErrmapInterceptor()
	existing := &types.KubeMQError{
		Code:    types.ErrCodeTransient,
		Message: "retries exhausted",
	}
	result := mapper.mapError(context.Background(), "SendEvent", existing)
	assert.Equal(t, existing, result)
}
