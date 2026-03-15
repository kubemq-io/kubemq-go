package middleware

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type authMockClientStream struct {
	ctx context.Context
}

func (m *authMockClientStream) Header() (metadata.MD, error) { return nil, nil }
func (m *authMockClientStream) Trailer() metadata.MD         { return nil }
func (m *authMockClientStream) CloseSend() error             { return nil }
func (m *authMockClientStream) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}
func (m *authMockClientStream) SendMsg(any) error { return nil }
func (m *authMockClientStream) RecvMsg(any) error { return nil }

func TestAuthInterceptor_StreamInterceptor_Success(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &mockProvider{token: "stream-success-token"}
	ai := NewAuthInterceptor(ctx, provider, log)
	defer ai.Close()

	interceptor := ai.StreamInterceptor()

	var capturedMD metadata.MD
	mockStreamer := func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
		md, ok := metadata.FromOutgoingContext(ctx)
		if ok {
			capturedMD = md
		}
		return &authMockClientStream{ctx: ctx}, nil
	}

	stream, err := interceptor(context.Background(), &grpc.StreamDesc{}, nil, "/test.Stream", mockStreamer)
	require.NoError(t, err)
	assert.NotNil(t, stream, "should return a non-nil stream on success")
	assert.Contains(t, capturedMD["authorization"], "stream-success-token")
}

func TestAuthInterceptor_StreamInterceptor_AuthError_Refresh(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callCount := atomic.Int64{}
	provider := &mockProvider{token: "old-stream-token"}
	ai := NewAuthInterceptor(ctx, provider, log)
	defer ai.Close()

	interceptor := ai.StreamInterceptor()

	var retryMD metadata.MD
	mockStreamer := func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
		n := callCount.Add(1)
		if n == 1 {
			return nil, &types.KubeMQError{Code: types.ErrCodeAuthentication, Message: "unauthenticated"}
		}
		md, ok := metadata.FromOutgoingContext(ctx)
		if ok {
			retryMD = md
		}
		return &authMockClientStream{ctx: ctx}, nil
	}

	provider.setToken("refreshed-stream-token")

	stream, err := interceptor(context.Background(), &grpc.StreamDesc{}, nil, "/test.Stream", mockStreamer)
	require.NoError(t, err)
	assert.NotNil(t, stream)
	assert.Equal(t, int64(2), callCount.Load(), "should retry after auth error and token refresh")
	assert.Contains(t, retryMD["authorization"], "refreshed-stream-token")
}

func TestAuthInterceptor_StreamInterceptor_NonAuthError(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &mockProvider{token: "token"}
	ai := NewAuthInterceptor(ctx, provider, log)
	defer ai.Close()

	interceptor := ai.StreamInterceptor()

	expectedErr := &types.KubeMQError{Code: types.ErrCodeTransient, Message: "server unavailable"}
	mockStreamer := func(_ context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
		return nil, expectedErr
	}

	stream, err := interceptor(context.Background(), &grpc.StreamDesc{}, nil, "/test.Stream", mockStreamer)
	assert.Nil(t, stream)
	require.Error(t, err)

	var kErr *types.KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, types.ErrCodeTransient, kErr.Code, "non-auth error should pass through without retry")
}

func TestAuthInterceptor_HandleUnauthenticated_RetryNonAuthError(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callCount := atomic.Int64{}
	provider := &mockProvider{token: "token"}
	ai := NewAuthInterceptor(ctx, provider, log)
	defer ai.Close()

	interceptor := ai.UnaryInterceptor()

	mockInvoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		n := callCount.Add(1)
		if n == 1 {
			return &types.KubeMQError{Code: types.ErrCodeAuthentication, Message: "unauthenticated"}
		}
		return &types.KubeMQError{Code: types.ErrCodeTransient, Message: "server busy"}
	}

	err := interceptor(context.Background(), "/test.Method", nil, nil, nil, mockInvoker)
	require.Error(t, err)

	var kErr *types.KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, types.ErrCodeTransient, kErr.Code, "non-auth retry error should be returned as-is")
	assert.Equal(t, "server busy", kErr.Message)
	assert.Equal(t, int64(2), callCount.Load())
}

func TestScheduleProactiveRefresh_ZeroExpiry(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &mockProvider{token: "t"}
	ai := NewAuthInterceptor(ctx, provider, log)
	defer ai.Close()

	ai.scheduleProactiveRefresh(time.Time{})
}

func TestScheduleProactiveRefresh_ExpiredToken(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &mockProvider{token: "t"}
	ai := NewAuthInterceptor(ctx, provider, log)
	defer ai.Close()

	ai.scheduleProactiveRefresh(time.Now().Add(-1 * time.Hour))
}

func TestScheduleProactiveRefresh_LargeWindow(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &mockProvider{token: "t"}
	ai := NewAuthInterceptor(ctx, provider, log)
	defer ai.Close()

	// lifetime = 600s → window = 60s → clamped to 30s
	ai.scheduleProactiveRefresh(time.Now().Add(10 * time.Minute))
}

func TestProactiveRefreshLoop_TokenRefreshError(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &mockProvider{
		token:     "initial-token",
		expiresAt: time.Now().Add(100 * time.Millisecond),
	}
	ai := NewAuthInterceptor(ctx, provider, log)
	defer ai.Close()

	interceptor := ai.UnaryInterceptor()
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		return nil
	}
	err := interceptor(context.Background(), "/test.Method", nil, nil, nil, invoker)
	require.NoError(t, err)

	provider.mu.Lock()
	provider.err = errors.New("token refresh failed")
	provider.mu.Unlock()

	// Wait for proactive refresh to fire (~90ms) and fail
	time.Sleep(300 * time.Millisecond)

	log.mu.Lock()
	hasError := false
	for _, msg := range log.errorMsgs {
		if msg == "proactive token refresh failed" {
			hasError = true
			break
		}
	}
	log.mu.Unlock()
	assert.True(t, hasError, "expected proactive refresh error to be logged")
}
