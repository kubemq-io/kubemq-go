package middleware

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type testLogger struct {
	debugMsgs []string
	errorMsgs []string
	mu        sync.Mutex
}

func (l *testLogger) Debug(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.debugMsgs = append(l.debugMsgs, msg)
}
func (l *testLogger) Info(msg string, _ ...any) {}
func (l *testLogger) Warn(msg string, _ ...any) {}
func (l *testLogger) Error(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errorMsgs = append(l.errorMsgs, msg)
}

type mockProvider struct {
	mu        sync.RWMutex
	token     string
	expiresAt time.Time
	err       error
	calls     atomic.Int64
}

func (p *mockProvider) setToken(t string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.token = t
}

func (p *mockProvider) GetToken(_ context.Context) (string, time.Time, error) {
	p.calls.Add(1)
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.token, p.expiresAt, p.err
}

func TestStaticTokenProvider(t *testing.T) {
	p := types.NewStaticTokenProvider("test-token-123")
	token, expiresAt, err := p.GetToken(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "test-token-123", token)
	assert.True(t, expiresAt.IsZero(), "static provider should return zero expiresAt")
}

func TestTokenCache_GetToken_CachesResult(t *testing.T) {
	log := &testLogger{}
	provider := &mockProvider{token: "cached-token"}
	tc := newTokenCache(provider, log)

	token1, err := tc.getToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "cached-token", token1)

	token2, err := tc.getToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "cached-token", token2)

	assert.Equal(t, int64(1), provider.calls.Load(), "provider should be called only once for cached token")
}

func TestTokenCache_Invalidation(t *testing.T) {
	log := &testLogger{}
	provider := &mockProvider{token: "token-v1"}
	tc := newTokenCache(provider, log)

	_, err := tc.getToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(1), provider.calls.Load())

	tc.invalidate()

	provider.setToken("token-v2")
	token, err := tc.getToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "token-v2", token)
	assert.Equal(t, int64(2), provider.calls.Load())
}

func TestTokenCache_Serialization(t *testing.T) {
	log := &testLogger{}
	provider := &mockProvider{token: "concurrent-token"}
	tc := newTokenCache(provider, log)
	tc.invalidate()

	var wg sync.WaitGroup
	const goroutines = 20

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, err := tc.getToken(context.Background())
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	calls := provider.calls.Load()
	assert.LessOrEqual(t, calls, int64(goroutines), "should not call provider more than goroutine count")
	assert.GreaterOrEqual(t, calls, int64(1), "should call provider at least once")
}

func TestTokenCache_ExpiredToken_RefreshesAutomatically(t *testing.T) {
	log := &testLogger{}
	provider := &mockProvider{
		token:     "expiring-token",
		expiresAt: time.Now().Add(-1 * time.Second),
	}
	tc := newTokenCache(provider, log)

	tc.token = "old"
	tc.expiresAt = time.Now().Add(-1 * time.Second)
	tc.valid = true

	token, err := tc.getToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "expiring-token", token)
	assert.Equal(t, int64(1), provider.calls.Load())
}

func TestTokenCache_ProviderError_Classification(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantCode  types.ErrorCode
		wantRetry bool
	}{
		{
			name:      "auth error with unauthorized keyword",
			err:       errors.New("token is unauthorized"),
			wantCode:  types.ErrCodeAuthentication,
			wantRetry: false,
		},
		{
			name:      "auth error with expired keyword",
			err:       errors.New("token has expired"),
			wantCode:  types.ErrCodeAuthentication,
			wantRetry: false,
		},
		{
			name:      "transient network error",
			err:       errors.New("connection refused"),
			wantCode:  types.ErrCodeTransient,
			wantRetry: true,
		},
		{
			name:      "pre-classified KubeMQError passes through",
			err:       &types.KubeMQError{Code: types.ErrCodeValidation, Message: "bad input"},
			wantCode:  types.ErrCodeValidation,
			wantRetry: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &testLogger{}
			provider := &mockProvider{err: tt.err}
			tc := newTokenCache(provider, log)

			_, err := tc.getToken(context.Background())
			require.Error(t, err)

			var kErr *types.KubeMQError
			require.True(t, errors.As(err, &kErr))
			assert.Equal(t, tt.wantCode, kErr.Code)
			assert.Equal(t, tt.wantRetry, kErr.IsRetryable)
		})
	}
}

func TestAuthInterceptor_InjectToken(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &mockProvider{token: "injected-token"}
	ai := NewAuthInterceptor(provider, log, ctx)
	defer ai.Close()

	interceptor := ai.UnaryInterceptor()

	var capturedMD metadata.MD
	mockInvoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		md, ok := metadata.FromOutgoingContext(ctx)
		if ok {
			capturedMD = md
		}
		return nil
	}

	err := interceptor(context.Background(), "/test.Method", nil, nil, nil, mockInvoker)
	require.NoError(t, err)
	assert.Contains(t, capturedMD["authorization"], "injected-token")
}

func TestAuthInterceptor_ReactiveRefresh(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callCount := atomic.Int64{}
	provider := &mockProvider{token: "old-token"}
	ai := NewAuthInterceptor(provider, log, ctx)
	defer ai.Close()

	interceptor := ai.UnaryInterceptor()

	mockInvoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		n := callCount.Add(1)
		if n == 1 {
			return &types.KubeMQError{Code: types.ErrCodeAuthentication, Message: "unauthenticated"}
		}
		return nil
	}

	provider.setToken("refreshed-token")

	err := interceptor(context.Background(), "/test.Method", nil, nil, nil, mockInvoker)
	require.NoError(t, err)
	assert.Equal(t, int64(2), callCount.Load(), "should retry after refresh")
}

func TestAuthInterceptor_ReactiveRefresh_FailsAfterRetry(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &mockProvider{token: "bad-token"}
	ai := NewAuthInterceptor(provider, log, ctx)
	defer ai.Close()

	interceptor := ai.UnaryInterceptor()

	mockInvoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		return &types.KubeMQError{Code: types.ErrCodeAuthentication, Message: "unauthenticated"}
	}

	err := interceptor(context.Background(), "/test.Method", nil, nil, nil, mockInvoker)
	require.Error(t, err)

	var kErr *types.KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, types.ErrCodeAuthentication, kErr.Code)
	assert.False(t, kErr.IsRetryable)
	assert.Equal(t, "authentication failed after token refresh", kErr.Message)
}

func TestAuthInterceptor_StreamInjectToken(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &mockProvider{token: "stream-token"}
	ai := NewAuthInterceptor(provider, log, ctx)
	defer ai.Close()

	interceptor := ai.StreamInterceptor()

	var capturedMD metadata.MD
	mockStreamer := func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
		md, ok := metadata.FromOutgoingContext(ctx)
		if ok {
			capturedMD = md
		}
		return nil, nil
	}

	_, err := interceptor(context.Background(), &grpc.StreamDesc{}, nil, "/test.Stream", mockStreamer)
	require.NoError(t, err)
	assert.Contains(t, capturedMD["authorization"], "stream-token")
}

func TestAuthInterceptor_ProactiveRefresh(t *testing.T) {
	log := &testLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &mockProvider{
		token:     "short-lived",
		expiresAt: time.Now().Add(200 * time.Millisecond),
	}
	ai := NewAuthInterceptor(provider, log, ctx)
	defer ai.Close()

	interceptor := ai.UnaryInterceptor()
	mockInvoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		return nil
	}
	err := interceptor(context.Background(), "/test.Method", nil, nil, nil, mockInvoker)
	require.NoError(t, err)

	provider.setToken("refreshed-proactively")
	time.Sleep(400 * time.Millisecond)

	assert.GreaterOrEqual(t, provider.calls.Load(), int64(2),
		"provider should be called again for proactive refresh")
}

func TestContainsAny(t *testing.T) {
	assert.True(t, containsAny("Token is Invalid", "invalid"))
	assert.True(t, containsAny("request was DENIED", "denied"))
	assert.False(t, containsAny("connection timeout", "invalid", "expired"))
}
