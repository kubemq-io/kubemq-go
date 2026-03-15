package middleware

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func containsAny(s string, substrs ...string) bool {
	lower := strings.ToLower(s)
	for _, sub := range substrs {
		if strings.Contains(lower, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

// tokenCache holds a cached token with serialized refresh via mutex.
type tokenCache struct {
	mu        sync.Mutex
	provider  types.CredentialProvider
	token     string
	expiresAt time.Time
	valid     bool
	logger    types.Logger
	onRefresh func(expiresAt time.Time)
}

func newTokenCache(provider types.CredentialProvider, logger types.Logger) *tokenCache {
	return &tokenCache{
		provider: provider,
		logger:   logger,
	}
}

// getToken returns a cached token or fetches a new one from the provider.
// Calls to the provider are serialized by the mutex.
func (tc *tokenCache) getToken(ctx context.Context) (string, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.valid && (tc.expiresAt.IsZero() || time.Now().Before(tc.expiresAt)) {
		return tc.token, nil
	}

	token, expiresAt, err := tc.provider.GetToken(ctx)
	if err != nil {
		return "", tc.classifyProviderError(err)
	}

	tc.token = token
	tc.expiresAt = expiresAt
	tc.valid = true

	tc.logger.Debug("auth token refreshed", "token_present", true, "expires_at_set", !expiresAt.IsZero())

	if tc.onRefresh != nil && !expiresAt.IsZero() {
		tc.onRefresh(expiresAt)
	}

	return token, nil
}

// invalidate marks the cached token as invalid, forcing re-fetch on next getToken.
func (tc *tokenCache) invalidate() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.valid = false
	tc.logger.Debug("auth token invalidated", "token_present", false)
}

func (tc *tokenCache) classifyProviderError(err error) error {
	var kubemqErr *types.KubeMQError
	if errors.As(err, &kubemqErr) {
		return kubemqErr
	}

	msg := err.Error()
	if containsAny(msg, "invalid", "expired", "unauthorized", "forbidden", "denied") {
		return &types.KubeMQError{
			Code:        types.ErrCodeAuthentication,
			Message:     "credential provider returned authentication error",
			IsRetryable: false,
			Cause:       err,
		}
	}

	return &types.KubeMQError{
		Code:        types.ErrCodeTransient,
		Message:     "credential provider returned transient error",
		IsRetryable: true,
		Cause:       err,
	}
}

// authInterceptor injects auth tokens into gRPC metadata and handles
// reactive/proactive token refresh.
type authInterceptor struct {
	cache        *tokenCache
	refreshTimer *time.Timer
	stopRefresh  context.CancelFunc
	logger       types.Logger
}

// NewAuthInterceptor creates an auth interceptor with token caching and
// proactive refresh. The interceptor's lifecycle is tied to clientCtx.
func NewAuthInterceptor(clientCtx context.Context, provider types.CredentialProvider, logger types.Logger) *authInterceptor {
	ai := &authInterceptor{
		cache:  newTokenCache(provider, logger),
		logger: logger,
	}
	refreshCtx, cancel := context.WithCancel(clientCtx)
	ai.stopRefresh = cancel
	ai.refreshTimer = time.NewTimer(time.Duration(math.MaxInt64))
	ai.refreshTimer.Stop()

	ai.cache.onRefresh = ai.scheduleProactiveRefresh

	go ai.proactiveRefreshLoop(refreshCtx)
	return ai
}

func (*authInterceptor) Name() string { return "auth" }

// UnaryInterceptor returns a gRPC unary interceptor that injects the auth
// token and handles reactive refresh on UNAUTHENTICATED.
func (a *authInterceptor) UnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		token, err := a.cache.getToken(ctx)
		if err != nil {
			return err
		}

		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", token)
		err = invoker(ctx, method, req, reply, cc, opts...)
		if err == nil {
			return nil
		}

		var kErr *types.KubeMQError
		if errors.As(err, &kErr) && kErr.Code == types.ErrCodeAuthentication {
			return a.handleUnauthenticated(ctx, method, req, reply, cc, invoker, opts...)
		}

		return err
	}
}

// StreamInterceptor returns a gRPC stream interceptor that injects the auth
// token and handles reactive refresh on UNAUTHENTICATED.
func (a *authInterceptor) StreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
		method string, streamer grpc.Streamer, opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		token, err := a.cache.getToken(ctx)
		if err != nil {
			return nil, err
		}

		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", token)
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			var kErr *types.KubeMQError
			if errors.As(err, &kErr) && kErr.Code == types.ErrCodeAuthentication {
				a.cache.invalidate()
				token, refreshErr := a.cache.getToken(ctx)
				if refreshErr != nil {
					return nil, refreshErr
				}
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", token)
				return streamer(ctx, desc, cc, method, opts...)
			}
			return nil, err
		}
		return stream, nil
	}
}

func (a *authInterceptor) handleUnauthenticated(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
) error {
	a.cache.invalidate()

	token, err := a.cache.getToken(ctx)
	if err != nil {
		return err
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", token)
	retryErr := invoker(ctx, method, req, reply, cc, opts...)
	if retryErr != nil {
		var kErr *types.KubeMQError
		if errors.As(retryErr, &kErr) && kErr.Code == types.ErrCodeAuthentication {
			return &types.KubeMQError{
				Code:        types.ErrCodeAuthentication,
				Message:     "authentication failed after token refresh",
				Operation:   method,
				IsRetryable: false,
				Cause:       retryErr,
			}
		}
		return retryErr
	}
	return nil
}

func (a *authInterceptor) scheduleProactiveRefresh(expiresAt time.Time) {
	if expiresAt.IsZero() {
		return
	}

	lifetime := time.Until(expiresAt)
	if lifetime <= 0 {
		return
	}

	window := lifetime / 10
	if window > 30*time.Second {
		window = 30 * time.Second
	}
	refreshAt := expiresAt.Add(-window)

	a.refreshTimer.Reset(time.Until(refreshAt))
}

func (a *authInterceptor) proactiveRefreshLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			a.refreshTimer.Stop()
			return
		case <-a.refreshTimer.C:
			a.cache.invalidate()
			_, err := a.cache.getToken(ctx)
			if err != nil {
				a.logger.Error("proactive token refresh failed", "error", err)
				a.refreshTimer.Reset(5 * time.Second)
				continue
			}
		}
	}
}

// Close stops the proactive refresh goroutine.
func (a *authInterceptor) Close() {
	if a.stopRefresh != nil {
		a.stopRefresh()
	}
}
