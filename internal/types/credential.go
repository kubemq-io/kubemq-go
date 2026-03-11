package types

import (
	"context"
	"time"
)

// CredentialProvider supplies authentication tokens for KubeMQ connections.
// Implementations must be safe for concurrent use.
//
// The SDK guarantees serialized invocation: at most one GetToken() call
// is outstanding at any time. Implementations do not need internal locking
// for the GetToken method itself.
//
// expiresAt: When non-zero, the SDK uses this hint for proactive refresh
// scheduling. When zero, the SDK only refreshes reactively (on UNAUTHENTICATED).
type CredentialProvider interface {
	GetToken(ctx context.Context) (token string, expiresAt time.Time, err error)
}

// StaticTokenProvider implements CredentialProvider with a fixed token
// that never expires.
type StaticTokenProvider struct {
	token string
}

// NewStaticTokenProvider returns a CredentialProvider that always returns
// the given token with zero expiry.
func NewStaticTokenProvider(token string) *StaticTokenProvider {
	return &StaticTokenProvider{token: token}
}

func (p *StaticTokenProvider) GetToken(_ context.Context) (string, time.Time, error) {
	return p.token, time.Time{}, nil
}
