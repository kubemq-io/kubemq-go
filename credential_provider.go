package kubemq

import (
	"github.com/kubemq-io/kubemq-go/v2/internal/types"
)

// CredentialProvider supplies authentication tokens for KubeMQ connections.
// Re-exported from internal/types for public API stability.
type CredentialProvider = types.CredentialProvider

// StaticTokenProvider implements CredentialProvider with a fixed token.
// Re-exported from internal/types for public API stability.
type StaticTokenProvider = types.StaticTokenProvider

// NewStaticTokenProvider returns a CredentialProvider that always returns
// the given token with zero expiry (never expires, no proactive refresh).
func NewStaticTokenProvider(token string) *StaticTokenProvider {
	return types.NewStaticTokenProvider(token)
}
