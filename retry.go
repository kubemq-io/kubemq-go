package kubemq

import (
	"github.com/kubemq-io/kubemq-go/v2/internal/types"
)

// RetryPolicy configures automatic retry behavior for transient errors.
type RetryPolicy = types.RetryPolicy

// Logger is the structured logging interface for the SDK.
// Defined in TYPE-REGISTRY.md, owned by 05-observability-spec.md.
type Logger = types.Logger

// JitterMode controls how jitter is applied to retry backoff delays.
type JitterMode = types.JitterMode

const (
	JitterNone  = types.JitterNone
	JitterFull  = types.JitterFull
	JitterEqual = types.JitterEqual
)

// DefaultRetryPolicy returns a RetryPolicy with sensible defaults:
// MaxRetries=3, InitialBackoff=100ms, MaxBackoff=10s, Multiplier=2.0, JitterMode=JitterFull.
func DefaultRetryPolicy() RetryPolicy {
	return types.DefaultRetryPolicy()
}
