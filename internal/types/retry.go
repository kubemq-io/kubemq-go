package types

import "time"

// RetryPolicy configures automatic retry behavior for transient errors.
// Immutable after client construction — create a new client to change policy.
type RetryPolicy struct {
	MaxRetries     int           // 0 = no retries. Default: 3. Range: 0-10.
	InitialBackoff time.Duration // Default: 100ms. Range: 50ms-5s.
	MaxBackoff     time.Duration // Default: 10s. Range: 1s-120s.
	Multiplier     float64       // Default: 2.0. Range: 1.5-3.0.
	JitterMode     JitterMode    // Default: JitterFull.
}

// DefaultRetryPolicy returns a RetryPolicy with sensible defaults.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2.0,
		JitterMode:     JitterFull,
	}
}
