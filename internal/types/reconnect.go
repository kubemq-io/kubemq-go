package types

import "time"

// ReconnectPolicy configures the automatic reconnection behavior.
// Defined in TYPE-REGISTRY.md, owned by 02-connection-transport-spec.md.
type ReconnectPolicy struct {
	MaxAttempts  int           // 0 = unlimited
	InitialDelay time.Duration // Default: 1s
	MaxDelay     time.Duration // Default: 30s
	Multiplier   float64       // Default: 2.0
	JitterMode   JitterMode    // Default: JitterFull
	BufferSize   int           // Messages to buffer during reconnect. Default: 1000
}

// DefaultReconnectPolicy returns the TYPE-REGISTRY-specified defaults.
func DefaultReconnectPolicy() ReconnectPolicy {
	return ReconnectPolicy{
		MaxAttempts:  0,
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
		JitterMode:   JitterFull,
		BufferSize:   1000,
	}
}
