package kubemq

import (
	"time"
)

// CallbackConfig configures subscription callback dispatch behavior.
// Immutable after client construction.
type CallbackConfig struct {
	// MaxConcurrent is the maximum number of callbacks executing concurrently.
	// Default: 1 (sequential processing). Set to higher values for parallel
	// message processing. Each subscription gets its own concurrency limit.
	//
	// When MaxConcurrent > 1, callbacks MAY execute concurrently and MAY
	// be delivered out of order. Use MaxConcurrent=1 for ordered processing.
	MaxConcurrent int

	// Timeout is the maximum time to wait for in-flight callbacks to complete
	// during Close(). Default: 30s.
	// This is separate from the operation drain timeout (5s, spec 02 REQ-CONN-4).
	Timeout time.Duration
}

// DefaultCallbackConfig returns the golden-standard defaults:
// sequential processing (MaxConcurrent=1), 30s shutdown timeout.
func DefaultCallbackConfig() CallbackConfig {
	return CallbackConfig{
		MaxConcurrent: 1,
		Timeout:       30 * time.Second,
	}
}

// WithMaxConcurrentCallbacks sets the maximum number of subscription callbacks
// that may execute concurrently per subscription. Default: 1 (sequential).
//
// When set to 1 (default), callbacks are delivered sequentially in order.
// When set to N > 1, up to N callbacks execute concurrently. Message ordering
// is NOT guaranteed with concurrent callbacks.
func WithMaxConcurrentCallbacks(n int) Option {
	return newFuncOption(func(o *Options) {
		if n < 1 {
			n = 1
		}
		o.callbackConfig.MaxConcurrent = n
	})
}

// WithCallbackTimeout sets the maximum time Close() waits for in-flight
// callbacks to complete before forcibly shutting down. Default: 30s.
//
// This timeout is separate from the operation drain timeout (5s).
// The callback timeout covers subscription message handlers;
// the drain timeout covers in-flight RPC operations.
func WithCallbackTimeout(d time.Duration) Option {
	return newFuncOption(func(o *Options) {
		o.callbackConfig.Timeout = d
	})
}
