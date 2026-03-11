package kubemq

import (
	"github.com/kubemq-io/kubemq-go/v2/internal/types"
)

// ConnectionState represents the connection lifecycle state.
// Goroutine-safe. Value type; reads via client.State() are atomic.
// Defined in internal/types; re-exported here for public API stability.
type ConnectionState = types.ConnectionState

const (
	StateIdle         = types.StateIdle
	StateConnecting   = types.StateConnecting
	StateReady        = types.StateReady
	StateReconnecting = types.StateReconnecting
	StateClosed       = types.StateClosed
)

// ReconnectPolicy configures the automatic reconnection behavior.
// Immutable after construction. No goroutine-safety concerns.
// Defined in internal/types; re-exported here for public API stability.
type ReconnectPolicy = types.ReconnectPolicy

// DefaultReconnectPolicy returns the TYPE-REGISTRY-specified defaults.
func DefaultReconnectPolicy() ReconnectPolicy {
	return types.DefaultReconnectPolicy()
}

// StateCallbacks holds user-registered handlers for connection state transitions.
// All callbacks are invoked asynchronously on a dedicated goroutine.
type StateCallbacks struct {
	OnConnected    func() // CONNECTING → READY (initial connection)
	OnDisconnected func() // READY → RECONNECTING
	OnReconnecting func() // same transition as OnDisconnected (alias for clarity)
	OnReconnected  func() // RECONNECTING → READY
	OnClosed       func() // any → CLOSED
}
