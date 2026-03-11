// Package types provides shared type definitions used by both the root
// kubemq package and internal packages. It exists to break import cycles
// between the public API layer and the transport layer.
package types

// ConnectionState represents the state of a client connection.
type ConnectionState int

const (
	// StateIdle indicates the connection has not been started.
	StateIdle ConnectionState = iota
	// StateConnecting indicates a connection attempt is in progress.
	StateConnecting
	// StateReady indicates the connection is established and ready for use.
	StateReady
	// StateReconnecting indicates the connection was lost and a reconnection attempt is in progress.
	StateReconnecting
	// StateClosed indicates the connection has been permanently closed.
	StateClosed
)

// String returns a human-readable representation of the ConnectionState.
func (s ConnectionState) String() string {
	switch s {
	case StateIdle:
		return "IDLE"
	case StateConnecting:
		return "CONNECTING"
	case StateReady:
		return "READY"
	case StateReconnecting:
		return "RECONNECTING"
	case StateClosed:
		return "CLOSED"
	default:
		return "UNKNOWN"
	}
}
