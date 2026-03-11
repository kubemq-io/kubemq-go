package transport

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// KeepaliveConfig holds the gRPC keepalive parameters.
type KeepaliveConfig struct {
	Time                time.Duration // Keepalive ping interval. Default: 10s
	Timeout             time.Duration // Ping response timeout. Default: 5s
	PermitWithoutStream bool          // Send pings even without active streams. Default: true
}

// DefaultKeepaliveConfig returns the GS-specified defaults.
// Detection time: Time + Timeout = 15s.
func DefaultKeepaliveConfig() KeepaliveConfig {
	return KeepaliveConfig{
		Time:                10 * time.Second,
		Timeout:             5 * time.Second,
		PermitWithoutStream: true,
	}
}

// dialOption converts the config to a gRPC dial option.
func (kc KeepaliveConfig) dialOption() grpc.DialOption {
	return grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                kc.Time,
		Timeout:             kc.Timeout,
		PermitWithoutStream: kc.PermitWithoutStream,
	})
}
