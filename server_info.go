package kubemq

import "fmt"

// ServerInfo contains information about a KubeMQ server, returned by Ping.
// Immutable after construction. Safe to read from multiple goroutines.
type ServerInfo struct {
	Host                string
	Version             string
	ServerStartTime     int64
	ServerUpTimeSeconds int64
}

// String returns a human-readable representation of the server info.
func (s *ServerInfo) String() string {
	return fmt.Sprintf("Host: %s, Version: %s, StartTime: %d, UpTime: %ds",
		s.Host, s.Version, s.ServerStartTime, s.ServerUpTimeSeconds)
}
