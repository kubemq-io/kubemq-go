package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionStateString(t *testing.T) {
	tests := []struct {
		state    ConnectionState
		expected string
	}{
		{StateIdle, "IDLE"},
		{StateConnecting, "CONNECTING"},
		{StateReady, "READY"},
		{StateReconnecting, "RECONNECTING"},
		{StateClosed, "CLOSED"},
		{ConnectionState(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestJitterModeString(t *testing.T) {
	tests := []struct {
		mode     JitterMode
		expected string
	}{
		{JitterNone, "NONE"},
		{JitterFull, "FULL"},
		{JitterEqual, "EQUAL"},
		{JitterMode(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mode.String())
		})
	}
}
