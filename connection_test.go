package kubemq

import (
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestConnectionStateReExports(t *testing.T) {
	assert.Equal(t, types.StateIdle, StateIdle)
	assert.Equal(t, types.StateConnecting, StateConnecting)
	assert.Equal(t, types.StateReady, StateReady)
	assert.Equal(t, types.StateReconnecting, StateReconnecting)
	assert.Equal(t, types.StateClosed, StateClosed)
}

func TestConnectionStateString(t *testing.T) {
	assert.Equal(t, "IDLE", StateIdle.String())
	assert.Equal(t, "CONNECTING", StateConnecting.String())
	assert.Equal(t, "READY", StateReady.String())
	assert.Equal(t, "RECONNECTING", StateReconnecting.String())
	assert.Equal(t, "CLOSED", StateClosed.String())
}

func TestDefaultReconnectPolicy(t *testing.T) {
	p := DefaultReconnectPolicy()
	assert.Equal(t, 0, p.MaxAttempts)
	assert.Equal(t, 1*time.Second, p.InitialDelay)
	assert.Equal(t, 30*time.Second, p.MaxDelay)
	assert.Equal(t, 2.0, p.Multiplier)
	assert.Equal(t, JitterFull, p.JitterMode)
	assert.Equal(t, 1000, p.BufferSize)
}

func TestReconnectPolicyTypeAlias(t *testing.T) {
	p := types.DefaultReconnectPolicy()
	assert.Equal(t, 0, p.MaxAttempts)
}

func TestStateCallbacksZeroValue(t *testing.T) {
	var sc StateCallbacks
	assert.Nil(t, sc.OnConnected)
	assert.Nil(t, sc.OnDisconnected)
	assert.Nil(t, sc.OnReconnecting)
	assert.Nil(t, sc.OnReconnected)
	assert.Nil(t, sc.OnClosed)
}

func TestJitterModeReExports(t *testing.T) {
	assert.Equal(t, types.JitterNone, JitterNone)
	assert.Equal(t, types.JitterFull, JitterFull)
	assert.Equal(t, types.JitterEqual, JitterEqual)
}
