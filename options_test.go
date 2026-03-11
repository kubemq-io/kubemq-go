package kubemq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDefaultOptions(t *testing.T) {
	opts := GetDefaultOptions()

	assert.Equal(t, "localhost", opts.host)
	assert.Equal(t, 50000, opts.port)
	assert.False(t, opts.isSecured)
	assert.Equal(t, "", opts.clientId)
	assert.Equal(t, 10, opts.receiveBufferSize)
	assert.Equal(t, 15*time.Minute, opts.defaultCacheTTL)

	// v2 defaults
	assert.Equal(t, 10*time.Second, opts.connectionTimeout)
	assert.Equal(t, 100*1024*1024, opts.maxRecvMsgSize)
	assert.Equal(t, 100*1024*1024, opts.maxSendMsgSize)
	assert.True(t, opts.waitForReady)
	assert.Equal(t, 10*time.Second, opts.keepaliveTime)
	assert.Equal(t, 5*time.Second, opts.keepaliveTimeout)
	assert.True(t, opts.permitKeepaliveWithoutStream)
	assert.Equal(t, 5*time.Second, opts.drainTimeout)
	assert.False(t, opts.checkConnection)

	// Reconnect policy defaults
	assert.Equal(t, 0, opts.reconnectPolicy.MaxAttempts)
	assert.Equal(t, 1*time.Second, opts.reconnectPolicy.InitialDelay)
	assert.Equal(t, 30*time.Second, opts.reconnectPolicy.MaxDelay)
	assert.Equal(t, 2.0, opts.reconnectPolicy.Multiplier)
	assert.Equal(t, JitterFull, opts.reconnectPolicy.JitterMode)
	assert.Equal(t, 1000, opts.reconnectPolicy.BufferSize)
}

func TestValidateEmptyHost(t *testing.T) {
	opts := GetDefaultOptions()
	opts.host = ""
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "host address is required")
}

func TestValidateInvalidPort(t *testing.T) {
	opts := GetDefaultOptions()
	opts.port = 0
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "port must be between 1 and 65535")

	opts.port = 70000
	err = opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "port must be between 1 and 65535")
}

func TestValidateNegativeConnectionTimeout(t *testing.T) {
	opts := GetDefaultOptions()
	opts.connectionTimeout = -1
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection timeout must be positive")
}

func TestValidateNegativeMaxRecvMsgSize(t *testing.T) {
	opts := GetDefaultOptions()
	opts.maxRecvMsgSize = 0
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max receive message size must be positive")
}

func TestValidateNegativeMaxSendMsgSize(t *testing.T) {
	opts := GetDefaultOptions()
	opts.maxSendMsgSize = 0
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max send message size must be positive")
}

func TestValidateReconnectPolicyInitialDelay(t *testing.T) {
	opts := GetDefaultOptions()
	opts.reconnectPolicy.InitialDelay = 0
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconnect initial delay must be positive")
}

func TestValidateReconnectPolicyMaxDelay(t *testing.T) {
	opts := GetDefaultOptions()
	opts.reconnectPolicy.MaxDelay = 100 * time.Millisecond
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconnect max delay must be >= initial delay")
}

func TestValidateReconnectPolicyMultiplier(t *testing.T) {
	opts := GetDefaultOptions()
	opts.reconnectPolicy.Multiplier = 0.5
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconnect multiplier must be >= 1.0")
}

func TestValidateSuccess(t *testing.T) {
	opts := GetDefaultOptions()
	err := opts.Validate()
	require.NoError(t, err)
}

func TestWithAddress(t *testing.T) {
	opts := GetDefaultOptions()
	WithAddress("example.com", 9090).apply(opts)
	assert.Equal(t, "example.com", opts.host)
	assert.Equal(t, 9090, opts.port)
}

func TestWithConnectionTimeout(t *testing.T) {
	opts := GetDefaultOptions()
	WithConnectionTimeout(30 * time.Second).apply(opts)
	assert.Equal(t, 30*time.Second, opts.connectionTimeout)
}

func TestWithMaxReceiveMessageSize(t *testing.T) {
	opts := GetDefaultOptions()
	WithMaxReceiveMessageSize(50 * 1024 * 1024).apply(opts)
	assert.Equal(t, 50*1024*1024, opts.maxRecvMsgSize)
}

func TestWithMaxSendMessageSize(t *testing.T) {
	opts := GetDefaultOptions()
	WithMaxSendMessageSize(50 * 1024 * 1024).apply(opts)
	assert.Equal(t, 50*1024*1024, opts.maxSendMsgSize)
}

func TestWithWaitForReady(t *testing.T) {
	opts := GetDefaultOptions()
	WithWaitForReady(false).apply(opts)
	assert.False(t, opts.waitForReady)
}

func TestWithDrainTimeout(t *testing.T) {
	opts := GetDefaultOptions()
	WithDrainTimeout(15 * time.Second).apply(opts)
	assert.Equal(t, 15*time.Second, opts.drainTimeout)
}

func TestWithKeepaliveTime(t *testing.T) {
	opts := GetDefaultOptions()
	WithKeepaliveTime(20 * time.Second).apply(opts)
	assert.Equal(t, 20*time.Second, opts.keepaliveTime)
}

func TestWithKeepaliveTimeout(t *testing.T) {
	opts := GetDefaultOptions()
	WithKeepaliveTimeout(3 * time.Second).apply(opts)
	assert.Equal(t, 3*time.Second, opts.keepaliveTimeout)
}

func TestWithPermitKeepaliveWithoutStream(t *testing.T) {
	opts := GetDefaultOptions()
	WithPermitKeepaliveWithoutStream(false).apply(opts)
	assert.False(t, opts.permitKeepaliveWithoutStream)
}

func TestWithReconnectPolicy(t *testing.T) {
	opts := GetDefaultOptions()
	p := ReconnectPolicy{
		MaxAttempts:  5,
		InitialDelay: 2 * time.Second,
		MaxDelay:     60 * time.Second,
		Multiplier:   3.0,
		JitterMode:   JitterEqual,
		BufferSize:   500,
	}
	WithReconnectPolicy(p).apply(opts)
	assert.Equal(t, 5, opts.reconnectPolicy.MaxAttempts)
	assert.Equal(t, 2*time.Second, opts.reconnectPolicy.InitialDelay)
	assert.Equal(t, 60*time.Second, opts.reconnectPolicy.MaxDelay)
	assert.Equal(t, 3.0, opts.reconnectPolicy.Multiplier)
	assert.Equal(t, JitterEqual, opts.reconnectPolicy.JitterMode)
	assert.Equal(t, 500, opts.reconnectPolicy.BufferSize)
}

func TestWithStateCallbacks(t *testing.T) {
	opts := GetDefaultOptions()
	called := false

	WithOnConnected(func() { called = true }).apply(opts)
	assert.NotNil(t, opts.stateCallbacks.OnConnected)

	WithOnDisconnected(func() {}).apply(opts)
	assert.NotNil(t, opts.stateCallbacks.OnDisconnected)

	WithOnReconnecting(func() {}).apply(opts)
	assert.NotNil(t, opts.stateCallbacks.OnReconnecting)

	WithOnReconnected(func() {}).apply(opts)
	assert.NotNil(t, opts.stateCallbacks.OnReconnected)

	WithOnClosed(func() {}).apply(opts)
	assert.NotNil(t, opts.stateCallbacks.OnClosed)

	opts.stateCallbacks.OnConnected()
	assert.True(t, called)
}

func TestWithOnBufferDrain(t *testing.T) {
	opts := GetDefaultOptions()
	var count int
	WithOnBufferDrain(func(n int) { count = n }).apply(opts)
	assert.NotNil(t, opts.onBufferDrain)
	opts.onBufferDrain(42)
	assert.Equal(t, 42, count)
}

func TestValidateOptions_InvalidReceiveBufferSize(t *testing.T) {
	o := GetDefaultOptions()
	o.receiveBufferSize = 0
	err := o.Validate()
	assert.Error(t, err)
}

func TestValidateOptions_NegativeCacheTTL(t *testing.T) {
	o := GetDefaultOptions()
	o.defaultCacheTTL = -1
	err := o.Validate()
	assert.Error(t, err)
}

func TestValidateOptions_InvalidDefaultChannel(t *testing.T) {
	o := GetDefaultOptions()
	o.defaultChannel = "invalid channel name with spaces"
	err := o.Validate()
	assert.Error(t, err)
}

func TestValidateOptions_ValidDefaultChannel(t *testing.T) {
	o := GetDefaultOptions()
	o.defaultChannel = "orders.events"
	err := o.Validate()
	assert.NoError(t, err)
}
