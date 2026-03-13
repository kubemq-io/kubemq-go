package types

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- credential.go coverage ---

func TestStaticTokenProvider(t *testing.T) {
	p := NewStaticTokenProvider("my-token")
	require.NotNil(t, p)

	token, expiresAt, err := p.GetToken(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "my-token", token)
	assert.True(t, expiresAt.IsZero())
}

// --- reconnect.go coverage ---

func TestDefaultReconnectPolicy(t *testing.T) {
	p := DefaultReconnectPolicy()
	assert.Equal(t, 0, p.MaxAttempts)
	assert.Equal(t, 1*time.Second, p.InitialDelay)
	assert.Equal(t, 30*time.Second, p.MaxDelay)
	assert.Equal(t, 2.0, p.Multiplier)
	assert.Equal(t, JitterFull, p.JitterMode)
	assert.Equal(t, 1000, p.BufferSize)
}

// --- retry.go coverage ---

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	assert.Equal(t, 3, p.MaxRetries)
	assert.Equal(t, 100*time.Millisecond, p.InitialBackoff)
	assert.Equal(t, 10*time.Second, p.MaxBackoff)
	assert.Equal(t, 2.0, p.Multiplier)
	assert.Equal(t, JitterFull, p.JitterMode)
}

// --- errors.go Error() branch coverage ---

func TestKubeMQError_NoOperationNoChannel(t *testing.T) {
	err := &KubeMQError{
		Code:    ErrCodeTransient,
		Message: "connection refused",
	}
	got := err.Error()
	assert.Contains(t, got, "failed: connection refused")
}

func TestKubeMQError_NoOperationWithChannel(t *testing.T) {
	err := &KubeMQError{
		Code:    ErrCodeValidation,
		Channel: "ch",
		Message: "invalid payload",
	}
	got := err.Error()
	assert.Contains(t, got, "failed on channel \"ch\": invalid payload")
}

func TestKubeMQError_WithOperationNoChannel(t *testing.T) {
	err := &KubeMQError{
		Code:      ErrCodeTimeout,
		Operation: "SendEvent",
		Message:   "deadline exceeded",
	}
	got := err.Error()
	assert.Contains(t, got, "SendEvent failed: deadline exceeded")
}

func TestKubeMQError_WithOperationAndChannel(t *testing.T) {
	err := &KubeMQError{
		Code:      ErrCodeNotFound,
		Operation: "Subscribe",
		Channel:   "orders",
		Message:   "channel not found",
	}
	got := err.Error()
	assert.Contains(t, got, "Subscribe failed on channel \"orders\": channel not found")
}

func TestKubeMQError_CauseOverMessage(t *testing.T) {
	err := &KubeMQError{
		Code:    ErrCodeFatal,
		Message: "fallback message",
		Cause:   errors.New("root cause"),
	}
	got := err.Error()
	assert.Contains(t, got, "failed: root cause")
	assert.Contains(t, got, "root cause")
	assert.NotContains(t, got, "fallback message")
}

func TestKubeMQError_CodeFallback(t *testing.T) {
	err := &KubeMQError{
		Code: ErrCodeAuthorization,
	}
	got := err.Error()
	assert.Contains(t, got, "failed: AUTHORIZATION")
}

func TestKubeMQError_Suggestion(t *testing.T) {
	err := &KubeMQError{
		Code:    ErrCodeTransient,
		Message: "connection lost",
	}
	got := err.Error()
	assert.Contains(t, got, "failed: connection lost")
	assert.Contains(t, got, "Suggestion:")
	assert.Contains(t, got, "Retry the operation")
}

func TestKubeMQError_NoSuggestion(t *testing.T) {
	// Use unknown ErrorCode so ErrorSuggestions returns empty string
	err := &KubeMQError{
		Code:    ErrorCode("UNKNOWN_CODE"),
		Message: "something happened",
	}
	got := err.Error()
	assert.Equal(t, "failed: something happened", got)
	assert.NotContains(t, got, "Suggestion:")
}
