package testutil

import (
	"errors"
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AssertKubeMQError verifies the error is a *KubeMQError with the expected code.
func AssertKubeMQError(t *testing.T, err error, expectedCode types.ErrorCode) {
	t.Helper()
	require.Error(t, err)
	var kErr *types.KubeMQError
	require.True(t, errors.As(err, &kErr), "expected *KubeMQError, got %T: %v", err, err)
	assert.Equal(t, expectedCode, kErr.Code, "expected error code %s, got %s", expectedCode, kErr.Code)
}

// AssertRetryable verifies the error is a *KubeMQError with IsRetryable=true.
func AssertRetryable(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var kErr *types.KubeMQError
	require.True(t, errors.As(err, &kErr), "expected *KubeMQError, got %T", err)
	assert.True(t, kErr.IsRetryable, "expected retryable error, got non-retryable with code %s", kErr.Code)
}

// AssertNotRetryable verifies the error is a *KubeMQError with IsRetryable=false.
func AssertNotRetryable(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var kErr *types.KubeMQError
	require.True(t, errors.As(err, &kErr), "expected *KubeMQError, got %T", err)
	assert.False(t, kErr.IsRetryable, "expected non-retryable error, got retryable with code %s", kErr.Code)
}

// AssertErrorContains verifies the error message contains the substring.
func AssertErrorContains(t *testing.T, err error, substr string) {
	t.Helper()
	require.Error(t, err)
	assert.Contains(t, err.Error(), substr)
}
