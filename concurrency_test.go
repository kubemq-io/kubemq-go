package kubemq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultCallbackConfig_Values(t *testing.T) {
	cfg := DefaultCallbackConfig()
	assert.Equal(t, 1, cfg.MaxConcurrent)
	assert.Equal(t, 30*time.Second, cfg.Timeout)
}

func TestWithMaxConcurrentCallbacks_Valid(t *testing.T) {
	opts := GetDefaultOptions()
	WithMaxConcurrentCallbacks(4).apply(opts)
	assert.Equal(t, 4, opts.callbackConfig.MaxConcurrent)
}

func TestWithMaxConcurrentCallbacks_Floor(t *testing.T) {
	opts := GetDefaultOptions()
	WithMaxConcurrentCallbacks(0).apply(opts)
	assert.Equal(t, 1, opts.callbackConfig.MaxConcurrent)
}

func TestWithMaxConcurrentCallbacks_Negative(t *testing.T) {
	opts := GetDefaultOptions()
	WithMaxConcurrentCallbacks(-5).apply(opts)
	assert.Equal(t, 1, opts.callbackConfig.MaxConcurrent)
}

func TestWithCallbackTimeout(t *testing.T) {
	opts := GetDefaultOptions()
	WithCallbackTimeout(10 * time.Second).apply(opts)
	assert.Equal(t, 10*time.Second, opts.callbackConfig.Timeout)
}
