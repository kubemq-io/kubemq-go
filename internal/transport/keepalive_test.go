package transport

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultKeepaliveConfig(t *testing.T) {
	cfg := DefaultKeepaliveConfig()
	assert.Equal(t, 10*time.Second, cfg.Time)
	assert.Equal(t, 5*time.Second, cfg.Timeout)
	assert.True(t, cfg.PermitWithoutStream)
}

func TestKeepaliveConfigDialOption(t *testing.T) {
	cfg := KeepaliveConfig{
		Time:                15 * time.Second,
		Timeout:             3 * time.Second,
		PermitWithoutStream: false,
	}
	opt := cfg.dialOption()
	assert.NotNil(t, opt)
}

func TestKeepaliveConfigCustomValues(t *testing.T) {
	cfg := KeepaliveConfig{
		Time:                20 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}
	assert.Equal(t, 20*time.Second, cfg.Time)
	assert.Equal(t, 10*time.Second, cfg.Timeout)
	assert.True(t, cfg.PermitWithoutStream)
}
