package kubemq

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSlogAdapter_Nil(t *testing.T) {
	adapter := NewSlogAdapter(nil)
	assert.NotNil(t, adapter)
	adapter.Debug("test")
	adapter.Info("test")
	adapter.Warn("test")
	adapter.Error("test")
}

func TestNewSlogAdapter_Methods(t *testing.T) {
	adapter := NewSlogAdapter(nil)
	adapter.Debug("debug msg", "key", "value")
	adapter.Info("info msg", "key", "value")
	adapter.Warn("warn msg", "key", "value")
	adapter.Error("error msg", "key", "value")
}

func TestNewSlogAdapter_WithExplicitLogger(t *testing.T) {
	l := slog.Default()
	adapter := NewSlogAdapter(l)
	assert.NotNil(t, adapter)
	adapter.Info("explicit logger works", "count", 42)
}

func TestNoopLogger(t *testing.T) {
	var l noopLogger
	l.Debug("test")
	l.Info("test")
	l.Warn("test")
	l.Error("test")
}

func TestNoopLogger_WithKeysAndValues(t *testing.T) {
	var l noopLogger
	l.Debug("debug", "key", "value", "count", 1)
	l.Info("info", "key", "value", "count", 2)
	l.Warn("warn", "key", "value", "count", 3)
	l.Error("error", "key", "value", "count", 4)
}

func TestNoopLogger_ImplementsLogger(t *testing.T) {
	var l Logger = noopLogger{}
	assert.NotNil(t, l)
}

func TestSlogAdapter_ImplementsLogger(t *testing.T) {
	var l Logger = NewSlogAdapter(nil)
	assert.NotNil(t, l)
}

func TestNoopLoggerInterface(t *testing.T) {
	var l noopLogger
	assert.NotPanics(t, func() {
		l.Debug("msg")
		l.Info("msg")
		l.Warn("msg")
		l.Error("msg")
		l.Debug("msg", "k1", "v1", "k2", 2)
		l.Info("msg", "k1", "v1", "k2", 2)
		l.Warn("msg", "k1", "v1", "k2", 2)
		l.Error("msg", "k1", "v1", "k2", 2)
	})
	var iface Logger = l
	assert.NotNil(t, iface)
}
