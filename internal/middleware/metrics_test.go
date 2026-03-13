package middleware

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	kubemqotel "github.com/kubemq-io/kubemq-go/v2/internal/otel"
	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

var errMeterFail = errors.New("simulated meter error")

type failingMeter struct {
	noop.Meter
}

func (failingMeter) Float64Histogram(string, ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	return noop.Float64Histogram{}, errMeterFail
}

func (failingMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return noop.Int64Counter{}, errMeterFail
}

func (failingMeter) Int64UpDownCounter(string, ...metric.Int64UpDownCounterOption) (metric.Int64UpDownCounter, error) {
	return noop.Int64UpDownCounter{}, errMeterFail
}

func TestDefaultCardinalityConfig(t *testing.T) {
	cfg := DefaultCardinalityConfig()
	assert.Equal(t, 100, cfg.Threshold)
	assert.Nil(t, cfg.Allowlist)
}

func TestMetricsCollector_RecordConnectionUp(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	m := oi.Metrics()
	m.RecordConnectionUp(context.Background())
}

func TestMetricsCollector_RecordConnectionDown(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	m := oi.Metrics()
	m.RecordConnectionDown(context.Background())
}

func TestMetricsCollector_RecordReconnectionAttempt(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	m := oi.Metrics()
	m.RecordReconnectionAttempt(context.Background())
}

func TestMetricsCollector_RecordRetryAttempt(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	m := oi.Metrics()
	m.RecordRetryAttempt(context.Background(), "SendEvent", errors.New("transient"))
}

func TestMetricsCollector_RecordRetryExhausted(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	m := oi.Metrics()
	m.RecordRetryExhausted(context.Background(), "SendEvent", errors.New("exhausted"))
}

func TestCardinalityManager_Reset(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 10}, "2.0.0")
	m := oi.Metrics()
	m.cardinality.Reset()
	assert.True(t, m.cardinality.ShouldIncludeChannel("new-channel"))
}

// --- Additional metrics tests ---

func TestRecordOperation_Publish(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")
	mc := oi.Metrics()
	mc.RecordOperation(context.Background(), SpanConfig{
		Operation: kubemqotel.OpPublish,
		Channel:   "events-ch",
	}, 50*time.Millisecond, nil)
}

func TestRecordOperation_Send(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")
	mc := oi.Metrics()
	mc.RecordOperation(context.Background(), SpanConfig{
		Operation: kubemqotel.OpSend,
		Channel:   "queue-ch",
	}, 100*time.Millisecond, nil)
}

func TestRecordOperation_Process(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")
	mc := oi.Metrics()
	mc.RecordOperation(context.Background(), SpanConfig{
		Operation: kubemqotel.OpProcess,
		Channel:   "consumer-ch",
	}, 200*time.Millisecond, nil)
}

func TestRecordOperation_Receive(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")
	mc := oi.Metrics()
	mc.RecordOperation(context.Background(), SpanConfig{
		Operation: kubemqotel.OpReceive,
		Channel:   "recv-ch",
	}, 10*time.Millisecond, nil)
}

func TestRecordOperation_WithBatchCount(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")
	mc := oi.Metrics()

	mc.RecordOperation(context.Background(), SpanConfig{
		Operation:  kubemqotel.OpPublish,
		Channel:    "batch-ch",
		BatchCount: 10,
	}, 50*time.Millisecond, nil)

	mc.RecordOperation(context.Background(), SpanConfig{
		Operation:  kubemqotel.OpProcess,
		Channel:    "batch-ch",
		BatchCount: 5,
	}, 30*time.Millisecond, nil)
}

func TestRecordOperation_WithError(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")
	mc := oi.Metrics()
	mc.RecordOperation(context.Background(), SpanConfig{
		Operation: kubemqotel.OpPublish,
		Channel:   "err-ch",
	}, 5*time.Millisecond, errors.New("send failed"))
}

func TestRecordOperation_WithKubeMQError(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")
	mc := oi.Metrics()
	mc.RecordOperation(context.Background(), SpanConfig{
		Operation: kubemqotel.OpSend,
		Channel:   "err-ch",
	}, 5*time.Millisecond, &types.KubeMQError{Code: types.ErrCodeTimeout, Message: "timed out"})
}

func TestRecordOperation_UnknownOperation(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")
	mc := oi.Metrics()
	mc.RecordOperation(context.Background(), SpanConfig{
		Operation: "settle",
		Channel:   "ch",
	}, 1*time.Millisecond, nil)
}

func TestRecordRetryAttempt_NilError(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	mc := oi.Metrics()
	mc.RecordRetryAttempt(context.Background(), "SendEvent", nil)
}

func TestRecordRetryExhausted_NilError(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	mc := oi.Metrics()
	mc.RecordRetryExhausted(context.Background(), "SendEvent", nil)
}

func TestShouldIncludeChannel_EmptyChannel(t *testing.T) {
	cm := newCardinalityManager(CardinalityConfig{Threshold: 10}, &testLogger{})
	assert.False(t, cm.ShouldIncludeChannel(""))
}

func TestShouldIncludeChannel_Allowlist(t *testing.T) {
	cm := newCardinalityManager(CardinalityConfig{
		Threshold: 2,
		Allowlist: []string{"important-ch", "vip-ch"},
	}, &testLogger{})

	cm.ShouldIncludeChannel("ch-1")
	cm.ShouldIncludeChannel("ch-2")

	assert.False(t, cm.ShouldIncludeChannel("ch-3"), "threshold exceeded for non-allowlisted channel")
	assert.True(t, cm.ShouldIncludeChannel("important-ch"), "allowlisted channel should always be included")
	assert.True(t, cm.ShouldIncludeChannel("vip-ch"), "allowlisted channel should always be included")
}

func TestShouldIncludeChannel_ThresholdExceeded(t *testing.T) {
	log := &testLogger{}
	cm := newCardinalityManager(CardinalityConfig{Threshold: 3}, log)

	require.True(t, cm.ShouldIncludeChannel("ch-1"))
	require.True(t, cm.ShouldIncludeChannel("ch-2"))
	require.True(t, cm.ShouldIncludeChannel("ch-3"))

	assert.False(t, cm.ShouldIncludeChannel("ch-4"), "4th channel should be rejected after threshold of 3")

	assert.True(t, cm.ShouldIncludeChannel("ch-1"), "already-seen channel should still be included")
}

func TestShouldIncludeChannel_ThresholdZero(t *testing.T) {
	cm := newCardinalityManager(CardinalityConfig{Threshold: 0}, &testLogger{})
	assert.False(t, cm.ShouldIncludeChannel("any-ch"), "threshold 0 should reject all channels")
}

func TestShouldIncludeChannel_ConcurrentAccess(t *testing.T) {
	cm := newCardinalityManager(CardinalityConfig{Threshold: 1000}, &testLogger{})

	done := make(chan struct{})
	for i := 0; i < 50; i++ {
		go func(n int) {
			defer func() { done <- struct{}{} }()
			ch := fmt.Sprintf("ch-%d", n)
			cm.ShouldIncludeChannel(ch)
		}(i)
	}
	for i := 0; i < 50; i++ {
		<-done
	}
}

func TestCardinalityManager_ResetClearsExceeded(t *testing.T) {
	cm := newCardinalityManager(CardinalityConfig{Threshold: 1}, &testLogger{})

	require.True(t, cm.ShouldIncludeChannel("ch-1"))
	require.False(t, cm.ShouldIncludeChannel("ch-2"))

	cm.Reset()

	assert.True(t, cm.ShouldIncludeChannel("ch-new"), "should accept channels after reset")
}

func TestNewMetricsCollector_MeterErrors(t *testing.T) {
	log := &testLogger{}
	fm := &failingMeter{}
	mc := newMetricsCollector(fm, log, DefaultCardinalityConfig())

	require.NotNil(t, mc, "collector should be returned even when meter returns errors")

	log.mu.Lock()
	errCount := len(log.errorMsgs)
	log.mu.Unlock()

	assert.Equal(t, 7, errCount, "should log 7 error messages for all failed instrument registrations")
}
