package middleware

import (
	"context"
	"sync"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/otel"
	"github.com/kubemq-io/kubemq-go/v2/internal/types"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// metricsCollector records OTel metrics per GS 05-observability.md.
type metricsCollector struct {
	operationDuration metric.Float64Histogram
	sentMessages      metric.Int64Counter
	consumedMessages  metric.Int64Counter
	connectionCount   metric.Int64UpDownCounter
	reconnections     metric.Int64Counter
	retryAttempts     metric.Int64Counter
	retryExhausted    metric.Int64Counter

	cardinality *cardinalityManager
	logger      types.Logger
}

// Histogram bucket boundaries per GS specification.
var durationBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1,
	0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 30, 60,
}

func newMetricsCollector(
	meter metric.Meter,
	logger types.Logger,
	cardCfg CardinalityConfig,
) *metricsCollector {
	mc := &metricsCollector{
		logger:      logger,
		cardinality: newCardinalityManager(cardCfg, logger),
	}

	var err error

	mc.operationDuration, err = meter.Float64Histogram(
		"messaging.client.operation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of each messaging operation"),
		metric.WithExplicitBucketBoundaries(durationBuckets...),
	)
	if err != nil {
		logger.Error("failed to create operation.duration histogram", "error", err)
	}

	mc.sentMessages, err = meter.Int64Counter(
		"messaging.client.sent.messages",
		metric.WithUnit("{message}"),
		metric.WithDescription("Total messages sent"),
	)
	if err != nil {
		logger.Error("failed to create sent.messages counter", "error", err)
	}

	mc.consumedMessages, err = meter.Int64Counter(
		"messaging.client.consumed.messages",
		metric.WithUnit("{message}"),
		metric.WithDescription("Total messages consumed"),
	)
	if err != nil {
		logger.Error("failed to create consumed.messages counter", "error", err)
	}

	mc.connectionCount, err = meter.Int64UpDownCounter(
		"messaging.client.connection.count",
		metric.WithUnit("{connection}"),
		metric.WithDescription("Active connections"),
	)
	if err != nil {
		logger.Error("failed to create connection.count counter", "error", err)
	}

	mc.reconnections, err = meter.Int64Counter(
		"messaging.client.reconnections",
		metric.WithUnit("{attempt}"),
		metric.WithDescription("Reconnection attempts"),
	)
	if err != nil {
		logger.Error("failed to create reconnections counter", "error", err)
	}

	mc.retryAttempts, err = meter.Int64Counter(
		"kubemq.client.retry.attempts",
		metric.WithUnit("{attempt}"),
		metric.WithDescription("Retry attempts"),
	)
	if err != nil {
		logger.Error("failed to create retry.attempts counter", "error", err)
	}

	mc.retryExhausted, err = meter.Int64Counter(
		"kubemq.client.retry.exhausted",
		metric.WithUnit("{attempt}"),
		metric.WithDescription("Retries exhausted"),
	)
	if err != nil {
		logger.Error("failed to create retry.exhausted counter", "error", err)
	}

	return mc
}

// RecordOperation records duration and message count metrics for an operation.
func (mc *metricsCollector) RecordOperation(
	ctx context.Context,
	cfg SpanConfig, //nolint:gocritic // hugeParam: value semantics kept for API consistency with StartSpan
	duration time.Duration,
	err error,
) {
	attrs := mc.baseAttributes(cfg.Operation, cfg.Channel, err)

	mc.operationDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))

	switch cfg.Operation {
	case otel.OpPublish, otel.OpSend:
		count := int64(1)
		if cfg.BatchCount > 0 {
			count = int64(cfg.BatchCount)
		}
		mc.sentMessages.Add(ctx, count, metric.WithAttributes(attrs...))
	case otel.OpProcess, otel.OpReceive:
		count := int64(1)
		if cfg.BatchCount > 0 {
			count = int64(cfg.BatchCount)
		}
		mc.consumedMessages.Add(ctx, count, metric.WithAttributes(attrs...))
	}
}

// RecordConnectionUp increments the connection count.
func (mc *metricsCollector) RecordConnectionUp(ctx context.Context) {
	mc.connectionCount.Add(ctx, 1, metric.WithAttributes(
		attribute.String(otel.AttrMessagingSystem, otel.MessagingSystemKubeMQ),
	))
}

// RecordConnectionDown decrements the connection count.
func (mc *metricsCollector) RecordConnectionDown(ctx context.Context) {
	mc.connectionCount.Add(ctx, -1, metric.WithAttributes(
		attribute.String(otel.AttrMessagingSystem, otel.MessagingSystemKubeMQ),
	))
}

// RecordReconnectionAttempt increments the reconnection counter.
func (mc *metricsCollector) RecordReconnectionAttempt(ctx context.Context) {
	mc.reconnections.Add(ctx, 1, metric.WithAttributes(
		attribute.String(otel.AttrMessagingSystem, otel.MessagingSystemKubeMQ),
	))
}

// RecordRetryAttempt increments the retry attempts counter.
func (mc *metricsCollector) RecordRetryAttempt(
	ctx context.Context,
	operationName string,
	err error,
) {
	attrs := []attribute.KeyValue{
		attribute.String(otel.AttrMessagingSystem, otel.MessagingSystemKubeMQ),
		attribute.String(otel.AttrMessagingOperationName, operationName),
	}
	if err != nil {
		attrs = append(attrs, attribute.String(otel.AttrErrorType, errorTypeValue(err)))
	}
	mc.retryAttempts.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordRetryExhausted increments the retry exhausted counter.
func (mc *metricsCollector) RecordRetryExhausted(
	ctx context.Context,
	operationName string,
	err error,
) {
	attrs := []attribute.KeyValue{
		attribute.String(otel.AttrMessagingSystem, otel.MessagingSystemKubeMQ),
		attribute.String(otel.AttrMessagingOperationName, operationName),
	}
	if err != nil {
		attrs = append(attrs, attribute.String(otel.AttrErrorType, errorTypeValue(err)))
	}
	mc.retryExhausted.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// baseAttributes returns the common metric attributes for an operation.
func (mc *metricsCollector) baseAttributes(
	operationName string,
	channel string,
	err error,
) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(otel.AttrMessagingSystem, otel.MessagingSystemKubeMQ),
		attribute.String(otel.AttrMessagingOperationName, operationName),
	}

	if mc.cardinality.ShouldIncludeChannel(channel) {
		attrs = append(attrs, attribute.String(otel.AttrMessagingDestinationName, channel))
	}

	if err != nil {
		attrs = append(attrs, attribute.String(otel.AttrErrorType, errorTypeValue(err)))
	}

	return attrs
}

// CardinalityConfig controls metric cardinality for messaging.destination.name.
type CardinalityConfig struct {
	Threshold int
	Allowlist []string
}

// DefaultCardinalityConfig returns the default cardinality configuration.
func DefaultCardinalityConfig() CardinalityConfig {
	return CardinalityConfig{
		Threshold: 100,
		Allowlist: nil,
	}
}

type cardinalityManager struct {
	config   CardinalityConfig
	logger   types.Logger
	mu       sync.RWMutex
	seen     map[string]struct{}
	exceeded bool
	allowSet map[string]struct{}
}

func newCardinalityManager(cfg CardinalityConfig, logger types.Logger) *cardinalityManager {
	allowSet := make(map[string]struct{}, len(cfg.Allowlist))
	for _, ch := range cfg.Allowlist {
		allowSet[ch] = struct{}{}
	}
	return &cardinalityManager{
		config:   cfg,
		logger:   logger,
		seen:     make(map[string]struct{}),
		allowSet: allowSet,
	}
}

// ShouldIncludeChannel returns true if the channel name should be included
// as a metric attribute value. Returns false when cardinality threshold is
// exceeded for non-allowlisted channels.
func (cm *cardinalityManager) ShouldIncludeChannel(channel string) bool {
	if channel == "" {
		return false
	}

	if _, ok := cm.allowSet[channel]; ok {
		return true
	}

	cm.mu.RLock()
	_, known := cm.seen[channel]
	exceeded := cm.exceeded
	cm.mu.RUnlock()

	if known {
		return true
	}

	if exceeded {
		return false
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, known = cm.seen[channel]; known {
		return true
	}

	if len(cm.seen) >= cm.config.Threshold {
		if !cm.exceeded {
			cm.exceeded = true
			cm.logger.Warn("metric cardinality threshold exceeded — omitting messaging.destination.name for new channels",
				"threshold", cm.config.Threshold,
				"channel", channel,
			)
		}
		return false
	}

	cm.seen[channel] = struct{}{}
	return true
}

// Reset clears the seen channels. Useful for testing.
func (cm *cardinalityManager) Reset() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.seen = make(map[string]struct{})
	cm.exceeded = false
}
