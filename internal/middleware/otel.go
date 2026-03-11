package middleware

import (
	"context"
	"errors"
	"fmt"
	"time"

	kubemqotel "github.com/kubemq-io/kubemq-go/v2/internal/otel"
	"github.com/kubemq-io/kubemq-go/v2/internal/types"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// OTelInterceptor creates OTel spans and records metrics for all SDK operations.
// When no OTel SDK is registered (no-op provider), all instrumentation is
// effectively free — the OTel API returns no-op spans and meters.
//
// Exported for use by client.go within the module; hidden from external users
// by the internal/ package path.
type OTelInterceptor struct {
	tracer  trace.Tracer
	logger  types.Logger
	metrics *metricsCollector
	addr    string
	port    int
}

// NewOTelInterceptor creates the OTel instrumentation interceptor.
//
// tracerProvider and meterProvider are stored as `any` in Options (to avoid
// hard OTel import in the public API). They are type-asserted here.
// If nil, falls back to the global OTel provider (which is no-op if
// no OTel SDK is registered).
func NewOTelInterceptor(
	tracerProvider any,
	meterProvider any,
	logger types.Logger,
	addr string,
	port int,
	cardinalityCfg CardinalityConfig,
	version string,
) *OTelInterceptor {
	tp := resolveTracerProvider(tracerProvider)
	mp := resolveMeterProvider(meterProvider)

	tracer := tp.Tracer(
		kubemqotel.InstrumentationName,
		trace.WithInstrumentationVersion(version),
	)

	meter := mp.Meter(
		kubemqotel.InstrumentationName,
		metric.WithInstrumentationVersion(version),
	)

	mc := newMetricsCollector(meter, logger, cardinalityCfg)

	return &OTelInterceptor{
		tracer:  tracer,
		logger:  logger,
		metrics: mc,
		addr:    addr,
		port:    port,
	}
}

func resolveTracerProvider(tp any) trace.TracerProvider {
	if tp == nil {
		return otel.GetTracerProvider()
	}
	if p, ok := tp.(trace.TracerProvider); ok {
		return p
	}
	return otel.GetTracerProvider()
}

func resolveMeterProvider(mp any) metric.MeterProvider {
	if mp == nil {
		return otel.GetMeterProvider()
	}
	if p, ok := mp.(metric.MeterProvider); ok {
		return p
	}
	return otel.GetMeterProvider()
}

func (*OTelInterceptor) Name() string { return "otel" }

// SpanConfig defines the parameters for creating a span for an SDK operation.
type SpanConfig struct {
	Operation  string
	Channel    string
	SpanKind   trace.SpanKind
	ClientID   string
	MessageID  string
	Group      string
	BodySize   int
	BatchCount int
	Links      []trace.Link
}

// StartSpan creates and starts an OTel span for the given operation.
// Returns the enriched context and a finisher function that must be called
// with the operation error (or nil) to end the span and record metrics.
func (o *OTelInterceptor) StartSpan(ctx context.Context, cfg SpanConfig) (context.Context, func(error)) {
	spanName := fmt.Sprintf("%s %s", cfg.Operation, cfg.Channel)

	attrs := []attribute.KeyValue{
		kubemqotel.MessagingSystemAttr(),
		attribute.String(kubemqotel.AttrMessagingOperationName, cfg.Operation),
		attribute.String(kubemqotel.AttrMessagingOperationType, cfg.Operation),
		attribute.String(kubemqotel.AttrMessagingDestinationName, cfg.Channel),
		attribute.String(kubemqotel.AttrServerAddress, o.addr),
		attribute.Int(kubemqotel.AttrServerPort, o.port),
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(cfg.SpanKind),
		trace.WithAttributes(attrs...),
	}

	if len(cfg.Links) > 0 {
		linksCapped := cfg.Links
		if len(linksCapped) > 128 {
			linksCapped = linksCapped[:128]
		}
		opts = append(opts, trace.WithLinks(linksCapped...))
	}

	ctx, span := o.tracer.Start(ctx, spanName, opts...)

	if span.IsRecording() {
		if cfg.ClientID != "" {
			span.SetAttributes(attribute.String(kubemqotel.AttrMessagingClientID, cfg.ClientID))
		}
		if cfg.MessageID != "" {
			span.SetAttributes(attribute.String(kubemqotel.AttrMessagingMessageID, cfg.MessageID))
		}
		if cfg.Group != "" {
			span.SetAttributes(attribute.String(kubemqotel.AttrMessagingConsumerGroup, cfg.Group))
		}
		if cfg.BodySize > 0 {
			span.SetAttributes(attribute.Int(kubemqotel.AttrMessagingMessageBodySize, cfg.BodySize))
		}
		if cfg.BatchCount > 0 {
			span.SetAttributes(attribute.Int(kubemqotel.AttrMessagingBatchCount, cfg.BatchCount))
		}
	}

	start := time.Now()

	finisher := func(err error) {
		duration := time.Since(start)

		if err != nil {
			span.SetStatus(codes.Error, errorDescription(err))
			span.SetAttributes(attribute.String(kubemqotel.AttrErrorType, errorTypeValue(err)))
		}

		span.End()

		o.metrics.RecordOperation(ctx, cfg, duration, err)
	}

	return ctx, finisher
}

// RecordRetryEvent adds a "retry" span event with required attributes.
func (o *OTelInterceptor) RecordRetryEvent(ctx context.Context, attempt int, delay time.Duration, err error) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.AddEvent("retry", trace.WithAttributes(
		attribute.Int(kubemqotel.AttrRetryAttempt, attempt),
		attribute.Float64(kubemqotel.AttrRetryDelaySeconds, delay.Seconds()),
		attribute.String(kubemqotel.AttrErrorType, errorTypeValue(err)),
	))
}

// Metrics returns the underlying metrics collector for connection state
// integration and retry metric recording.
func (o *OTelInterceptor) Metrics() *metricsCollector {
	return o.metrics
}

func errorDescription(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func errorTypeValue(err error) string {
	if err == nil {
		return ""
	}
	var kErr *types.KubeMQError
	if errors.As(err, &kErr) {
		return errorCodeToType(kErr.Code)
	}
	return "fatal"
}

func errorCodeToType(code types.ErrorCode) string {
	switch code {
	case types.ErrCodeTransient:
		return "transient"
	case types.ErrCodeTimeout:
		return "timeout"
	case types.ErrCodeThrottling:
		return "throttling"
	case types.ErrCodeAuthentication:
		return "authentication"
	case types.ErrCodeAuthorization:
		return "authorization"
	case types.ErrCodeValidation:
		return "validation"
	case types.ErrCodeNotFound:
		return "not_found"
	case types.ErrCodeFatal:
		return "fatal"
	case types.ErrCodeCancellation:
		return "cancellation"
	case types.ErrCodeBackpressure:
		return "backpressure"
	default:
		return "fatal"
	}
}
