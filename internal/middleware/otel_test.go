package middleware

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestOTelInterceptor_Name(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	assert.Equal(t, "otel", oi.Name())
}

func TestOTelInterceptor_StartSpan_NoOp(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	ctx, finish := oi.StartSpan(context.Background(), SpanConfig{
		Operation: "test_op",
		Channel:   "test-ch",
		SpanKind:  trace.SpanKindProducer,
		ClientID:  "test-client",
	})
	assert.NotNil(t, ctx)
	require.NotNil(t, finish)
	finish(nil)
}

func TestOTelInterceptor_StartSpan_WithError(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	ctx, finish := oi.StartSpan(context.Background(), SpanConfig{
		Operation: "test_op",
		Channel:   "test-ch",
		SpanKind:  trace.SpanKindProducer,
	})
	assert.NotNil(t, ctx)
	finish(errors.New("test error"))
}

func TestOTelInterceptor_RecordRetryEvent(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	oi.RecordRetryEvent(context.Background(), 1, 100*time.Millisecond, errors.New("retry error"))
}

func TestOTelInterceptor_Metrics(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	m := oi.Metrics()
	assert.NotNil(t, m)
}

func TestErrorDescription(t *testing.T) {
	assert.Equal(t, "test error", errorDescription(errors.New("test error")))
	assert.Equal(t, "", errorDescription(nil))
}

func TestErrorTypeValue(t *testing.T) {
	assert.NotEmpty(t, errorTypeValue(errors.New("test")))
	assert.Equal(t, "", errorTypeValue(nil))
}

func TestErrorCodeToType(t *testing.T) {
	assert.Equal(t, "timeout", errorCodeToType("TIMEOUT"))
	assert.Equal(t, "fatal", errorCodeToType(""))
}

// --- Additional OTel tests ---

func TestStartSpan_WithRealTracerProvider(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	oi := NewOTelInterceptor(tp, nil, &testLogger{}, "10.0.0.1", 50000, CardinalityConfig{Threshold: 100}, "2.1.0")
	ctx, finish := oi.StartSpan(context.Background(), SpanConfig{
		Operation:  "publish",
		Channel:    "orders",
		SpanKind:   trace.SpanKindProducer,
		ClientID:   "client-1",
		MessageID:  "msg-abc",
		Group:      "group-A",
		BodySize:   512,
		BatchCount: 3,
	})
	require.NotNil(t, ctx)

	span := trace.SpanFromContext(ctx)
	assert.True(t, span.SpanContext().IsValid(), "span should have a valid SpanContext with real provider")

	finish(nil)
}

func TestStartSpan_FinishWithKubeMQError(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	oi := NewOTelInterceptor(tp, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")
	_, finish := oi.StartSpan(context.Background(), SpanConfig{
		Operation: "send",
		Channel:   "test-ch",
		SpanKind:  trace.SpanKindProducer,
	})

	kErr := &types.KubeMQError{Code: types.ErrCodeTimeout, Message: "deadline exceeded"}
	finish(kErr)
}

func TestStartSpan_WithLinksCapping(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	links := make([]trace.Link, 200)
	for i := range links {
		links[i] = trace.Link{}
	}

	oi := NewOTelInterceptor(tp, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")
	ctx, finish := oi.StartSpan(context.Background(), SpanConfig{
		Operation: "process",
		Channel:   "batch-ch",
		SpanKind:  trace.SpanKindConsumer,
		Links:     links,
	})
	assert.NotNil(t, ctx)
	finish(nil)
}

func TestStartSpan_WithFewLinks(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	links := []trace.Link{{}, {}}

	oi := NewOTelInterceptor(tp, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")
	ctx, finish := oi.StartSpan(context.Background(), SpanConfig{
		Operation: "process",
		Channel:   "ch",
		SpanKind:  trace.SpanKindConsumer,
		Links:     links,
	})
	assert.NotNil(t, ctx)
	finish(nil)
}

func TestRecordRetryEvent_WithRealProvider(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	oi := NewOTelInterceptor(tp, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")
	ctx, finish := oi.StartSpan(context.Background(), SpanConfig{
		Operation: "send",
		Channel:   "retry-ch",
		SpanKind:  trace.SpanKindProducer,
	})

	oi.RecordRetryEvent(ctx, 1, 100*time.Millisecond, errors.New("transient"))
	oi.RecordRetryEvent(ctx, 2, 200*time.Millisecond, errors.New("transient again"))
	finish(nil)
}

func TestErrorCodeToType_AllBranches(t *testing.T) {
	tests := []struct {
		code types.ErrorCode
		want string
	}{
		{types.ErrCodeTransient, "transient"},
		{types.ErrCodeTimeout, "timeout"},
		{types.ErrCodeThrottling, "throttling"},
		{types.ErrCodeAuthentication, "authentication"},
		{types.ErrCodeAuthorization, "authorization"},
		{types.ErrCodeValidation, "validation"},
		{types.ErrCodeNotFound, "not_found"},
		{types.ErrCodeFatal, "fatal"},
		{types.ErrCodeCancellation, "cancellation"},
		{types.ErrCodeBackpressure, "backpressure"},
		{"UNKNOWN_CODE", "fatal"},
		{"", "fatal"},
	}
	for _, tt := range tests {
		t.Run(string(tt.code), func(t *testing.T) {
			assert.Equal(t, tt.want, errorCodeToType(tt.code))
		})
	}
}

func TestErrorTypeValue_WithKubeMQError(t *testing.T) {
	kErr := &types.KubeMQError{Code: types.ErrCodeThrottling, Message: "rate limited"}
	assert.Equal(t, "throttling", errorTypeValue(kErr))
}

func TestErrorTypeValue_WithPlainError(t *testing.T) {
	assert.Equal(t, "fatal", errorTypeValue(errors.New("unknown")))
}

func TestErrorTypeValue_WithWrappedKubeMQError(t *testing.T) {
	inner := &types.KubeMQError{Code: types.ErrCodeNotFound, Message: "channel missing"}
	wrapped := fmt.Errorf("operation failed: %w", inner)
	assert.Equal(t, "not_found", errorTypeValue(wrapped))
}

func TestResolveTracerProvider_Nil(t *testing.T) {
	tp := resolveTracerProvider(nil)
	assert.NotNil(t, tp)
}

func TestResolveTracerProvider_ValidProvider(t *testing.T) {
	realTP := sdktrace.NewTracerProvider()
	defer func() { _ = realTP.Shutdown(context.Background()) }()

	tp := resolveTracerProvider(realTP)
	assert.Equal(t, realTP, tp)
}

func TestResolveTracerProvider_WrongType(t *testing.T) {
	tp := resolveTracerProvider("not-a-provider")
	assert.NotNil(t, tp, "should fall back to global provider")
}

func TestResolveMeterProvider_Nil(t *testing.T) {
	mp := resolveMeterProvider(nil)
	assert.NotNil(t, mp)
}

func TestResolveMeterProvider_ValidProvider(t *testing.T) {
	realMP := sdkmetric.NewMeterProvider()
	defer func() { _ = realMP.Shutdown(context.Background()) }()

	mp := resolveMeterProvider(realMP)
	assert.Equal(t, realMP, mp)
}

func TestResolveMeterProvider_WrongType(t *testing.T) {
	mp := resolveMeterProvider(42)
	assert.NotNil(t, mp, "should fall back to global provider")
}

func TestNewOTelInterceptor_WithRealProviders(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	mp := sdkmetric.NewMeterProvider()
	defer func() {
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	}()

	oi := NewOTelInterceptor(tp, mp, &testLogger{}, "myhost", 9090, CardinalityConfig{Threshold: 50}, "3.0.0")
	assert.Equal(t, "otel", oi.Name())
	assert.NotNil(t, oi.Metrics())
}

func TestNewOTelInterceptor_WithInvalidProviderTypes(t *testing.T) {
	oi := NewOTelInterceptor("bad-tp", struct{}{}, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	assert.Equal(t, "otel", oi.Name())
}

func TestStartSpan_MinimalConfig(t *testing.T) {
	oi := NewOTelInterceptor(nil, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{}, "2.0.0")
	ctx, finish := oi.StartSpan(context.Background(), SpanConfig{
		Operation: "publish",
		Channel:   "ch",
		SpanKind:  trace.SpanKindProducer,
	})
	assert.NotNil(t, ctx)
	finish(nil)
}

func TestStartSpan_FinishWithNilAndNonNilErrors(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	oi := NewOTelInterceptor(tp, nil, &testLogger{}, "localhost", 50000, CardinalityConfig{Threshold: 100}, "2.0.0")

	t.Run("nil error", func(t *testing.T) {
		_, finish := oi.StartSpan(context.Background(), SpanConfig{
			Operation: "publish",
			Channel:   "ch",
			SpanKind:  trace.SpanKindProducer,
		})
		finish(nil)
	})

	t.Run("plain error", func(t *testing.T) {
		_, finish := oi.StartSpan(context.Background(), SpanConfig{
			Operation: "publish",
			Channel:   "ch",
			SpanKind:  trace.SpanKindProducer,
		})
		finish(errors.New("connection refused"))
	})

	t.Run("KubeMQ error", func(t *testing.T) {
		_, finish := oi.StartSpan(context.Background(), SpanConfig{
			Operation: "send",
			Channel:   "ch",
			SpanKind:  trace.SpanKindProducer,
		})
		finish(&types.KubeMQError{Code: types.ErrCodeValidation, Message: "bad input"})
	})
}

var _ metric.MeterProvider = (*sdkmetric.MeterProvider)(nil)
