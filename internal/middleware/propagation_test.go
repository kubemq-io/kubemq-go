package middleware

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestTagsCarrier_GetSetKeys(t *testing.T) {
	tc := &TagsCarrier{Tags: map[string]string{}}
	tc.Set("key1", "value1")
	tc.Set("key2", "value2")
	assert.Equal(t, "value1", tc.Get("key1"))
	assert.Equal(t, "value2", tc.Get("key2"))
	assert.Equal(t, "", tc.Get("missing"))
	keys := tc.Keys()
	assert.Len(t, keys, 2)
}

func TestInjectTraceContext_NoSpan(t *testing.T) {
	tags := map[string]string{"existing": "tag"}
	result := InjectTraceContext(context.Background(), tags)
	assert.Equal(t, "tag", result["existing"])
}

func TestExtractTraceContext_NoTrace(t *testing.T) {
	tags := map[string]string{"key": "value"}
	ctx := ExtractTraceContext(context.Background(), tags)
	assert.NotNil(t, ctx)
}

func TestExtractProducerLink(t *testing.T) {
	tags := map[string]string{}
	link := ExtractProducerLink(tags)
	assert.False(t, link.SpanContext.IsValid())
}

// --- Additional propagation tests ---

func TestTagsCarrier_Get_NilTags(t *testing.T) {
	tc := &TagsCarrier{Tags: nil}
	assert.Equal(t, "", tc.Get("any-key"))
}

func TestTagsCarrier_Set_NilTags(t *testing.T) {
	tc := &TagsCarrier{Tags: nil}
	tc.Set("key", "value")
	assert.Equal(t, "value", tc.Get("key"), "Set should initialize nil map")
	assert.Len(t, tc.Tags, 1)
}

func TestTagsCarrier_Keys_Empty(t *testing.T) {
	tc := &TagsCarrier{Tags: map[string]string{}}
	keys := tc.Keys()
	assert.Empty(t, keys)
}

func TestTagsCarrier_Keys_NilTags(t *testing.T) {
	tc := &TagsCarrier{Tags: nil}
	keys := tc.Keys()
	assert.Empty(t, keys)
}

func TestTagsCarrier_Overwrite(t *testing.T) {
	tc := &TagsCarrier{Tags: map[string]string{"k": "v1"}}
	tc.Set("k", "v2")
	assert.Equal(t, "v2", tc.Get("k"))
	assert.Len(t, tc.Tags, 1)
}

func TestInjectTraceContext_NilTags(t *testing.T) {
	result := InjectTraceContext(context.Background(), nil)
	require.NotNil(t, result, "should create a new map when nil is passed")
}

func TestExtractTraceContext_NilTags(t *testing.T) {
	ctx := ExtractTraceContext(context.Background(), nil)
	assert.NotNil(t, ctx, "should return original context when tags are nil")
}

func TestExtractProducerLink_NilTags(t *testing.T) {
	link := ExtractProducerLink(nil)
	assert.False(t, link.SpanContext.IsValid())
}

func TestInjectAndExtract_RoundTrip(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(prev)

	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "producer-op")
	defer span.End()

	tags := InjectTraceContext(ctx, nil)
	require.NotEmpty(t, tags["traceparent"], "traceparent should be injected")

	extractedCtx := ExtractTraceContext(context.Background(), tags)
	sc := trace.SpanContextFromContext(extractedCtx)
	assert.True(t, sc.IsValid(), "extracted span context should be valid")
	assert.Equal(t, span.SpanContext().TraceID(), sc.TraceID(), "trace ID should match")
}

func TestExtractProducerLink_WithValidTrace(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(prev)

	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "producer-op")
	defer span.End()

	tags := InjectTraceContext(ctx, nil)

	link := ExtractProducerLink(tags)
	assert.True(t, link.SpanContext.IsValid())
	assert.Equal(t, span.SpanContext().TraceID(), link.SpanContext.TraceID())
	assert.Equal(t, span.SpanContext().SpanID(), link.SpanContext.SpanID())
}

func TestExtractTraceContext_WithPopulatedHeaders(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(prev)

	tags := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}

	ctx := ExtractTraceContext(context.Background(), tags)
	sc := trace.SpanContextFromContext(ctx)
	assert.True(t, sc.IsValid())
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", sc.TraceID().String())
	assert.Equal(t, "00f067aa0ba902b7", sc.SpanID().String())
}

func TestInjectTraceContext_PreservesExistingTags(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(prev)

	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test-op")
	defer span.End()

	tags := map[string]string{"user-tag": "user-value", "env": "staging"}
	result := InjectTraceContext(ctx, tags)

	assert.Equal(t, "user-value", result["user-tag"])
	assert.Equal(t, "staging", result["env"])
	assert.NotEmpty(t, result["traceparent"])
}
