package middleware

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TagsCarrier adapts a message Tags map for OTel context propagation.
// Implements propagation.TextMapCarrier.
// TYPE-REGISTRY canonical definition (05-observability-spec.md).
type TagsCarrier struct {
	Tags map[string]string
}

var _ propagation.TextMapCarrier = (*TagsCarrier)(nil)

func (c *TagsCarrier) Get(key string) string {
	if c.Tags == nil {
		return ""
	}
	return c.Tags[key]
}

func (c *TagsCarrier) Set(key, value string) {
	if c.Tags == nil {
		c.Tags = make(map[string]string)
	}
	c.Tags[key] = value
}

func (c *TagsCarrier) Keys() []string {
	keys := make([]string, 0, len(c.Tags))
	for k := range c.Tags {
		keys = append(keys, k)
	}
	return keys
}

// InjectTraceContext injects the current span's trace context (traceparent,
// tracestate) into the message Tags map. Called before publishing.
func InjectTraceContext(ctx context.Context, tags map[string]string) map[string]string {
	if tags == nil {
		tags = make(map[string]string)
	}
	carrier := &TagsCarrier{Tags: tags}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Tags
}

// ExtractTraceContext extracts W3C Trace Context from message Tags and
// returns a context with the extracted span context. If no trace context
// is present, the original context is returned unchanged.
func ExtractTraceContext(ctx context.Context, tags map[string]string) context.Context {
	if tags == nil {
		return ctx
	}
	carrier := &TagsCarrier{Tags: tags}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// ExtractProducerLink extracts trace context from message Tags and returns
// a trace.Link suitable for linking a consumer span to the producer span.
// Returns an empty Link if no trace context is present.
func ExtractProducerLink(tags map[string]string) trace.Link {
	if tags == nil {
		return trace.Link{}
	}
	carrier := &TagsCarrier{Tags: tags}
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return trace.Link{}
	}
	return trace.Link{SpanContext: sc}
}
