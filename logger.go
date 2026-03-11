package kubemq

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// noopLogger discards all log output. This is the default Logger
// when no logger is configured via WithLogger.
type noopLogger struct{}

var _ Logger = noopLogger{}

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

// SlogAdapter wraps a *slog.Logger to implement the kubemq.Logger interface.
// This is the recommended logging integration for Go 1.21+.
//
// Usage:
//
//	client, _ := kubemq.NewClient(ctx,
//	    kubemq.WithLogger(kubemq.NewSlogAdapter(slog.Default())),
//	)
type SlogAdapter struct {
	logger *slog.Logger
}

// NewSlogAdapter creates a Logger backed by the given slog.Logger.
// If logger is nil, slog.Default() is used.
func NewSlogAdapter(logger *slog.Logger) *SlogAdapter {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlogAdapter{logger: logger}
}

var _ Logger = (*SlogAdapter)(nil)

func (a *SlogAdapter) Debug(msg string, keysAndValues ...any) {
	a.logger.Debug(msg, keysAndValues...)
}

func (a *SlogAdapter) Info(msg string, keysAndValues ...any) {
	a.logger.Info(msg, keysAndValues...)
}

func (a *SlogAdapter) Warn(msg string, keysAndValues ...any) {
	a.logger.Warn(msg, keysAndValues...)
}

func (a *SlogAdapter) Error(msg string, keysAndValues ...any) {
	a.logger.Error(msg, keysAndValues...)
}

// logWithTrace enriches keysAndValues with trace_id and span_id
// extracted from the context, if an active span exists.
func logWithTrace(ctx context.Context, keysAndValues []any) []any {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return keysAndValues
	}
	return append(keysAndValues,
		"trace_id", sc.TraceID().String(),
		"span_id", sc.SpanID().String(),
	)
}
