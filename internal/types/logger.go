package types

// Logger is the structured logging interface for the SDK.
// Defined in TYPE-REGISTRY.md, owned by 05-observability-spec.md.
type Logger interface {
	Debug(msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}
