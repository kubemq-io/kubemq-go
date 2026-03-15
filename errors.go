package kubemq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	grpccodes "google.golang.org/grpc/codes"
)

// Re-export error types from internal/types so middleware and root share the
// same type identity. This avoids import cycles (root → transport → middleware → types).

// KubeMQError is the primary structured error type returned by all SDK operations.
// Immutable after construction. Safe to read from multiple goroutines.
type KubeMQError = types.KubeMQError

// ErrorCode is a machine-readable, stable error code.
type ErrorCode = types.ErrorCode

// ErrorCategory classifies errors for programmatic handling.
type ErrorCategory = types.ErrorCategory

const (
	ErrCodeTransient      = types.ErrCodeTransient
	ErrCodeTimeout        = types.ErrCodeTimeout
	ErrCodeThrottling     = types.ErrCodeThrottling
	ErrCodeAuthentication = types.ErrCodeAuthentication
	ErrCodeAuthorization  = types.ErrCodeAuthorization
	ErrCodeValidation     = types.ErrCodeValidation
	ErrCodeNotFound       = types.ErrCodeNotFound
	ErrCodeFatal          = types.ErrCodeFatal
	ErrCodeCancellation   = types.ErrCodeCancellation
	ErrCodeBackpressure   = types.ErrCodeBackpressure
)

const (
	CategoryTransient      = types.CategoryTransient
	CategoryTimeout        = types.CategoryTimeout
	CategoryThrottling     = types.CategoryThrottling
	CategoryAuthentication = types.CategoryAuthentication
	CategoryAuthorization  = types.CategoryAuthorization
	CategoryValidation     = types.CategoryValidation
	CategoryNotFound       = types.CategoryNotFound
	CategoryFatal          = types.CategoryFatal
	CategoryCancellation   = types.CategoryCancellation
	CategoryBackpressure   = types.CategoryBackpressure
)

// Sentinel errors.
var (
	ErrClientClosed   = errors.New("kubemq: client closed")
	ErrNotImplemented = errors.New("kubemq: not implemented")
	ErrValidation     = errors.New("kubemq: validation failed")
)

// errorSuggestions aliases the suggestions map for root-package test access.
var errorSuggestions = types.ErrorSuggestions

// ClassifyGRPCCode maps a gRPC status code to an ErrorCategory and ErrorCode.
func ClassifyGRPCCode(code grpccodes.Code) (cat ErrorCategory, ec ErrorCode) {
	return types.ClassifyGRPCCode(code)
}

// IsRetryableCategory returns true for categories that are safe to retry.
func IsRetryableCategory(cat ErrorCategory) bool {
	return types.IsRetryableCategory(cat)
}

// errNoTransport returns a typed error for operations called on an
// uninitialized or closed client.
func errNoTransport(op string) error {
	return &KubeMQError{
		Code:      ErrCodeValidation,
		Message:   "client not initialized — call NewClient() first",
		Operation: op,
		Cause:     ErrValidation,
	}
}

// BufferFullError is returned when the reconnection message buffer overflows.
type BufferFullError struct {
	BufferSize  int
	QueuedCount int
}

func (e *BufferFullError) Error() string {
	return fmt.Sprintf("kubemq: reconnection buffer full (capacity=%d, queued=%d)", e.BufferSize, e.QueuedCount)
}

// StreamBrokenError is returned when a stream breaks with unacknowledged messages.
type StreamBrokenError struct {
	UnacknowledgedIDs []string
}

func (e *StreamBrokenError) Error() string {
	return fmt.Sprintf("kubemq: stream broken, %d unacknowledged messages", len(e.UnacknowledgedIDs))
}

// TransportError wraps a transport-layer error.
type TransportError struct {
	Op    string
	Cause error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("kubemq transport [%s]: %v", e.Op, e.Cause)
}

func (e *TransportError) Unwrap() error { return e.Cause }

// HandlerError wraps a user handler panic/error.
type HandlerError struct {
	Handler string
	Cause   error
}

func (e *HandlerError) Error() string {
	return fmt.Sprintf("kubemq handler [%s]: %v", e.Handler, e.Cause)
}

func (e *HandlerError) Unwrap() error { return e.Cause }

// Default timeout constants applied when the user's context has no deadline.
const (
	DefaultSendTimeout      = 5 * time.Second
	DefaultSubscribeTimeout = 10 * time.Second
	DefaultRPCTimeout       = 10 * time.Second
	DefaultQueueRecvTimeout = 10 * time.Second
	DefaultQueuePollTimeout = 30 * time.Second
)

// SubscribeOption configures subscription behavior.
type SubscribeOption func(*subscribeConfig)

type subscribeConfig struct {
	onError             func(error)
	onEvent             func(*Event)
	onEventStoreReceive func(*EventStoreReceive)
	onCommandReceive    func(*CommandReceive)
	onQueryReceive      func(*QueryReceive)
}

// WithOnError registers a callback invoked when a subscription encounters
// a transport or handler error. If not set, errors are logged at ERROR level.
func WithOnError(handler func(error)) SubscribeOption {
	return func(cfg *subscribeConfig) {
		cfg.onError = handler
	}
}

// WithOnEvent sets the handler for received events. Required for SubscribeToEvents.
func WithOnEvent(handler func(*Event)) SubscribeOption {
	return func(cfg *subscribeConfig) {
		cfg.onEvent = handler
	}
}

// WithOnEventStoreReceive sets the handler for received events store messages.
// Required for SubscribeToEventsStore.
func WithOnEventStoreReceive(handler func(*EventStoreReceive)) SubscribeOption {
	return func(cfg *subscribeConfig) {
		cfg.onEventStoreReceive = handler
	}
}

// WithOnCommandReceive sets the handler for received commands. Required for SubscribeToCommands.
func WithOnCommandReceive(handler func(*CommandReceive)) SubscribeOption {
	return func(cfg *subscribeConfig) {
		cfg.onCommandReceive = handler
	}
}

// WithOnQueryReceive sets the handler for received queries. Required for SubscribeToQueries.
func WithOnQueryReceive(handler func(*QueryReceive)) SubscribeOption {
	return func(cfg *subscribeConfig) {
		cfg.onQueryReceive = handler
	}
}

// streamReconnector manages stream reconnection and in-flight message tracking.
type streamReconnector struct {
	policy   RetryPolicy
	logger   Logger
	inFlight sync.Map
}

func newStreamReconnector(policy RetryPolicy, logger Logger) *streamReconnector {
	return &streamReconnector{
		policy: policy,
		logger: logger,
	}
}

func (sr *streamReconnector) trackMessage(id string) {
	sr.inFlight.Store(id, struct{}{})
}

func (sr *streamReconnector) ackMessage(id string) {
	sr.inFlight.Delete(id)
}

func (sr *streamReconnector) unacknowledgedIDs() []string {
	var ids []string
	sr.inFlight.Range(func(key, _ any) bool {
		if id, ok := key.(string); ok {
			ids = append(ids, id)
		}
		return true
	})
	return ids
}

func (sr *streamReconnector) onStreamBroken(errCb func(error)) {
	ids := sr.unacknowledgedIDs()
	if len(ids) > 0 {
		errCb(&StreamBrokenError{UnacknowledgedIDs: ids})
	}
	sr.inFlight = sync.Map{}
}
