package types

import (
	"errors"

	"google.golang.org/grpc/codes"
)

// ErrorCode is a machine-readable, stable error code.
// New codes may be added in minor versions; existing codes are never removed
// or changed in meaning within a major version.
type ErrorCode string

const (
	ErrCodeTransient      ErrorCode = "TRANSIENT"
	ErrCodeTimeout        ErrorCode = "TIMEOUT"
	ErrCodeThrottling     ErrorCode = "THROTTLING"
	ErrCodeAuthentication ErrorCode = "AUTHENTICATION"
	ErrCodeAuthorization  ErrorCode = "AUTHORIZATION"
	ErrCodeValidation     ErrorCode = "VALIDATION"
	ErrCodeNotFound       ErrorCode = "NOT_FOUND"
	ErrCodeFatal          ErrorCode = "FATAL"
	ErrCodeCancellation   ErrorCode = "CANCELLATION"
	ErrCodeBackpressure   ErrorCode = "BACKPRESSURE"
)

func (c ErrorCode) String() string { return string(c) }

// ErrorCategory classifies errors for programmatic handling.
// ErrorCategory is not stored on KubeMQError — it is derived from Code
// via CategoryForCode(). Users should use KubeMQError.IsRetryable as the
// primary signal rather than inspecting the category directly.
type ErrorCategory int

const (
	CategoryTransient ErrorCategory = iota
	CategoryTimeout
	CategoryThrottling
	CategoryAuthentication
	CategoryAuthorization
	CategoryValidation
	CategoryNotFound
	CategoryFatal
	CategoryCancellation
	CategoryBackpressure
)

// KubeMQError is the primary structured error type returned by all SDK operations.
type KubeMQError struct {
	Code        ErrorCode
	Message     string
	Operation   string
	Channel     string
	IsRetryable bool
	Cause       error
	RequestID   string
}

func (e *KubeMQError) Error() string {
	var b []byte
	b = append(b, e.Operation...)
	if e.Channel != "" {
		b = append(b, " failed on channel \""...)
		b = append(b, e.Channel...)
		b = append(b, '"')
	} else {
		b = append(b, " failed"...)
	}
	b = append(b, ": "...)
	if e.Cause != nil {
		b = append(b, e.Cause.Error()...)
	} else if e.Message != "" {
		b = append(b, e.Message...)
	} else {
		b = append(b, e.Code.String()...)
	}
	if suggestion := ErrorSuggestions[e.Code]; suggestion != "" {
		b = append(b, "\n  Suggestion: "...)
		b = append(b, suggestion...)
	}
	return string(b)
}

func (e *KubeMQError) Unwrap() error { return e.Cause }

// Is supports matching by ErrorCode via errors.Is.
// Two KubeMQErrors with the same Code but different Operation/Channel/Message
// are considered equal by errors.Is. This enables patterns like:
//
//	errors.Is(err, &KubeMQError{Code: ErrCodeTimeout})
func (e *KubeMQError) Is(target error) bool {
	var t *KubeMQError
	if errors.As(target, &t) {
		return e.Code == t.Code
	}
	return false
}

// ErrorSuggestions maps error codes to actionable resolution guidance.
// Read-only after package init.
var ErrorSuggestions = map[ErrorCode]string{
	ErrCodeTransient:      "Retry the operation. If the problem persists, check server health.",
	ErrCodeTimeout:        "Increase the timeout or check server connectivity and firewall rules.",
	ErrCodeThrottling:     "Reduce request rate or increase server capacity.",
	ErrCodeAuthentication: "Check your auth token. It may have expired.",
	ErrCodeAuthorization:  "Verify the client has permissions for this channel.",
	ErrCodeValidation:     "Check the request parameters (channel, body, metadata).",
	ErrCodeNotFound:       "The channel or queue does not exist. Create it first.",
	ErrCodeFatal:          "This is an unrecoverable server error. Contact support.",
	ErrCodeCancellation:   "The operation was cancelled by the caller.",
	ErrCodeBackpressure:   "The reconnection buffer is full. Wait for reconnection or increase buffer size.",
}

// CategoryForCode maps an ErrorCode to its ErrorCategory.
func CategoryForCode(code ErrorCode) ErrorCategory {
	switch code {
	case ErrCodeTransient:
		return CategoryTransient
	case ErrCodeTimeout:
		return CategoryTimeout
	case ErrCodeThrottling:
		return CategoryThrottling
	case ErrCodeAuthentication:
		return CategoryAuthentication
	case ErrCodeAuthorization:
		return CategoryAuthorization
	case ErrCodeValidation:
		return CategoryValidation
	case ErrCodeNotFound:
		return CategoryNotFound
	case ErrCodeFatal:
		return CategoryFatal
	case ErrCodeCancellation:
		return CategoryCancellation
	case ErrCodeBackpressure:
		return CategoryBackpressure
	default:
		return CategoryFatal
	}
}

// IsRetryableCategory returns true for categories that are safe to retry.
func IsRetryableCategory(cat ErrorCategory) bool {
	switch cat {
	case CategoryTransient, CategoryTimeout, CategoryThrottling:
		return true
	default:
		return false
	}
}

// NewError constructs a KubeMQError with correct IsRetryable derived from the code.
func NewError(code ErrorCode, op, channel, requestID string, cause error) *KubeMQError {
	cat := CategoryForCode(code)
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	return &KubeMQError{
		Code:        code,
		Message:     msg,
		Operation:   op,
		Channel:     channel,
		IsRetryable: IsRetryableCategory(cat),
		Cause:       cause,
		RequestID:   requestID,
	}
}

// ClassifyGRPCCode maps a gRPC status code to an ErrorCategory and ErrorCode.
func ClassifyGRPCCode(code codes.Code) (ErrorCategory, ErrorCode) {
	switch code {
	case codes.OK:
		return CategoryTransient, ErrCodeTransient
	case codes.Canceled:
		return CategoryCancellation, ErrCodeCancellation
	case codes.Unknown:
		return CategoryTransient, ErrCodeTransient
	case codes.InvalidArgument:
		return CategoryValidation, ErrCodeValidation
	case codes.DeadlineExceeded:
		return CategoryTimeout, ErrCodeTimeout
	case codes.NotFound:
		return CategoryNotFound, ErrCodeNotFound
	case codes.AlreadyExists:
		return CategoryValidation, ErrCodeValidation
	case codes.PermissionDenied:
		return CategoryAuthorization, ErrCodeAuthorization
	case codes.ResourceExhausted:
		return CategoryThrottling, ErrCodeThrottling
	case codes.FailedPrecondition:
		return CategoryValidation, ErrCodeValidation
	case codes.Aborted:
		return CategoryTransient, ErrCodeTransient
	case codes.OutOfRange:
		return CategoryValidation, ErrCodeValidation
	case codes.Unimplemented:
		return CategoryFatal, ErrCodeFatal
	case codes.Internal:
		return CategoryFatal, ErrCodeFatal
	case codes.Unavailable:
		return CategoryTransient, ErrCodeTransient
	case codes.DataLoss:
		return CategoryFatal, ErrCodeFatal
	case codes.Unauthenticated:
		return CategoryAuthentication, ErrCodeAuthentication
	default:
		return CategoryFatal, ErrCodeFatal
	}
}
