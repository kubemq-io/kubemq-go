package kubemq

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestKubeMQError_Error(t *testing.T) {
	err := &KubeMQError{
		Code:      ErrCodeTimeout,
		Message:   "deadline exceeded",
		Operation: "SendEvent",
		Channel:   "orders.events",
		Cause:     errors.New("context deadline exceeded"),
	}
	got := err.Error()
	assert.Contains(t, got, `SendEvent failed on channel "orders.events"`)
	assert.Contains(t, got, "context deadline exceeded")
	assert.Contains(t, got, "Suggestion:")
}

func TestKubeMQError_Unwrap(t *testing.T) {
	inner := errors.New("connection refused")
	err := &KubeMQError{Code: ErrCodeTransient, Cause: inner}
	assert.ErrorIs(t, err, inner)
}

func TestKubeMQError_As(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &KubeMQError{
		Code:      ErrCodeAuthentication,
		Operation: "Subscribe",
	})
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeAuthentication, kErr.Code)
	assert.Equal(t, "Subscribe", kErr.Operation)
}

func TestKubeMQError_Is(t *testing.T) {
	err1 := &KubeMQError{Code: ErrCodeTimeout}
	err2 := &KubeMQError{Code: ErrCodeTimeout}
	err3 := &KubeMQError{Code: ErrCodeFatal}
	assert.True(t, errors.Is(err1, err2))
	assert.False(t, errors.Is(err1, err3))
}

func TestAllErrorCodesHaveSuggestions(t *testing.T) {
	allCodes := []ErrorCode{
		ErrCodeTransient, ErrCodeTimeout, ErrCodeThrottling,
		ErrCodeAuthentication, ErrCodeAuthorization, ErrCodeValidation,
		ErrCodeNotFound, ErrCodeFatal, ErrCodeCancellation, ErrCodeBackpressure,
	}
	for _, code := range allCodes {
		assert.NotEmpty(t, errorSuggestions[code], "missing suggestion for %s", code)
	}
}

func TestBufferFullError(t *testing.T) {
	err := &BufferFullError{BufferSize: 1000, QueuedCount: 1000}
	assert.Contains(t, err.Error(), "capacity=1000")
	assert.Contains(t, err.Error(), "queued=1000")
}

func TestStreamBrokenError(t *testing.T) {
	err := &StreamBrokenError{UnacknowledgedIDs: []string{"a", "b", "c"}}
	assert.Contains(t, err.Error(), "3 unacknowledged messages")
}

func TestTransportError_Unwrap(t *testing.T) {
	inner := errors.New("connection reset")
	err := &TransportError{Op: "dial", Cause: inner}
	assert.ErrorIs(t, err, inner)
	assert.Contains(t, err.Error(), "kubemq transport [dial]")
}

func TestHandlerError_Unwrap(t *testing.T) {
	inner := errors.New("nil pointer")
	err := &HandlerError{Handler: "onMessage", Cause: inner}
	assert.ErrorIs(t, err, inner)
	assert.Contains(t, err.Error(), "kubemq handler [onMessage]")
}

func TestSentinelErrors(t *testing.T) {
	assert.ErrorIs(t, ErrClientClosed, ErrClientClosed)
	assert.ErrorIs(t, ErrValidation, ErrValidation)
	assert.ErrorIs(t, ErrNotImplemented, ErrNotImplemented)

	wrapped := fmt.Errorf("outer: %w", ErrClientClosed)
	assert.ErrorIs(t, wrapped, ErrClientClosed)
}

func TestClassifyGRPCCode_AllCodes(t *testing.T) {
	tests := []struct {
		code     codes.Code
		wantCat  ErrorCategory
		wantCode ErrorCode
	}{
		{codes.OK, CategoryCancellation, ErrCodeCancellation},
		{codes.Canceled, CategoryCancellation, ErrCodeCancellation},
		{codes.Unknown, CategoryTransient, ErrCodeTransient},
		{codes.InvalidArgument, CategoryValidation, ErrCodeValidation},
		{codes.DeadlineExceeded, CategoryTimeout, ErrCodeTimeout},
		{codes.NotFound, CategoryNotFound, ErrCodeNotFound},
		{codes.AlreadyExists, CategoryValidation, ErrCodeValidation},
		{codes.PermissionDenied, CategoryAuthorization, ErrCodeAuthorization},
		{codes.ResourceExhausted, CategoryThrottling, ErrCodeThrottling},
		{codes.FailedPrecondition, CategoryValidation, ErrCodeValidation},
		{codes.Aborted, CategoryTransient, ErrCodeTransient},
		{codes.OutOfRange, CategoryValidation, ErrCodeValidation},
		{codes.Unimplemented, CategoryFatal, ErrCodeFatal},
		{codes.Internal, CategoryFatal, ErrCodeFatal},
		{codes.Unavailable, CategoryTransient, ErrCodeTransient},
		{codes.DataLoss, CategoryFatal, ErrCodeFatal},
		{codes.Unauthenticated, CategoryAuthentication, ErrCodeAuthentication},
	}
	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			gotCat, gotCode := ClassifyGRPCCode(tt.code)
			assert.Equal(t, tt.wantCat, gotCat)
			assert.Equal(t, tt.wantCode, gotCode)
		})
	}
}

func TestIsRetryableCategory(t *testing.T) {
	retryable := []ErrorCategory{CategoryTransient, CategoryTimeout, CategoryThrottling}
	notRetryable := []ErrorCategory{
		CategoryAuthentication, CategoryAuthorization, CategoryValidation,
		CategoryNotFound, CategoryFatal, CategoryCancellation, CategoryBackpressure,
	}
	for _, cat := range retryable {
		assert.True(t, IsRetryableCategory(cat), "expected retryable: %d", cat)
	}
	for _, cat := range notRetryable {
		assert.False(t, IsRetryableCategory(cat), "expected not retryable: %d", cat)
	}
}

func TestActionableMessage_OperationAndChannel(t *testing.T) {
	err := &KubeMQError{
		Code:      ErrCodeTimeout,
		Operation: "SendEvent",
		Channel:   "orders.events",
		Cause:     errors.New("connection timeout after 5s"),
	}
	msg := err.Error()
	assert.Contains(t, msg, "SendEvent")
	assert.Contains(t, msg, `channel "orders.events"`)
	assert.Contains(t, msg, "connection timeout after 5s")
	assert.Contains(t, msg, "Suggestion:")
}

func TestActionableMessage_NoChannel(t *testing.T) {
	err := &KubeMQError{
		Code:      ErrCodeValidation,
		Operation: "NewClient",
		Cause:     errors.New("invalid host"),
	}
	msg := err.Error()
	assert.Contains(t, msg, "NewClient failed:")
	assert.NotContains(t, msg, `failed on channel`)
}

func TestActionableMessage_RetryExhaustion(t *testing.T) {
	err := &KubeMQError{
		Code:      ErrCodeTransient,
		Message:   "retries exhausted: 3/3 attempts over 12.4s",
		Operation: "SendEvent",
		Channel:   "orders.events",
	}
	msg := err.Error()
	assert.Contains(t, msg, "retries exhausted")
	assert.Contains(t, msg, "3/3 attempts")
}

func TestAsyncError_TransportError(t *testing.T) {
	var received error
	handler := func(err error) {
		received = err
	}
	transportErr := &TransportError{Op: "Subscribe", Cause: errors.New("stream broken")}
	handler(transportErr)
	var tErr *TransportError
	require.True(t, errors.As(received, &tErr))
	assert.Equal(t, "Subscribe", tErr.Op)
}

func TestAsyncError_HandlerVsTransport(t *testing.T) {
	transportErr := &TransportError{Op: "recv", Cause: errors.New("EOF")}
	handlerErr := &HandlerError{Handler: "onMsg", Cause: errors.New("nil pointer")}

	var tErr *TransportError
	var hErr *HandlerError
	assert.True(t, errors.As(transportErr, &tErr))
	assert.False(t, errors.As(transportErr, &hErr))
	assert.True(t, errors.As(handlerErr, &hErr))
	assert.False(t, errors.As(handlerErr, &tErr))
}

func TestAsyncError_PanicRecovery(t *testing.T) {
	var received error
	onError := func(err error) { received = err }

	func() {
		defer func() {
			if r := recover(); r != nil {
				onError(&HandlerError{Handler: "test", Cause: fmt.Errorf("panic: %v", r)})
			}
		}()
		panic("test panic")
	}()

	var hErr *HandlerError
	require.True(t, errors.As(received, &hErr))
	assert.Contains(t, hErr.Error(), "test panic")
}

func TestStreamReconnector_OnStreamBroken(t *testing.T) {
	sr := newStreamReconnector(DefaultRetryPolicy(), nil)
	sr.trackMessage("msg-1")
	sr.trackMessage("msg-2")

	var reportedErr error
	sr.onStreamBroken(func(err error) { reportedErr = err })

	var sErr *StreamBrokenError
	require.True(t, errors.As(reportedErr, &sErr))
	assert.Len(t, sErr.UnacknowledgedIDs, 2)
}

func TestStreamReconnector_AckMessage(t *testing.T) {
	sr := newStreamReconnector(DefaultRetryPolicy(), nil)
	sr.trackMessage("msg-1")
	sr.ackMessage("msg-1")
	ids := sr.unacknowledgedIDs()
	assert.Empty(t, ids)
}

func TestWithOnError(t *testing.T) {
	called := false
	opt := WithOnError(func(_ error) { called = true })
	cfg := &subscribeConfig{}
	opt(cfg)
	require.NotNil(t, cfg.onError)
	cfg.onError(errors.New("test"))
	assert.True(t, called)
}

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	assert.Equal(t, 3, p.MaxRetries)
	assert.Equal(t, 100*time.Millisecond, p.InitialBackoff)
	assert.Equal(t, 10*time.Second, p.MaxBackoff)
	assert.Equal(t, 2.0, p.Multiplier)
	assert.Equal(t, JitterFull, p.JitterMode)
}
