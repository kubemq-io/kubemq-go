package types

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestErrorCode_String(t *testing.T) {
	assert.Equal(t, "TRANSIENT", ErrCodeTransient.String())
	assert.Equal(t, "TIMEOUT", ErrCodeTimeout.String())
	assert.Equal(t, "AUTHENTICATION", ErrCodeAuthentication.String())
}

func TestKubeMQError_Error_WithCause(t *testing.T) {
	err := &KubeMQError{
		Code:      ErrCodeTimeout,
		Operation: "SendEvent",
		Channel:   "test-ch",
		Cause:     errors.New("deadline exceeded"),
	}
	msg := err.Error()
	assert.Contains(t, msg, "SendEvent")
	assert.Contains(t, msg, "test-ch")
	assert.Contains(t, msg, "deadline exceeded")
}

func TestKubeMQError_Error_WithMessage(t *testing.T) {
	err := &KubeMQError{
		Code:      ErrCodeValidation,
		Operation: "SendCommand",
		Message:   "channel required",
	}
	msg := err.Error()
	assert.Contains(t, msg, "channel required")
}

func TestKubeMQError_Error_CodeOnly(t *testing.T) {
	err := &KubeMQError{
		Code:      ErrCodeFatal,
		Operation: "Ping",
	}
	msg := err.Error()
	assert.Contains(t, msg, "FATAL")
}

func TestKubeMQError_Unwrap(t *testing.T) {
	cause := errors.New("root")
	err := &KubeMQError{Code: ErrCodeTransient, Cause: cause}
	assert.Equal(t, cause, err.Unwrap())
}

func TestKubeMQError_Is(t *testing.T) {
	err1 := &KubeMQError{Code: ErrCodeTimeout, Operation: "A"}
	err2 := &KubeMQError{Code: ErrCodeTimeout, Operation: "B"}
	err3 := &KubeMQError{Code: ErrCodeValidation, Operation: "A"}

	assert.True(t, err1.Is(err2))
	assert.False(t, err1.Is(err3))
	assert.False(t, err1.Is(errors.New("plain")))
}

func TestCategoryForCode_AllCodes(t *testing.T) {
	cases := map[ErrorCode]ErrorCategory{
		ErrCodeTransient:      CategoryTransient,
		ErrCodeTimeout:        CategoryTimeout,
		ErrCodeThrottling:     CategoryThrottling,
		ErrCodeAuthentication: CategoryAuthentication,
		ErrCodeAuthorization:  CategoryAuthorization,
		ErrCodeValidation:     CategoryValidation,
		ErrCodeNotFound:       CategoryNotFound,
		ErrCodeFatal:          CategoryFatal,
		ErrCodeCancellation:   CategoryCancellation,
		ErrCodeBackpressure:   CategoryBackpressure,
	}
	for code, want := range cases {
		assert.Equal(t, want, CategoryForCode(code), "code=%s", code)
	}
	assert.Equal(t, CategoryFatal, CategoryForCode("UNKNOWN_CODE"))
}

func TestIsRetryableCategory(t *testing.T) {
	assert.True(t, IsRetryableCategory(CategoryTransient))
	assert.True(t, IsRetryableCategory(CategoryTimeout))
	assert.True(t, IsRetryableCategory(CategoryThrottling))
	assert.False(t, IsRetryableCategory(CategoryAuthentication))
	assert.False(t, IsRetryableCategory(CategoryValidation))
	assert.False(t, IsRetryableCategory(CategoryFatal))
}

func TestNewError(t *testing.T) {
	cause := errors.New("test")
	err := NewError(ErrCodeTransient, "SendEvent", "ch1", "req-1", cause)
	require.NotNil(t, err)
	assert.Equal(t, ErrCodeTransient, err.Code)
	assert.Equal(t, "SendEvent", err.Operation)
	assert.Equal(t, "ch1", err.Channel)
	assert.Equal(t, "req-1", err.RequestID)
	assert.True(t, err.IsRetryable)
	assert.Equal(t, cause, err.Cause)
}

func TestNewError_NilCause(t *testing.T) {
	err := NewError(ErrCodeFatal, "Ping", "", "", nil)
	assert.Equal(t, "", err.Message)
	assert.False(t, err.IsRetryable)
}

func TestClassifyGRPCCode_AllCodes(t *testing.T) {
	tests := []struct {
		code     codes.Code
		wantCat  ErrorCategory
		wantCode ErrorCode
	}{
		{codes.OK, CategoryTransient, ErrCodeTransient},
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
		cat, code := ClassifyGRPCCode(tt.code)
		assert.Equal(t, tt.wantCat, cat, "grpc code=%v", tt.code)
		assert.Equal(t, tt.wantCode, code, "grpc code=%v", tt.code)
	}
}

func TestErrorSuggestions_HasAllCodes(t *testing.T) {
	allCodes := []ErrorCode{
		ErrCodeTransient, ErrCodeTimeout, ErrCodeThrottling,
		ErrCodeAuthentication, ErrCodeAuthorization, ErrCodeValidation,
		ErrCodeNotFound, ErrCodeFatal, ErrCodeCancellation, ErrCodeBackpressure,
	}
	for _, code := range allCodes {
		suggestion := ErrorSuggestions[code]
		assert.NotEmpty(t, suggestion, "missing suggestion for %s", code)
	}
}
