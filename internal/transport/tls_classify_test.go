package transport

import (
	"crypto/x509"
	"errors"
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestClassifyTLSError_CertValidation(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "unknown authority",
			err:  x509.UnknownAuthorityError{},
		},
		{
			name: "hostname mismatch",
			err:  x509.HostnameError{Host: "wrong.host"},
		},
		{
			name: "certificate invalid",
			err:  x509.CertificateInvalidError{Reason: x509.Expired},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, retryable := ClassifyTLSError(tt.err)
			assert.Equal(t, types.ErrCodeAuthentication, code)
			assert.False(t, retryable)
		})
	}
}

func TestClassifyTLSError_VersionNegotiation(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "protocol version",
			err:  errors.New("tls: protocol version not supported"),
		},
		{
			name: "no mutual cipher suite",
			err:  errors.New("tls: no mutual cipher suite"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, retryable := ClassifyTLSError(tt.err)
			assert.Equal(t, types.ErrCodeValidation, code)
			assert.False(t, retryable)
		})
	}
}

func TestClassifyTLSError_Network(t *testing.T) {
	code, retryable := ClassifyTLSError(errors.New("connection reset by peer"))
	assert.Equal(t, types.ErrCodeTransient, code)
	assert.True(t, retryable)
}
