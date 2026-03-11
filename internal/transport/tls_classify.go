package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"strings"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
)

// ClassifyTLSError classifies a TLS-related error into an ErrorCode and
// retryable flag. Used by the transport layer to provide structured error
// information for TLS handshake failures.
func ClassifyTLSError(err error) (types.ErrorCode, bool) {
	var certErr *tls.CertificateVerificationError
	if errors.As(err, &certErr) {
		return types.ErrCodeAuthentication, false
	}

	var x509Err x509.UnknownAuthorityError
	if errors.As(err, &x509Err) {
		return types.ErrCodeAuthentication, false
	}

	var hostErr x509.HostnameError
	if errors.As(err, &hostErr) {
		return types.ErrCodeAuthentication, false
	}

	var certInvalidErr x509.CertificateInvalidError
	if errors.As(err, &certInvalidErr) {
		return types.ErrCodeAuthentication, false
	}

	errMsg := err.Error()
	if strings.Contains(errMsg, "protocol version") ||
		strings.Contains(errMsg, "no mutual cipher suite") {
		return types.ErrCodeValidation, false
	}

	return types.ErrCodeTransient, true
}
