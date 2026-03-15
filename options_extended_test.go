package kubemq

import (
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithCredentials(t *testing.T) {
	opts := GetDefaultOptions()
	WithCredentials("/path/to/cert.pem", "example.com").apply(opts)
	assert.True(t, opts.isSecured)
	assert.Equal(t, "/path/to/cert.pem", opts.certFile)
	assert.Equal(t, "example.com", opts.serverOverrideDomain)
}

func TestWithCertificate(t *testing.T) {
	opts := GetDefaultOptions()
	WithCertificate("CERT-DATA-PEM", "example.com").apply(opts)
	assert.True(t, opts.isSecured)
	assert.Equal(t, "CERT-DATA-PEM", opts.certData)
	assert.Equal(t, "example.com", opts.serverOverrideDomain)
}

func TestWithReceiveBufferSize(t *testing.T) {
	opts := GetDefaultOptions()
	WithReceiveBufferSize(256).apply(opts)
	assert.Equal(t, 256, opts.receiveBufferSize)
}

func TestWithDefaultChannel(t *testing.T) {
	opts := GetDefaultOptions()
	WithDefaultChannel("my-channel").apply(opts)
	assert.Equal(t, "my-channel", opts.defaultChannel)
}

func TestWithDefaultCacheTTL(t *testing.T) {
	opts := GetDefaultOptions()
	WithDefaultCacheTTL(30 * time.Minute).apply(opts)
	assert.Equal(t, 30*time.Minute, opts.defaultCacheTTL)
}

func TestWithCheckConnection(t *testing.T) {
	opts := GetDefaultOptions()
	assert.False(t, opts.checkConnection)
	WithCheckConnection(true).apply(opts)
	assert.True(t, opts.checkConnection)
}

func TestWithRetryPolicy(t *testing.T) {
	opts := GetDefaultOptions()
	p := RetryPolicy{MaxRetries: 5}
	WithRetryPolicy(p).apply(opts)
	assert.Equal(t, 5, opts.retryPolicy.MaxRetries)
}

func TestWithMaxConcurrentRetries(t *testing.T) {
	opts := GetDefaultOptions()
	WithMaxConcurrentRetries(20).apply(opts)
	assert.Equal(t, 20, opts.maxConcurrentRetries)
}

func TestWithLogger(t *testing.T) {
	opts := GetDefaultOptions()
	adapter := NewSlogAdapter(nil)
	WithLogger(adapter).apply(opts)
	assert.NotNil(t, opts.logger)
}

func TestWithTracerProvider(t *testing.T) {
	opts := GetDefaultOptions()
	WithTracerProvider("dummy-tp").apply(opts)
	assert.Equal(t, "dummy-tp", opts.tracerProvider)
}

func TestWithMeterProvider(t *testing.T) {
	opts := GetDefaultOptions()
	WithMeterProvider("dummy-mp").apply(opts)
	assert.Equal(t, "dummy-mp", opts.meterProvider)
}

func TestWithCardinalityThreshold(t *testing.T) {
	opts := GetDefaultOptions()
	WithCardinalityThreshold(50).apply(opts)
	assert.Equal(t, 50, opts.cardinalityThreshold)
}

func TestWithCardinalityAllowlist(t *testing.T) {
	opts := GetDefaultOptions()
	channels := []string{"ch-a", "ch-b"}
	WithCardinalityAllowlist(channels).apply(opts)
	assert.Equal(t, channels, opts.cardinalityAllowlist)
}

func TestOptions_Validate_InvalidClientID(t *testing.T) {
	opts := GetDefaultOptions()
	opts.clientId = "\xff\xfe"
	err := opts.Validate()
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "invalid UTF-8")
}

func TestOptions_ValidateTLS_ValidCACertPEM(t *testing.T) {
	caPEM, _, _, _ := testGenerateCA(t)
	opts := GetDefaultOptions()
	opts.tlsConfig = &TLSConfig{CACertPEM: caPEM}
	err := opts.Validate()
	assert.NoError(t, err)
}

func TestOptions_ValidateTLS_ValidMinVersion(t *testing.T) {
	opts := GetDefaultOptions()
	opts.tlsConfig = &TLSConfig{MinVersion: tls.VersionTLS13}
	err := opts.Validate()
	assert.NoError(t, err)
}

func TestOptions_ValidateTLS_ValidMTLSFromPEM(t *testing.T) {
	caPEM, _, ca, caKey := testGenerateCA(t)
	certPEM, keyPEM := testGenerateCert(t, ca, caKey)
	opts := GetDefaultOptions()
	opts.tlsConfig = &TLSConfig{
		CACertPEM: caPEM,
		CertPEM:   certPEM,
		KeyPEM:    keyPEM,
	}
	err := opts.Validate()
	assert.NoError(t, err)
}

func TestWithClientId(t *testing.T) {
	opts := GetDefaultOptions()
	WithClientId("my-custom-client").apply(opts)
	assert.Equal(t, "my-custom-client", opts.clientId)
}
