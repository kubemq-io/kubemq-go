package kubemq

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testGenerateCA(t *testing.T) (certPEM, keyPEM []byte, cert *x509.Certificate, key *ecdsa.PrivateKey) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	require.NoError(t, err)

	parsedCert, err := x509.ParseCertificate(certDER)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	privDER, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})

	return certPEM, keyPEM, parsedCert, priv
}

func testGenerateCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey) (certPEM, keyPEM []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "Test Client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, ca, &priv.PublicKey, caKey)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	privDER, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})

	return certPEM, keyPEM
}

func TestWithTLS(t *testing.T) {
	caPEM, _, _, _ := testGenerateCA(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))

	opts := GetDefaultOptions()
	WithTLS(caFile).apply(opts)

	require.NotNil(t, opts.tlsConfig)
	assert.Equal(t, caFile, opts.tlsConfig.CACertFile)
}

func TestWithTLSFromPEM(t *testing.T) {
	caPEM, _, _, _ := testGenerateCA(t)

	opts := GetDefaultOptions()
	WithTLSFromPEM(caPEM).apply(opts)

	require.NotNil(t, opts.tlsConfig)
	assert.Equal(t, caPEM, opts.tlsConfig.CACertPEM)
}

func TestWithTLSConfig(t *testing.T) {
	cfg := &TLSConfig{ServerName: "kubemq.local"}
	opts := GetDefaultOptions()
	WithTLSConfig(cfg).apply(opts)

	require.NotNil(t, opts.tlsConfig)
	assert.Equal(t, "kubemq.local", opts.tlsConfig.ServerName)
}

func TestWithInsecureSkipVerify(t *testing.T) {
	opts := GetDefaultOptions()
	WithInsecureSkipVerify().apply(opts)

	assert.True(t, opts.insecureSkipVerify)
	assert.NotNil(t, opts.tlsConfig)
}

func TestWithServerNameOverride(t *testing.T) {
	opts := GetDefaultOptions()
	WithServerNameOverride("override.name").apply(opts)

	require.NotNil(t, opts.tlsConfig)
	assert.Equal(t, "override.name", opts.tlsConfig.ServerName)
}

func TestWithMTLS(t *testing.T) {
	opts := GetDefaultOptions()
	WithMTLS("/cert.pem", "/key.pem", "/ca.pem").apply(opts)

	require.NotNil(t, opts.tlsConfig)
	assert.Equal(t, "/cert.pem", opts.tlsConfig.CertFile)
	assert.Equal(t, "/key.pem", opts.tlsConfig.KeyFile)
	assert.Equal(t, "/ca.pem", opts.tlsConfig.CACertFile)
}

func TestWithMTLSFromPEM(t *testing.T) {
	opts := GetDefaultOptions()
	WithMTLSFromPEM([]byte("cert"), []byte("key"), []byte("ca")).apply(opts)

	require.NotNil(t, opts.tlsConfig)
	assert.Equal(t, []byte("cert"), opts.tlsConfig.CertPEM)
	assert.Equal(t, []byte("key"), opts.tlsConfig.KeyPEM)
	assert.Equal(t, []byte("ca"), opts.tlsConfig.CACertPEM)
}

func TestOptionsValidate_TLS_CACertFileNotFound(t *testing.T) {
	opts := GetDefaultOptions()
	opts.tlsConfig = &TLSConfig{CACertFile: "/nonexistent/ca.pem"}

	err := opts.Validate()
	require.Error(t, err)

	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "CA certificate file")
}

func TestOptionsValidate_TLS_InvalidCAPEM(t *testing.T) {
	opts := GetDefaultOptions()
	opts.tlsConfig = &TLSConfig{CACertPEM: []byte("not a real PEM")}

	err := opts.Validate()
	require.Error(t, err)

	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "no valid certificates")
}

func TestOptionsValidate_TLS_MinVersionTooLow(t *testing.T) {
	opts := GetDefaultOptions()
	opts.tlsConfig = &TLSConfig{MinVersion: tls.VersionTLS10}

	err := opts.Validate()
	require.Error(t, err)

	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestOptionsValidate_MTLS_MissingKey(t *testing.T) {
	caPEM, _, ca, caKey := testGenerateCA(t)
	certPEM, _ := testGenerateCert(t, ca, caKey)
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))

	opts := GetDefaultOptions()
	opts.tlsConfig = &TLSConfig{CACertFile: caFile, CertFile: certFile}

	err := opts.Validate()
	require.Error(t, err)

	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "both be provided")
}

func TestOptionsValidate_MTLS_MissingKeyPEM(t *testing.T) {
	opts := GetDefaultOptions()
	opts.tlsConfig = &TLSConfig{CertPEM: []byte("certdata")}

	err := opts.Validate()
	require.Error(t, err)

	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "both be provided")
}

func TestOptionsValidate_MTLS_InvalidCertKeyPair(t *testing.T) {
	caPEM, _, _, _ := testGenerateCA(t)
	_, _, ca2, caKey2 := testGenerateCA(t)
	_, keyPEM := testGenerateCert(t, ca2, caKey2)

	opts := GetDefaultOptions()
	opts.tlsConfig = &TLSConfig{
		CertPEM: caPEM,
		KeyPEM:  keyPEM,
	}

	err := opts.Validate()
	require.Error(t, err)

	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeAuthentication, kErr.Code)
}

func TestOptionsValidate_TLS_ValidConfig(t *testing.T) {
	caPEM, _, ca, caKey := testGenerateCA(t)
	certPEM, keyPEM := testGenerateCert(t, ca, caKey)
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))

	opts := GetDefaultOptions()
	opts.tlsConfig = &TLSConfig{
		CACertFile: caFile,
		CertFile:   certFile,
		KeyFile:    keyFile,
	}

	err := opts.Validate()
	require.NoError(t, err)
}

func TestOptionsString_Redacted(t *testing.T) {
	opts := GetDefaultOptions()
	WithAuthToken("super-secret-token-123").apply(opts)

	s := opts.String()
	assert.NotContains(t, s, "super-secret-token-123")
	assert.Contains(t, s, "credential_provider=true")
}

func TestOptionsString_NoCredentials(t *testing.T) {
	opts := GetDefaultOptions()
	s := opts.String()
	assert.Contains(t, s, "credential_provider=false")
	assert.Contains(t, s, "tls=false")
}

func TestOptionsString_WithTLS(t *testing.T) {
	caPEM, _, _, _ := testGenerateCA(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))

	opts := GetDefaultOptions()
	WithTLS(caFile).apply(opts)

	s := opts.String()
	assert.Contains(t, s, "tls=true")
}
