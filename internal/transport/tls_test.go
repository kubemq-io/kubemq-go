package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateTestCA(t *testing.T) (certPEM, keyPEM []byte, cert *x509.Certificate, key *ecdsa.PrivateKey) {
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

func generateTestCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey) (certPEM, keyPEM []byte) {
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

func writeTestCertFiles(t *testing.T, certPEM, keyPEM []byte) (certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()

	certFile = filepath.Join(dir, "cert.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))

	keyFile = filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	return certFile, keyFile
}

func TestBuildTLSConfig_MinVersion_Default(t *testing.T) {
	cfg, err := buildTLSConfig(nil, false)
	require.NoError(t, err)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
}

func TestBuildTLSConfig_MinVersion_RejectsLower(t *testing.T) {
	_, err := buildTLSConfig(&types.TLSConfig{MinVersion: tls.VersionTLS10}, false)
	require.Error(t, err)

	var kErr *types.KubeMQError
	require.ErrorAs(t, err, &kErr)
	assert.Equal(t, types.ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "below TLS 1.2")
}

func TestBuildTLSConfig_MinVersion_AcceptsTLS13(t *testing.T) {
	cfg, err := buildTLSConfig(&types.TLSConfig{MinVersion: tls.VersionTLS13}, false)
	require.NoError(t, err)
	assert.Equal(t, uint16(tls.VersionTLS13), cfg.MinVersion)
}

func TestBuildTLSConfig_CACertPEM(t *testing.T) {
	caPEM, _, _, _ := generateTestCA(t)

	cfg, err := buildTLSConfig(&types.TLSConfig{CACertPEM: caPEM}, false)
	require.NoError(t, err)
	assert.NotNil(t, cfg.RootCAs)
}

func TestBuildTLSConfig_CACertFile(t *testing.T) {
	caPEM, _, _, _ := generateTestCA(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))

	cfg, err := buildTLSConfig(&types.TLSConfig{CACertFile: caFile}, false)
	require.NoError(t, err)
	assert.NotNil(t, cfg.RootCAs)
}

func TestBuildTLSConfig_InvalidCACert(t *testing.T) {
	_, err := buildTLSConfig(&types.TLSConfig{CACertPEM: []byte("not valid PEM")}, false)
	require.Error(t, err)

	var kErr *types.KubeMQError
	require.ErrorAs(t, err, &kErr)
	assert.Equal(t, types.ErrCodeAuthentication, kErr.Code)
}

func TestBuildTLSConfig_SystemCA(t *testing.T) {
	cfg, err := buildTLSConfig(&types.TLSConfig{}, false)
	require.NoError(t, err)
	assert.Nil(t, cfg.RootCAs, "nil RootCAs means system CA bundle is used")
}

func TestBuildTLSConfig_InsecureSkipVerify(t *testing.T) {
	cfg, err := buildTLSConfig(nil, true)
	require.NoError(t, err)
	assert.True(t, cfg.InsecureSkipVerify)
}

func TestBuildTLSConfig_ServerName(t *testing.T) {
	cfg, err := buildTLSConfig(&types.TLSConfig{ServerName: "custom.host.example.com"}, false)
	require.NoError(t, err)
	assert.Equal(t, "custom.host.example.com", cfg.ServerName)
}

func TestBuildTLSConfig_MTLS_PEM(t *testing.T) {
	caPEM, _, ca, caKey := generateTestCA(t)
	certPEM, keyPEM := generateTestCert(t, ca, caKey)

	cfg, err := buildTLSConfig(&types.TLSConfig{
		CACertPEM: caPEM,
		CertPEM:   certPEM,
		KeyPEM:    keyPEM,
	}, false)
	require.NoError(t, err)
	assert.Len(t, cfg.Certificates, 1, "should have one client certificate")
	assert.NotNil(t, cfg.RootCAs)
}

func TestBuildTLSConfig_MTLS_Files(t *testing.T) {
	caPEM, _, ca, caKey := generateTestCA(t)
	certPEM, keyPEM := generateTestCert(t, ca, caKey)
	certFile, keyFile := writeTestCertFiles(t, certPEM, keyPEM)

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))

	cfg, err := buildTLSConfig(&types.TLSConfig{
		CACertFile: caFile,
		CertFile:   certFile,
		KeyFile:    keyFile,
	}, false)
	require.NoError(t, err)
	assert.NotNil(t, cfg.GetClientCertificate, "file-based mTLS should use GetClientCertificate callback")
	assert.Empty(t, cfg.Certificates, "file-based should not set static Certificates")
}

func TestBuildTLSConfig_MTLS_InvalidPEM(t *testing.T) {
	_, err := buildTLSConfig(&types.TLSConfig{
		CertPEM: []byte("bad cert"),
		KeyPEM:  []byte("bad key"),
	}, false)
	require.Error(t, err)

	var kErr *types.KubeMQError
	require.ErrorAs(t, err, &kErr)
	assert.Equal(t, types.ErrCodeAuthentication, kErr.Code)
}

func TestMakeFileReloader(t *testing.T) {
	_, _, ca, caKey := generateTestCA(t)
	certPEM, keyPEM := generateTestCert(t, ca, caKey)
	certFile, keyFile := writeTestCertFiles(t, certPEM, keyPEM)

	reloader := makeFileReloader(certFile, keyFile)

	cert, err := reloader(nil)
	require.NoError(t, err)
	assert.NotNil(t, cert)
}

func TestMakeFileReloader_FileChanged(t *testing.T) {
	_, _, ca, caKey := generateTestCA(t)
	certPEM1, keyPEM1 := generateTestCert(t, ca, caKey)
	certFile, keyFile := writeTestCertFiles(t, certPEM1, keyPEM1)

	reloader := makeFileReloader(certFile, keyFile)

	cert1, err := reloader(nil)
	require.NoError(t, err)

	certPEM2, keyPEM2 := generateTestCert(t, ca, caKey)
	require.NoError(t, os.WriteFile(certFile, certPEM2, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM2, 0o600))

	cert2, err := reloader(nil)
	require.NoError(t, err)

	assert.NotEqual(t, cert1.Certificate, cert2.Certificate,
		"reloader should pick up changed certificate files")
}

func TestMakeFileReloader_FileMissing(t *testing.T) {
	reloader := makeFileReloader("/nonexistent/cert.pem", "/nonexistent/key.pem")

	_, err := reloader(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to reload client certificate")
}
