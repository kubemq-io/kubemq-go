package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
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

func TestBuildTLSConfigWithLogger_NilConfig(t *testing.T) {
	log := &testLogger{}
	cfg, err := buildTLSConfigWithLogger(nil, false, log)
	require.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.False(t, cfg.InsecureSkipVerify)
}

func TestBuildTLSConfigWithLogger_SkipVerify(t *testing.T) {
	log := &testLogger{}
	cfg, err := buildTLSConfigWithLogger(nil, true, log)
	require.NoError(t, err)
	assert.True(t, cfg.InsecureSkipVerify)
}

func TestBuildTLSConfigWithLogger_ServerName(t *testing.T) {
	log := &testLogger{}
	tlsCfg := &types.TLSConfig{
		ServerName: "example.com",
	}
	cfg, err := buildTLSConfigWithLogger(tlsCfg, false, log)
	require.NoError(t, err)
	assert.Equal(t, "example.com", cfg.ServerName)
}

func TestBuildTLSConfigWithLogger_MinVersionTooLow(t *testing.T) {
	log := &testLogger{}
	tlsCfg := &types.TLSConfig{
		MinVersion: 0x0301, // TLS 1.0
	}
	_, err := buildTLSConfigWithLogger(tlsCfg, false, log)
	assert.Error(t, err)
}

func TestBuildTLSConfigWithLogger_InvalidCACert(t *testing.T) {
	log := &testLogger{}
	tlsCfg := &types.TLSConfig{
		CACertFile: "/nonexistent/ca.pem",
	}
	_, err := buildTLSConfigWithLogger(tlsCfg, false, log)
	assert.Error(t, err)
}

func generateSelfSignedCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

func TestMakeFileReloaderWithLogging_Success(t *testing.T) {
	certPEM, keyPEM := generateSelfSignedCert(t)
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	log := &testLogger{}
	reloader := makeFileReloaderWithLogging(certFile, keyFile, log)
	cert, err := reloader(nil)
	require.NoError(t, err)
	assert.NotNil(t, cert)
	assert.True(t, log.contains("reloading client certificate"))
	assert.True(t, log.contains("client certificate reloaded successfully"))
}

func TestMakeFileReloaderWithLogging_Error(t *testing.T) {
	log := &testLogger{}
	reloader := makeFileReloaderWithLogging("/nonexistent/cert.pem", "/nonexistent/key.pem", log)
	cert, err := reloader(nil)
	assert.Error(t, err)
	assert.Nil(t, cert)
	assert.True(t, log.contains("failed to reload client certificate"))
}
