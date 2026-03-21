package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
)

// buildTLSConfig constructs a *tls.Config from the user-provided TLSConfig.
// It validates all inputs eagerly (fail-fast) and returns descriptive errors.
func buildTLSConfig(cfg *types.TLSConfig, skipVerify bool) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: skipVerify, //nolint:gosec // G402: user-controlled option; documented as insecure
	}

	if cfg == nil {
		return tlsCfg, nil
	}

	if cfg.MinVersion != 0 {
		if cfg.MinVersion < tls.VersionTLS12 {
			return nil, &types.KubeMQError{
				Code:    types.ErrCodeValidation,
				Message: fmt.Sprintf("minimum TLS version 0x%04x is below TLS 1.2 (0x%04x); HTTP/2 requires TLS 1.2+", cfg.MinVersion, tls.VersionTLS12),
			}
		}
		tlsCfg.MinVersion = cfg.MinVersion
	}

	if cfg.ServerName != "" {
		tlsCfg.ServerName = cfg.ServerName
	}

	if err := loadCACert(tlsCfg, cfg); err != nil {
		return nil, err
	}

	if err := loadClientCert(tlsCfg, cfg, nil); err != nil {
		return nil, err
	}

	return tlsCfg, nil
}

// buildTLSConfigWithLogger is like buildTLSConfig but wires a logger into
// the GetClientCertificate callback for cert reload diagnostics.
func buildTLSConfigWithLogger(cfg *types.TLSConfig, skipVerify bool, log logger) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: skipVerify, //nolint:gosec // G402: user-controlled option; documented as insecure
	}

	if cfg == nil {
		return tlsCfg, nil
	}

	if cfg.MinVersion != 0 {
		if cfg.MinVersion < tls.VersionTLS12 {
			return nil, &types.KubeMQError{
				Code:    types.ErrCodeValidation,
				Message: fmt.Sprintf("minimum TLS version 0x%04x is below TLS 1.2 (0x%04x); HTTP/2 requires TLS 1.2+", cfg.MinVersion, tls.VersionTLS12),
			}
		}
		tlsCfg.MinVersion = cfg.MinVersion
	}

	if cfg.ServerName != "" {
		tlsCfg.ServerName = cfg.ServerName
	}

	if err := loadCACert(tlsCfg, cfg); err != nil {
		return nil, err
	}

	if err := loadClientCert(tlsCfg, cfg, log); err != nil {
		return nil, err
	}

	return tlsCfg, nil
}

func loadCACert(tlsCfg *tls.Config, cfg *types.TLSConfig) error {
	var caPEM []byte

	switch {
	case cfg.CACertFile != "":
		data, err := os.ReadFile(cfg.CACertFile)
		if err != nil {
			return &types.KubeMQError{
				Code:        types.ErrCodeAuthentication,
				Message:     "failed to read CA certificate file",
				IsRetryable: false,
				Cause:       err,
			}
		}
		caPEM = data
	case len(cfg.CACertPEM) > 0:
		caPEM = cfg.CACertPEM
	default:
		return nil
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return &types.KubeMQError{
			Code:        types.ErrCodeAuthentication,
			Message:     "CA certificate PEM data contains no valid certificates",
			IsRetryable: false,
		}
	}
	tlsCfg.RootCAs = pool
	return nil
}

func loadClientCert(tlsCfg *tls.Config, cfg *types.TLSConfig, log logger) error {
	hasFile := cfg.CertFile != "" && cfg.KeyFile != ""
	hasPEM := len(cfg.CertPEM) > 0 && len(cfg.KeyPEM) > 0

	if !hasFile && !hasPEM {
		return nil
	}

	if hasFile {
		if log != nil {
			tlsCfg.GetClientCertificate = makeFileReloaderWithLogging(cfg.CertFile, cfg.KeyFile, log)
		} else {
			tlsCfg.GetClientCertificate = makeFileReloader(cfg.CertFile, cfg.KeyFile)
		}
	} else {
		cert, err := tls.X509KeyPair(cfg.CertPEM, cfg.KeyPEM)
		if err != nil {
			return &types.KubeMQError{
				Code:        types.ErrCodeAuthentication,
				Message:     "failed to parse client certificate/key PEM",
				IsRetryable: false,
				Cause:       err,
			}
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return nil
}

func makeFileReloader(certFile, keyFile string) func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to reload client certificate: %w", err)
		}
		return &cert, nil
	}
}

func makeFileReloaderWithLogging(certFile, keyFile string, log logger) func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
		log.Info("reloading client certificate", "cert_file", certFile, "key_file", keyFile)

		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Error("failed to reload client certificate", "cert_file", certFile, "error", err)
			return nil, fmt.Errorf("failed to reload client certificate: %w", err)
		}

		log.Info("client certificate reloaded successfully", "cert_file", certFile)
		return &cert, nil
	}
}
