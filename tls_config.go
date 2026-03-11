package kubemq

import (
	"github.com/kubemq-io/kubemq-go/v2/internal/types"
)

// TLSConfig holds TLS configuration for the client connection.
// Re-exported from internal/types for public API stability.
type TLSConfig = types.TLSConfig

// WithTLS enables TLS with a CA certificate file.
// The system CA bundle is used if caCertFile is empty.
func WithTLS(caCertFile string) Option {
	return newFuncOption(func(o *Options) {
		o.tlsConfig = &TLSConfig{CACertFile: caCertFile}
	})
}

// WithTLSFromPEM enables TLS with CA certificate PEM bytes.
func WithTLSFromPEM(caPEM []byte) Option {
	return newFuncOption(func(o *Options) {
		o.tlsConfig = &TLSConfig{CACertPEM: caPEM}
	})
}

// WithTLSConfig enables TLS with a full TLSConfig.
func WithTLSConfig(cfg *TLSConfig) Option {
	return newFuncOption(func(o *Options) {
		o.tlsConfig = cfg
	})
}

// WithInsecureSkipVerify disables TLS certificate verification.
// WARNING: This should only be used in development/testing environments.
// A warning is logged on every connection attempt when this is active.
func WithInsecureSkipVerify() Option {
	return newFuncOption(func(o *Options) {
		o.insecureSkipVerify = true
		if o.tlsConfig == nil {
			o.tlsConfig = &TLSConfig{}
		}
	})
}

// WithServerNameOverride sets the server name used for TLS verification.
// Use this when connecting to a server whose certificate does not match
// the hostname being connected to (e.g., connecting by IP to a server
// with a DNS-name certificate).
func WithServerNameOverride(name string) Option {
	return newFuncOption(func(o *Options) {
		if o.tlsConfig == nil {
			o.tlsConfig = &TLSConfig{}
		}
		o.tlsConfig.ServerName = name
	})
}

// WithMTLS enables mutual TLS from certificate files.
// certFile: path to client certificate PEM
// keyFile: path to client private key PEM
// caFile: path to CA certificate PEM for verifying the server
func WithMTLS(certFile, keyFile, caFile string) Option {
	return newFuncOption(func(o *Options) {
		o.tlsConfig = &TLSConfig{
			CACertFile: caFile,
			CertFile:   certFile,
			KeyFile:    keyFile,
		}
	})
}

// WithMTLSFromPEM enables mutual TLS from in-memory PEM bytes.
func WithMTLSFromPEM(certPEM, keyPEM, caPEM []byte) Option {
	return newFuncOption(func(o *Options) {
		o.tlsConfig = &TLSConfig{
			CACertPEM: caPEM,
			CertPEM:   certPEM,
			KeyPEM:    keyPEM,
		}
	})
}
