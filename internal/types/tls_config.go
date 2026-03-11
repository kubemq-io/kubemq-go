package types

// TLSConfig holds TLS configuration for the client connection.
// All fields are optional; sensible defaults are applied.
type TLSConfig struct {
	CACertFile string // Path to CA certificate PEM file
	CACertPEM  []byte // CA certificate PEM bytes (alternative to file)
	CertFile   string // Client certificate PEM file (for mTLS)
	KeyFile    string // Client private key PEM file (for mTLS)
	CertPEM    []byte // Client certificate PEM bytes (for mTLS)
	KeyPEM     []byte // Client private key PEM bytes (for mTLS)
	ServerName string // Override for TLS server name verification
	MinVersion uint16 // Minimum TLS version (default: tls.VersionTLS12)
}
