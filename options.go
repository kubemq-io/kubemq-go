package kubemq

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
)

// Option configures a Client. Use the With* functions to create Options.
type Option interface {
	apply(*Options)
}

// Options holds all client configuration values. It is NOT safe for concurrent
// use. Used only during NewClient() construction; never shared. Immutable
// after NewClient returns.
type Options struct {
	host                 string
	port                 int
	isSecured            bool
	certFile             string
	certData             string
	serverOverrideDomain string
	authToken            string
	clientId             string
	receiveBufferSize    int
	defaultChannel       string
	defaultCacheTTL      time.Duration
	checkConnection      bool

	// v2 connection & transport fields (owned by 02-connection-transport-spec.md)
	reconnectPolicy              ReconnectPolicy
	connectionTimeout            time.Duration
	maxRecvMsgSize               int
	maxSendMsgSize               int
	waitForReady                 bool
	keepaliveTime                time.Duration
	keepaliveTimeout             time.Duration
	permitKeepaliveWithoutStream bool
	drainTimeout                 time.Duration

	// State callbacks
	stateCallbacks StateCallbacks
	onBufferDrain  func(discardedCount int)

	// Retry (owned by 01-error-handling-spec.md)
	retryPolicy          types.RetryPolicy
	maxConcurrentRetries int
	logger               types.Logger

	// Observability (owned by 05-observability-spec.md)
	tracerProvider       any // trace.TracerProvider — stored as any, cast internally
	meterProvider        any // metric.MeterProvider — stored as any, cast internally
	cardinalityThreshold int
	cardinalityAllowlist []string

	// Auth (owned by 03-auth-security-spec.md)
	credentialProvider CredentialProvider
	tlsConfig          *TLSConfig
	insecureSkipVerify bool

	// Callback dispatch (owned by 10-concurrency-spec.md)
	callbackConfig CallbackConfig
}

type funcOptions struct {
	fn func(*Options)
}

func (fo *funcOptions) apply(o *Options) {
	fo.fn(o)
}

func newFuncOption(f func(*Options)) *funcOptions {
	return &funcOptions{
		fn: f,
	}
}

// WithAddress sets the host and port of the KubeMQ server.
func WithAddress(host string, port int) Option {
	return newFuncOption(func(o *Options) {
		o.host = host
		o.port = port
	})
}

// WithCredentials sets TLS credentials from a certificate file.
// serverOverrideDomain is for testing only.
func WithCredentials(certFile, serverOverrideDomain string) Option {
	return newFuncOption(func(o *Options) {
		o.isSecured = true
		o.certFile = certFile
		o.serverOverrideDomain = serverOverrideDomain
	})
}

// WithCertificate sets TLS credentials from certificate PEM data.
// serverOverrideDomain is for testing only.
func WithCertificate(certData, serverOverrideDomain string) Option {
	return newFuncOption(func(o *Options) {
		o.isSecured = true
		o.certData = certData
		o.serverOverrideDomain = serverOverrideDomain
	})
}

// WithAuthToken sets a static auth token for authentication.
// This is a convenience wrapper that creates a StaticTokenProvider.
func WithAuthToken(token string) Option {
	return newFuncOption(func(o *Options) {
		o.credentialProvider = NewStaticTokenProvider(token)
	})
}

// WithCredentialProvider sets a custom credential provider for token authentication.
// The provider's GetToken is called with serialized access; at most one call
// is outstanding at any time.
func WithCredentialProvider(p CredentialProvider) Option {
	return newFuncOption(func(o *Options) {
		o.credentialProvider = p
	})
}

// WithClientId sets the client identifier used for all operations.
// Default: auto-generated UUID. Must be non-empty.
// The clientId appears in server-side logs and is used for queue consumer
// group identification.
func WithClientId(id string) Option {
	return newFuncOption(func(o *Options) {
		o.clientId = id
	})
}

// WithReceiveBufferSize sets the channel buffer size for subscription message delivery.
// Default: 10. Must be positive. Larger values reduce backpressure but increase memory usage.
func WithReceiveBufferSize(size int) Option {
	return newFuncOption(func(o *Options) {
		o.receiveBufferSize = size
	})
}

// WithDefaultChannel sets a default channel for operations that don't specify one.
// Default: "" (no default — channel must be set per-operation).
func WithDefaultChannel(channel string) Option {
	return newFuncOption(func(o *Options) {
		o.defaultChannel = channel
	})
}

// WithDefaultCacheTTL sets the default cache TTL for query responses.
// Default: 15m. Only applies when a CacheKey is set on the query.
func WithDefaultCacheTTL(ttl time.Duration) Option {
	return newFuncOption(func(o *Options) {
		o.defaultCacheTTL = ttl
	})
}

// WithCheckConnection enables a Ping check during NewClient to verify server connectivity.
// Default: false. When true, NewClient fails fast if the server is unreachable.
func WithCheckConnection(value bool) Option {
	return newFuncOption(func(o *Options) {
		o.checkConnection = value
	})
}

// WithRetryPolicy sets the retry policy for transient operation errors.
func WithRetryPolicy(p RetryPolicy) Option {
	return newFuncOption(func(o *Options) {
		o.retryPolicy = p
	})
}

// WithMaxConcurrentRetries limits the number of concurrent retry attempts
// per client instance. Default: 10.
func WithMaxConcurrentRetries(n int) Option {
	return newFuncOption(func(o *Options) {
		o.maxConcurrentRetries = n
	})
}

// WithLogger sets the structured logger for the client.
// Defined in TYPE-REGISTRY.md, owned by 05-observability-spec.md.
func WithLogger(l Logger) Option {
	return newFuncOption(func(o *Options) {
		o.logger = l
	})
}

// WithTracerProvider sets a custom OTel TracerProvider for trace instrumentation.
// If not set, falls back to otel.GetTracerProvider() (global, no-op if unconfigured).
//
// The provider is stored as `any` to avoid forcing an OTel import in user code
// that doesn't use tracing. The type is asserted internally.
//
// Owned by 05-observability-spec.md.
func WithTracerProvider(tp any) Option {
	return newFuncOption(func(o *Options) {
		o.tracerProvider = tp
	})
}

// WithMeterProvider sets a custom OTel MeterProvider for metric instrumentation.
// If not set, falls back to otel.GetMeterProvider() (global, no-op if unconfigured).
// Owned by 05-observability-spec.md.
func WithMeterProvider(mp any) Option {
	return newFuncOption(func(o *Options) {
		o.meterProvider = mp
	})
}

// WithCardinalityThreshold sets the maximum number of unique channel names
// tracked in metric attributes before omitting messaging.destination.name.
// Default: 100.
// Owned by 05-observability-spec.md.
func WithCardinalityThreshold(n int) Option {
	return newFuncOption(func(o *Options) {
		o.cardinalityThreshold = n
	})
}

// WithCardinalityAllowlist sets an explicit list of channel names that are
// always included in metric attributes regardless of cardinality threshold.
// Owned by 05-observability-spec.md.
func WithCardinalityAllowlist(channels []string) Option {
	return newFuncOption(func(o *Options) {
		o.cardinalityAllowlist = channels
	})
}

// WithReconnectPolicy sets the reconnection policy.
// Owned by 02-connection-transport-spec.md.
func WithReconnectPolicy(p ReconnectPolicy) Option {
	return newFuncOption(func(o *Options) {
		o.reconnectPolicy = p
	})
}

// WithConnectionTimeout sets the maximum time for initial connection establishment.
// Default: 10s. Does not apply to reconnection (which uses ReconnectPolicy).
func WithConnectionTimeout(d time.Duration) Option {
	return newFuncOption(func(o *Options) {
		o.connectionTimeout = d
	})
}

// WithMaxReceiveMessageSize sets the maximum inbound gRPC message size in bytes.
// Default: 104857600 (100 MB).
func WithMaxReceiveMessageSize(size int) Option {
	return newFuncOption(func(o *Options) {
		o.maxRecvMsgSize = size
	})
}

// WithMaxSendMessageSize sets the maximum outbound gRPC message size in bytes.
// Default: 104857600 (100 MB).
func WithMaxSendMessageSize(size int) Option {
	return newFuncOption(func(o *Options) {
		o.maxSendMsgSize = size
	})
}

// WithWaitForReady controls whether operations block until the connection is READY.
// When true, operations block during CONNECTING and RECONNECTING states.
// When false, operations fail immediately if state is not READY.
// Default: true.
func WithWaitForReady(wait bool) Option {
	return newFuncOption(func(o *Options) {
		o.waitForReady = wait
	})
}

// WithDrainTimeout sets the maximum time to wait for in-flight operations during Close().
// Default: 5s.
func WithDrainTimeout(d time.Duration) Option {
	return newFuncOption(func(o *Options) {
		o.drainTimeout = d
	})
}

// WithKeepaliveTime sets the keepalive ping interval.
// Default: 10s. Detection time = KeepaliveTime + KeepaliveTimeout (default 15s).
// When connecting through cloud load balancers, verify that the load balancer's
// idle timeout exceeds this interval.
func WithKeepaliveTime(d time.Duration) Option {
	return newFuncOption(func(o *Options) {
		o.keepaliveTime = d
	})
}

// WithKeepaliveTimeout sets the keepalive ping response timeout.
// Default: 5s.
func WithKeepaliveTimeout(d time.Duration) Option {
	return newFuncOption(func(o *Options) {
		o.keepaliveTimeout = d
	})
}

// WithPermitKeepaliveWithoutStream controls whether keepalive pings are sent
// when there are no active streams. Default: true.
func WithPermitKeepaliveWithoutStream(permit bool) Option {
	return newFuncOption(func(o *Options) {
		o.permitKeepaliveWithoutStream = permit
	})
}

// WithOnConnected registers a callback invoked when the initial connection
// transitions from CONNECTING to READY.
func WithOnConnected(fn func()) Option {
	return newFuncOption(func(o *Options) {
		o.stateCallbacks.OnConnected = fn
	})
}

// WithOnDisconnected registers a callback invoked when the connection
// transitions from READY to RECONNECTING.
func WithOnDisconnected(fn func()) Option {
	return newFuncOption(func(o *Options) {
		o.stateCallbacks.OnDisconnected = fn
	})
}

// WithOnReconnecting registers a callback invoked when the connection
// transitions from READY to RECONNECTING (alias for OnDisconnected).
func WithOnReconnecting(fn func()) Option {
	return newFuncOption(func(o *Options) {
		o.stateCallbacks.OnReconnecting = fn
	})
}

// WithOnReconnected registers a callback invoked when reconnection succeeds
// (RECONNECTING → READY).
func WithOnReconnected(fn func()) Option {
	return newFuncOption(func(o *Options) {
		o.stateCallbacks.OnReconnected = fn
	})
}

// WithOnClosed registers a callback invoked when the connection transitions
// to CLOSED from any state.
func WithOnClosed(fn func()) Option {
	return newFuncOption(func(o *Options) {
		o.stateCallbacks.OnClosed = fn
	})
}

// WithOnBufferDrain registers a callback invoked when buffered messages
// are discarded on transition to CLOSED.
func WithOnBufferDrain(fn func(discardedCount int)) Option {
	return newFuncOption(func(o *Options) {
		o.onBufferDrain = fn
	})
}

// GetDefaultOptions returns a new Options struct with sensible default values.
func GetDefaultOptions() *Options {
	return &Options{
		host:                         "localhost",
		port:                         50000,
		isSecured:                    false,
		certFile:                     "",
		certData:                     "",
		authToken:                    "",
		clientId:                     "",
		receiveBufferSize:            10,
		defaultChannel:               "",
		defaultCacheTTL:              15 * time.Minute,
		checkConnection:              false,
		reconnectPolicy:              types.DefaultReconnectPolicy(),
		connectionTimeout:            10 * time.Second,
		maxRecvMsgSize:               4 * 1024 * 1024,
		maxSendMsgSize:               100 * 1024 * 1024,
		waitForReady:                 true,
		keepaliveTime:                10 * time.Second,
		keepaliveTimeout:             5 * time.Second,
		permitKeepaliveWithoutStream: true,
		drainTimeout:                 5 * time.Second,
		retryPolicy:                  types.DefaultRetryPolicy(),
		maxConcurrentRetries:         10,
		cardinalityThreshold:         100,
		callbackConfig:               DefaultCallbackConfig(),
	}
}

// Validate checks that the Options contain the minimum required configuration.
func (o *Options) Validate() error {
	if o.host == "" {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "host address is required",
		}
	}
	if o.port <= 0 || o.port > 65535 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: fmt.Sprintf("port must be between 1 and 65535, got %d", o.port),
		}
	}
	if o.connectionTimeout <= 0 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "connection timeout must be positive",
		}
	}
	if o.keepaliveTime > 0 && o.keepaliveTime < 5*time.Second {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "keepalive time must be >= 5s (server minimum ping interval)",
		}
	}
	if o.maxRecvMsgSize <= 0 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "max receive message size must be positive",
		}
	}
	if o.maxSendMsgSize <= 0 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "max send message size must be positive",
		}
	}
	if o.reconnectPolicy.InitialDelay <= 0 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "reconnect initial delay must be positive",
		}
	}
	if o.reconnectPolicy.MaxDelay < o.reconnectPolicy.InitialDelay {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "reconnect max delay must be >= initial delay",
		}
	}
	if o.reconnectPolicy.Multiplier < 1.0 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "reconnect multiplier must be >= 1.0",
		}
	}

	if o.clientId != "" {
		if err := validateClientID(o.clientId); err != nil {
			return err
		}
	}
	if o.receiveBufferSize <= 0 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "receive buffer size must be positive",
		}
	}
	if o.defaultChannel != "" {
		if err := validateChannel(o.defaultChannel); err != nil {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: fmt.Sprintf("invalid default channel: %v", err),
				Cause:   ErrValidation,
			}
		}
	}
	if o.defaultCacheTTL < 0 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "default cache TTL must be non-negative",
		}
	}

	if err := o.validateTLS(); err != nil {
		return err
	}
	return nil
}

// validateTLS checks TLS-related options at construction time (fail-fast).
func (o *Options) validateTLS() error {
	if o.tlsConfig == nil {
		return nil
	}
	cfg := o.tlsConfig

	if cfg.MinVersion != 0 && cfg.MinVersion < tls.VersionTLS12 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: fmt.Sprintf("minimum TLS version 0x%04x is below TLS 1.2 (0x%04x); HTTP/2 requires TLS 1.2+", cfg.MinVersion, tls.VersionTLS12),
		}
	}

	if cfg.CACertFile != "" {
		if _, err := os.Stat(cfg.CACertFile); err != nil {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: fmt.Sprintf("CA certificate file %q: %v (check the file path and permissions)", cfg.CACertFile, err),
			}
		}
	}

	if len(cfg.CACertPEM) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cfg.CACertPEM) {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: "CA certificate PEM data contains no valid certificates",
			}
		}
	}

	if cfg.CertFile != "" || cfg.KeyFile != "" {
		if cfg.CertFile == "" || cfg.KeyFile == "" {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: "client certificate file and key file must both be provided for mTLS",
			}
		}
		if _, err := os.Stat(cfg.CertFile); err != nil {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: fmt.Sprintf("client certificate file %q: %v", cfg.CertFile, err),
			}
		}
		if _, err := os.Stat(cfg.KeyFile); err != nil {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: fmt.Sprintf("client key file %q: %v", cfg.KeyFile, err),
			}
		}
		if _, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile); err != nil {
			return &KubeMQError{
				Code:    ErrCodeAuthentication,
				Message: fmt.Sprintf("client certificate/key pair invalid: %v", err),
				Cause:   err,
			}
		}
	}

	if len(cfg.CertPEM) > 0 || len(cfg.KeyPEM) > 0 {
		if len(cfg.CertPEM) == 0 || len(cfg.KeyPEM) == 0 {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: "client certificate PEM and key PEM must both be provided for mTLS",
			}
		}
		if _, err := tls.X509KeyPair(cfg.CertPEM, cfg.KeyPEM); err != nil {
			return &KubeMQError{
				Code:    ErrCodeAuthentication,
				Message: fmt.Sprintf("client certificate/key PEM invalid: %v", err),
				Cause:   err,
			}
		}
	}

	return nil
}

// String returns a human-readable representation of the options.
// Credentials are redacted — only presence is shown, never values.
func (o *Options) String() string {
	return fmt.Sprintf("Options{host=%q, port=%d, tls=%v, credential_provider=%v, insecure_skip_verify=%v}",
		o.host, o.port,
		o.tlsConfig != nil,
		o.credentialProvider != nil,
		o.insecureSkipVerify,
	)
}
