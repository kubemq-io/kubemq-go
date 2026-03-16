package kubemq

import (
	"context"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/kubemq-io/kubemq-go/v2/pkg/uuid"
)

const (
	defaultRequestTimeout = time.Second * 5
)

// Client is safe for concurrent use by multiple goroutines. A single Client
// instance uses one gRPC connection for all operations. Create one Client and
// share it across goroutines — do not create a Client per goroutine or per
// operation.
//
// All methods on Client are goroutine-safe. The underlying connection multiplexes
// concurrent operations over a single gRPC channel (HTTP/2 stream multiplexing).
//
// Example:
//
//	client, err := kubemq.NewClient(ctx, kubemq.WithAddress("localhost", 50000))
//	if err != nil { log.Fatal(err) }
//	defer client.Close()
//
//	// Share client across goroutines
//	for i := 0; i < 10; i++ {
//	    go func() {
//	        _ = client.SendEvent(ctx, event)
//	    }()
//	}
type Client struct {
	opts      *Options
	transport transport.Transport
	otel      *middleware.OTelInterceptor
}

// NewClient creates a KubeMQ client with production-ready defaults.
// This is the canonical constructor per TYPE-REGISTRY.md.
//
// The server address is provided via WithAddress(host, port).
// For local development, the default is localhost:50000.
//
// Parameters:
//   - ctx: controls the lifetime of the initial connection attempt. If the
//     context is cancelled or its deadline expires before the connection is
//     established, NewClient returns a TIMEOUT or CANCELLATION error. The
//     context is NOT stored — ongoing operations use their own contexts.
//   - op: zero or more Option functions that override defaults. When no options
//     are provided, the client uses the defaults listed below. Options are
//     applied in order; the last write wins for each setting.
//
// Default configuration (override with Option functions):
//   - address: localhost:50000
//   - clientId: auto-generated UUID
//   - autoReconnect: enabled (ReconnectPolicy with unlimited attempts)
//   - connectionTimeout: 10s
//   - keepalive: enabled (10s interval, 5s timeout)
//   - retryPolicy: 3 retries with exponential backoff
//   - waitForReady: true
//
// Returns a connected *Client on success. The caller must call Close() when
// done to release the underlying gRPC connection. On failure, returns nil and
// a *KubeMQError describing the issue.
//
// Possible errors:
//   - VALIDATION: host is empty, port is 0, or an Option sets an invalid value
//   - TRANSIENT: temporary network failure during initial connection (retryable)
//   - TIMEOUT: connection deadline exceeded (ctx deadline or connectionTimeout)
//   - AUTHENTICATION: TLS handshake failed or invalid credentials
//   - CANCELLATION: ctx was cancelled before connection completed
//
// Example:
//
//	client, err := kubemq.NewClient(ctx,
//	    kubemq.WithAddress("localhost", 50000),
//	)
//	if err != nil { log.Fatal(err) }
//	defer client.Close()
//
// See also: Client, Close, Option, WithAddress.
func NewClient(ctx context.Context, op ...Option) (*Client, error) {
	opts := GetDefaultOptions()
	for _, o := range op {
		o.apply(opts)
	}

	if opts.clientId == "" {
		opts.clientId = uuid.New()
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	otelInt := middleware.NewOTelInterceptor(
		opts.tracerProvider,
		opts.meterProvider,
		opts.logger,
		opts.host,
		opts.port,
		middleware.CardinalityConfig{
			Threshold: opts.cardinalityThreshold,
			Allowlist: opts.cardinalityAllowlist,
		},
		Version,
	)

	t, err := transport.NewGRPC(ctx, transport.Config{
		Host:                         opts.host,
		Port:                         opts.port,
		IsSecured:                    opts.isSecured,
		CertFile:                     opts.certFile,
		CertData:                     opts.certData,
		ServerOverrideDomain:         opts.serverOverrideDomain,
		AuthToken:                    opts.authToken,
		ClientID:                     opts.clientId,
		MaxSendSize:                  opts.maxSendMsgSize,
		MaxReceiveSize:               opts.maxRecvMsgSize,
		RetryPolicy:                  opts.retryPolicy,
		Logger:                       opts.logger,
		MaxConcurrentRetries:         opts.maxConcurrentRetries,
		ReconnectPolicy:              opts.reconnectPolicy,
		ConnectionTimeout:            opts.connectionTimeout,
		KeepaliveTime:                opts.keepaliveTime,
		KeepaliveTimeout:             opts.keepaliveTimeout,
		PermitKeepaliveWithoutStream: opts.permitKeepaliveWithoutStream,
		DrainTimeout:                 opts.drainTimeout,
		WaitForReady:                 opts.waitForReady,
		CheckConnection:              opts.checkConnection,
		CredentialProvider:           opts.credentialProvider,
		TLSConfig:                    opts.tlsConfig,
		InsecureSkipVerify:           opts.insecureSkipVerify,
		MaxConcurrentCallbacks:       opts.callbackConfig.MaxConcurrent,
		CallbackTimeout:              opts.callbackConfig.Timeout,
		ReceiveBufferSize:            opts.receiveBufferSize,
		OnConnected:                  opts.stateCallbacks.OnConnected,
		OnDisconnected:               opts.stateCallbacks.OnDisconnected,
		OnReconnecting:               opts.stateCallbacks.OnReconnecting,
		OnReconnected:                opts.stateCallbacks.OnReconnected,
		OnClosed:                     opts.stateCallbacks.OnClosed,
		OnBufferDrain:                opts.onBufferDrain,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		opts:      opts,
		transport: t,
		otel:      otelInt,
	}, nil
}

// State returns the current connection state. Thread-safe.
func (c *Client) State() ConnectionState {
	return c.transport.State()
}

// checkClosed returns ErrClientClosed if the client has been closed.
func (c *Client) checkClosed() error {
	if c.transport.State() == StateClosed {
		return ErrClientClosed
	}
	return nil
}

// defaultErrorHandler logs unhandled subscription errors at ERROR level.
func (c *Client) defaultErrorHandler(err error) {
	if c.opts.logger != nil {
		c.opts.logger.Error("unhandled subscription error", "error", err)
	}
}

// Close closes the client connection gracefully. In-flight operations are
// drained with a configurable timeout (default 5s), then in-flight subscription
// callbacks are drained with a separate timeout (default 30s). After Close
// returns, all methods return ErrClientClosed. Close is idempotent and safe
// to call concurrently from multiple goroutines.
func (c *Client) Close() error {
	if c.transport != nil {
		return c.transport.Close()
	}
	return nil
}

// Ping checks connectivity with the KubeMQ server and returns server info.
func (c *Client) Ping(ctx context.Context) (*ServerInfo, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	result, err := c.transport.Ping(ctx)
	if err != nil {
		return nil, err
	}
	return &ServerInfo{
		Host:                result.Host,
		Version:             result.Version,
		ServerStartTime:     result.ServerStartTime,
		ServerUpTimeSeconds: result.ServerUpTimeSeconds,
	}, nil
}
