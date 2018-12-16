package kubemq

import "time"

const kubeMQTokenHeader = "X-Kubemq-Server-Token"

type Option interface {
	apply(*Options)
}
type TransportType int

const TransportTypeGRPC TransportType = iota

type Options struct {
	host                 string
	port                 int
	isSecured            bool
	certFile             string
	serverOverrideDomain string
	token                string
	clientId             string
	receiveBufferSize    int
	defaultChannel       string
	defaultCacheTTL      time.Duration
	transportType        TransportType
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

// WithAddress - set host and port address of KubeMQ server
func WithAddress(host string, port int) Option {
	return newFuncOption(func(o *Options) {
		o.host = host
		o.port = port
	})
}

// WithCredentials - set secured TLS credentials from the input certificate file for client.
// serverNameOverride is for testing only. If set to a non empty string,
// it will override the virtual host name of authority (e.g. :authority header field) in requests.
func WithCredentials(certFile, serverOverrideDomain string) Option {
	return newFuncOption(func(o *Options) {
		o.isSecured = true
		o.certFile = certFile
		o.serverOverrideDomain = serverOverrideDomain
	})
}

// WithToken - set KubeMQ token to be used for KubeMQ connection - not mandatory, only if enforced by the KubeMQ server
func WithToken(token string) Option {
	return newFuncOption(func(o *Options) {
		o.token = token
	})
}

// WithClientId - set client id to be used in all functions call with this client - mandatory
func WithClientId(id string) Option {
	return newFuncOption(func(o *Options) {
		o.clientId = id
	})
}

// WithReceiveBufferSize - set length of buffered channel to be set in all subscriptions
func WithReceiveBufferSize(size int) Option {
	return newFuncOption(func(o *Options) {
		o.receiveBufferSize = size
	})
}

// WithDefaultChannel - set default channel for any outbound requests
func WithDefualtChannel(channel string) Option {
	return newFuncOption(func(o *Options) {
		o.defaultChannel = channel
	})
}

// WithDefaultCacheTTL - set default cache time to live for any query requests with any CacheKey set value
func WithDefaultCacheTTL(ttl time.Duration) Option {
	return newFuncOption(func(o *Options) {
		o.defaultCacheTTL = ttl
	})
}

// WithTransportType - set client transport type, currently GRPC or Rest
func WithTransportType(transportType TransportType) Option {
	return newFuncOption(func(o *Options) {
		o.transportType = transportType
	})
}

func GetDefaultOptions() *Options {
	return &Options{
		host:                 "localhost",
		port:                 50000,
		isSecured:            false,
		certFile:             "",
		serverOverrideDomain: "",
		token:                "",
		clientId:             "ClientId",
		receiveBufferSize:    10,
		defaultChannel:       "",
		defaultCacheTTL:      time.Minute * 15,
	}
}
