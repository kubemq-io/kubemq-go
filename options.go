package kubemq

import (
	"errors"
	"strings"
	"time"
)

const kubeMQAuthTokenHeader = "authorization"

type Option interface {
	apply(*Options)
}
type TransportType int

const (
	TransportTypeGRPC TransportType = iota
	TransportTypeRest
)

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
	transportType        TransportType
	restUri              string
	webSocketUri         string
	autoReconnect        bool
	reconnectInterval    time.Duration
	maxReconnect         int
	checkConnection      bool
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

// WithUriAddress - set uri address of KubeMQ server
func WithUri(uri string) Option {
	return newFuncOption(func(o *Options) {
		o.restUri = uri
		o.webSocketUri = strings.Replace(uri, "http", "ws", 1)
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

// WithCertificate - set secured TLS credentials from the input certificate data for client.
// serverNameOverride is for testing only. If set to a non empty string,
// it will override the virtual host name of authority (e.g. :authority header field) in requests.
func WithCertificate(certData, serverOverrideDomain string) Option {
	return newFuncOption(func(o *Options) {
		o.isSecured = true
		o.certData = certData
		o.serverOverrideDomain = serverOverrideDomain
	})
}

// WithAuthToken - set KubeMQ JWT Auth token to be used for KubeMQ connection
func WithAuthToken(token string) Option {
	return newFuncOption(func(o *Options) {
		o.authToken = token
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
func WithDefaultChannel(channel string) Option {
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

// WithAutoReconnect - set automatic reconnection in case of lost connectivity to server
func WithAutoReconnect(value bool) Option {
	return newFuncOption(func(o *Options) {
		o.autoReconnect = value
	})
}

// WithReconnectInterval - set reconnection interval duration, default is 5 seconds
func WithReconnectInterval(duration time.Duration) Option {
	return newFuncOption(func(o *Options) {
		o.reconnectInterval = duration
	})
}

// WithMaxReconnects - set max reconnects before return error, default 0, never.
func WithMaxReconnects(value int) Option {
	return newFuncOption(func(o *Options) {
		o.maxReconnect = value
	})
}

// WithTransportType - set client transport type, currently GRPC or Rest
func WithTransportType(transportType TransportType) Option {
	return newFuncOption(func(o *Options) {
		o.transportType = transportType
	})
}

// WithCheckConnection - set server connectivity on client create
func WithCheckConnection(value bool) Option {
	return newFuncOption(func(o *Options) {
		o.checkConnection = value
	})
}

func GetDefaultOptions() *Options {
	return &Options{
		host:                 "",
		port:                 0,
		isSecured:            false,
		certFile:             "",
		certData:             "",
		serverOverrideDomain: "",
		authToken:            "",
		clientId:             "ClientId",
		receiveBufferSize:    10,
		defaultChannel:       "",
		defaultCacheTTL:      time.Minute * 15,
		transportType:        0,
		restUri:              "",
		webSocketUri:         "",
		autoReconnect:        false,
		reconnectInterval:    5 * time.Second,
		maxReconnect:         0,
		checkConnection:      false,
	}
}

func (o *Options) Validate() error {
	switch o.transportType {
	case TransportTypeGRPC:
		if o.host == "" {
			return errors.New("invalid host")
		}
		if o.port <= 0 {
			return errors.New("invalid port")
		}
	case TransportTypeRest:
		if o.restUri == "" {
			return errors.New("invalid address uri")
		}
	default:
		return errors.New("no transport type was set")
	}
	return nil
}
