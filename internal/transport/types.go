package transport

import (
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
)

// ServerInfoResult is the internal representation of a ping response.
type ServerInfoResult struct {
	Host                string
	Version             string
	ServerStartTime     int64
	ServerUpTimeSeconds int64
}

// SendEventRequest is the internal request type for sending events.
type SendEventRequest struct {
	ID       string
	ClientID string
	Channel  string
	Metadata string
	Body     []byte
	Tags     map[string]string
}

// SendEventStoreRequest is the internal request type for sending event store messages.
type SendEventStoreRequest struct {
	ID       string
	ClientID string
	Channel  string
	Metadata string
	Body     []byte
	Tags     map[string]string
}

// EventStreamItem is the internal request type for sending events on a stream.
type EventStreamItem struct {
	ID       string
	ClientID string
	Channel  string
	Metadata string
	Body     []byte
	Tags     map[string]string
	Store    bool
}

// EventStreamResult is the internal result type for event stream responses.
type EventStreamResult struct {
	EventID string
	Sent    bool
	Error   string
}

// SendEventStoreResult is the internal result type for event store send operations.
type SendEventStoreResult struct {
	ID   string
	Sent bool
	Err  error
}

// SendCommandRequest is the internal request type for sending commands.
type SendCommandRequest struct {
	ID       string
	ClientID string
	Channel  string
	Metadata string
	Body     []byte
	Timeout  time.Duration
	Tags     map[string]string
	Span     []byte
}

// SendCommandResult is the internal result type for command send operations.
type SendCommandResult struct {
	CommandID        string
	ResponseClientID string
	Executed         bool
	ExecutedAt       time.Time
	Error            string
	Tags             map[string]string
}

// SendQueryRequest is the internal request type for sending queries.
type SendQueryRequest struct {
	ID       string
	ClientID string
	Channel  string
	Metadata string
	Body     []byte
	Timeout  time.Duration
	CacheKey string
	CacheTTL time.Duration
	Tags     map[string]string
	Span     []byte
}

// SendQueryResult is the internal result type for query send operations.
type SendQueryResult struct {
	QueryID          string
	Executed         bool
	ExecutedAt       time.Time
	Metadata         string
	ResponseClientID string
	Body             []byte
	CacheHit         bool
	Error            string
	Tags             map[string]string
}

// SendResponseRequest is the internal request type for sending responses.
type SendResponseRequest struct {
	RequestID  string
	ResponseTo string
	Metadata   string
	Body       []byte
	ClientID   string
	ExecutedAt time.Time
	Err        error
	Tags       map[string]string
	Span       []byte
}

// QueueMessageItem is the internal representation of a queue message.
type QueueMessageItem struct {
	ID         string
	ClientID   string
	Channel    string
	Metadata   string
	Body       []byte
	Tags       map[string]string
	Policy     *QueueMessagePolicy
	Attributes *QueueMessageAttributes
}

// QueueMessagePolicy defines delivery policy for queue messages.
type QueueMessagePolicy struct {
	ExpirationSeconds int
	DelaySeconds      int
	MaxReceiveCount   int
	MaxReceiveQueue   string
}

// QueueMessageAttributes contains receive-side metadata for queue messages.
type QueueMessageAttributes struct {
	Timestamp         int64
	Sequence          uint64
	MD5OfBody         string
	ReceiveCount      int
	ReRouted          bool
	ReRoutedFromQueue string
	ExpirationAt      int64
	DelayedTo         int64
}

// SendQueueMessagesRequest is the internal request for sending queue messages.
type SendQueueMessagesRequest struct {
	Messages []*QueueMessageItem
}

// SendQueueMessageResultItem is the internal result for a single queue message send.
type SendQueueMessageResultItem struct {
	MessageID    string
	SentAt       int64
	ExpirationAt int64
	DelayedTo    int64
	IsError      bool
	Error        string
}

// SendQueueMessagesResult is the internal result for batch queue message sends.
type SendQueueMessagesResult struct {
	Results []*SendQueueMessageResultItem
}

// ReceiveQueueMessagesReq is the internal request for receiving queue messages.
type ReceiveQueueMessagesReq struct {
	RequestID           string
	ClientID            string
	Channel             string
	MaxNumberOfMessages int32
	WaitTimeSeconds     int32
	IsPeak              bool
}

// ReceiveQueueMessagesResp is the internal response for receiving queue messages.
type ReceiveQueueMessagesResp struct {
	RequestID        string
	Messages         []*QueueMessageItem
	MessagesReceived int32
	MessagesExpired  int32
	IsPeak           bool
	IsError          bool
	Error            string
}

// AckAllQueueMessagesReq is the internal request for acknowledging all queue messages.
type AckAllQueueMessagesReq struct {
	RequestID       string
	ClientID        string
	Channel         string
	WaitTimeSeconds int32
}

// AckAllQueueMessagesResp is the internal response for acknowledging all queue messages.
type AckAllQueueMessagesResp struct {
	RequestID        string
	AffectedMessages uint64
	IsError          bool
	Error            string
}

// SubscribeRequest is the internal request for subscription operations.
type SubscribeRequest struct {
	Channel           string
	Group             string
	ClientID          string
	SubscriptionType  int32
	SubscriptionValue int64
}

// QueueDownstreamRequest is the internal request for queue downstream (receive) streams.
type QueueDownstreamRequest struct {
	ClientID    string
	Channel     string
	MaxItems    int32
	WaitTimeout int32
	AutoAck     bool
}

// CreateChannelRequest is the internal request for creating a channel.
type CreateChannelRequest struct {
	ClientID    string
	Channel     string
	ChannelType string
}

// DeleteChannelRequest is the internal request for deleting a channel.
type DeleteChannelRequest struct {
	ClientID    string
	Channel     string
	ChannelType string
}

// ListChannelsRequest is the internal request for listing channels.
type ListChannelsRequest struct {
	ClientID    string
	ChannelType string
	Search      string
}

// ChannelInfo represents information about a channel.
type ChannelInfo struct {
	Name         string
	Type         string
	LastActivity int64
	IsActive     bool
	Incoming     *ChannelStats
	Outgoing     *ChannelStats
}

// ChannelStats holds statistics for a channel direction.
type ChannelStats struct {
	Messages  int64
	Volume    int64
	Responses int64
	Waiting   int64
	Expired   int64
	Delayed   int64
}

// EventReceiveItem is the internal representation of a received event.
type EventReceiveItem struct {
	ID        string
	Channel   string
	Metadata  string
	Body      []byte
	Timestamp int64
	Tags      map[string]string
}

// EventStoreReceiveItem is the internal representation of a received event store message.
type EventStoreReceiveItem struct {
	ID        string
	Channel   string
	Metadata  string
	Body      []byte
	Timestamp int64
	Sequence  uint64
	Tags      map[string]string
}

// CommandReceiveItem is the internal representation of a received command.
type CommandReceiveItem struct {
	ID         string
	ClientID   string
	Channel    string
	Metadata   string
	Body       []byte
	ResponseTo string
	Tags       map[string]string
	Span       []byte
}

// QueryReceiveItem is the internal representation of a received query.
type QueryReceiveItem struct {
	ID         string
	ClientID   string
	Channel    string
	Metadata   string
	Body       []byte
	ResponseTo string
	Tags       map[string]string
	Span       []byte
}

// QueueUpstreamResult is the internal representation of an upstream response.
type QueueUpstreamResult struct {
	RefRequestID string
	Results      []*SendQueueMessageResultItem
	IsError      bool
	Error        string
}

// QueueDownstreamSendRequest is the internal request sent on a downstream stream.
type QueueDownstreamSendRequest struct {
	RequestID        string
	ClientID         string
	RequestType      int32
	Channel          string
	MaxItems         int32
	WaitTimeout      int32
	AutoAck          bool
	ReQueueChannel   string
	SequenceRange    []int64
	RefTransactionID string
	Metadata         map[string]string
}

// QueueDownstreamResult is the internal representation of a downstream response.
type QueueDownstreamResult struct {
	TransactionID       string
	RefRequestID        string
	Messages            []*QueueMessageItem
	ActiveOffsets       []int64
	IsError             bool
	Error               string
	TransactionComplete bool
	Metadata            map[string]string
}

// Config holds configuration for creating a transport connection.
type Config struct {
	Host                 string
	Port                 int
	IsSecured            bool
	CertFile             string
	CertData             string
	ServerOverrideDomain string
	AuthToken            string
	ClientID             string
	MaxSendSize          int
	MaxReceiveSize       int
	RetryPolicy          types.RetryPolicy
	Logger               types.Logger
	MaxConcurrentRetries int

	// v2 connection & transport fields (owned by 02-connection-transport-spec.md)
	ReconnectPolicy              types.ReconnectPolicy
	ConnectionTimeout            time.Duration
	KeepaliveTime                time.Duration
	KeepaliveTimeout             time.Duration
	PermitKeepaliveWithoutStream bool
	DrainTimeout                 time.Duration
	WaitForReady                 bool
	CheckConnection              bool

	// Auth & Security (owned by 03-auth-security-spec.md)
	CredentialProvider types.CredentialProvider
	TLSConfig          *types.TLSConfig
	InsecureSkipVerify bool

	// Callback dispatch (owned by 10-concurrency-spec.md)
	// Passed as primitives to avoid importing root package (XS-4).
	MaxConcurrentCallbacks int
	CallbackTimeout        time.Duration

	// Subscription buffering
	ReceiveBufferSize int

	// State callbacks (passed as primitives to avoid import cycles)
	OnConnected    func()
	OnDisconnected func()
	OnReconnecting func()
	OnReconnected  func()
	OnClosed       func()
	OnBufferDrain  func(discardedCount int)
}
