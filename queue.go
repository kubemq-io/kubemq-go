package kubemq

import "fmt"

// QueueMessage is an outbound queue message. It is NOT safe for concurrent
// use — create a new QueueMessage for each send operation. Do not share
// QueueMessage instances across goroutines.
//
// Fields:
//   - ID: unique message identifier. Auto-generated UUID if empty at send time.
//   - ClientID: sender identifier. Auto-populated from client defaults if empty.
//   - Channel: target queue channel name (required unless WithDefaultChannel is set).
//     Wildcards are not supported for queue channels.
//   - Metadata: arbitrary string metadata stored with the message.
//   - Body: binary payload. At least one of Body or Metadata must be non-empty.
//   - Tags: key-value string pairs stored alongside the message, useful for
//     filtering or routing.
//   - Policy: optional delivery policy controlling expiration, delay, and
//     dead-letter routing. Nil means no policy (message never expires, no delay).
//   - Attributes: receive-side metadata populated by the server when the message
//     is received (nil on outbound messages). Contains sequence number, timestamp,
//     receive count, etc.
//
// See also: SendQueueMessage, PollQueue, QueuePolicy,
// QueueMessageAttributes.
type QueueMessage struct {
	ID         string
	ClientID   string
	Channel    string
	Metadata   string
	Body       []byte
	Tags       map[string]string
	Policy     *QueuePolicy
	Attributes *QueueMessageAttributes
}

// NewQueueMessage creates an empty QueueMessage.
func NewQueueMessage() *QueueMessage {
	return &QueueMessage{}
}

// SetId sets the queue message ID.
func (qm *QueueMessage) SetId(id string) *QueueMessage {
	qm.ID = id
	return qm
}

// SetClientId sets the client identifier for this message.
func (qm *QueueMessage) SetClientId(clientId string) *QueueMessage {
	qm.ClientID = clientId
	return qm
}

// SetChannel sets the target queue channel.
func (qm *QueueMessage) SetChannel(channel string) *QueueMessage {
	qm.Channel = channel
	return qm
}

// SetMetadata sets the message metadata.
func (qm *QueueMessage) SetMetadata(metadata string) *QueueMessage {
	qm.Metadata = metadata
	return qm
}

// SetBody sets the message body payload.
func (qm *QueueMessage) SetBody(body []byte) *QueueMessage {
	qm.Body = body
	return qm
}

// SetTags replaces all tags on this message.
func (qm *QueueMessage) SetTags(tags map[string]string) *QueueMessage {
	qm.Tags = make(map[string]string, len(tags))
	for key, value := range tags {
		qm.Tags[key] = value
	}
	return qm
}

// AddTag adds a single key-value tag to this message.
func (qm *QueueMessage) AddTag(key, value string) *QueueMessage {
	if qm.Tags == nil {
		qm.Tags = map[string]string{}
	}
	qm.Tags[key] = value
	return qm
}

// SetPolicy sets the delivery policy for this message.
func (qm *QueueMessage) SetPolicy(policy *QueuePolicy) *QueueMessage {
	qm.Policy = policy
	return qm
}

// SetExpirationSeconds sets the message expiration in seconds.
func (qm *QueueMessage) SetExpirationSeconds(seconds int) *QueueMessage {
	if qm.Policy == nil {
		qm.Policy = &QueuePolicy{}
	}
	qm.Policy.ExpirationSeconds = seconds
	return qm
}

// SetDelaySeconds sets the message delay in seconds.
func (qm *QueueMessage) SetDelaySeconds(seconds int) *QueueMessage {
	if qm.Policy == nil {
		qm.Policy = &QueuePolicy{}
	}
	qm.Policy.DelaySeconds = seconds
	return qm
}

// SetMaxReceiveCount sets the maximum number of receive attempts.
func (qm *QueueMessage) SetMaxReceiveCount(count int) *QueueMessage {
	if qm.Policy == nil {
		qm.Policy = &QueuePolicy{}
	}
	qm.Policy.MaxReceiveCount = count
	return qm
}

// SetMaxReceiveQueue sets the dead-letter queue for messages exceeding max receive count.
func (qm *QueueMessage) SetMaxReceiveQueue(queue string) *QueueMessage {
	if qm.Policy == nil {
		qm.Policy = &QueuePolicy{}
	}
	qm.Policy.MaxReceiveQueue = queue
	return qm
}

// Validate checks all required fields and constraints.
// Called automatically before send operations; can also be called explicitly.
func (qm *QueueMessage) Validate() error {
	return validateQueueMessage(qm, nil)
}

// String returns a human-readable representation of the queue message.
func (qm *QueueMessage) String() string {
	return fmt.Sprintf("ID: %s, Channel: %s, ClientID: %s", qm.ID, qm.Channel, qm.ClientID)
}

// QueuePolicy defines delivery policy for queue messages.
//
// Fields:
//   - ExpirationSeconds: message time-to-live in seconds. The message is
//     automatically removed from the queue after this duration. Zero means
//     no expiration (message persists until consumed).
//   - DelaySeconds: delivery delay in seconds. The message is invisible to
//     consumers until the delay elapses. Zero means immediate availability.
//   - MaxReceiveCount: maximum number of times the message can be received
//     (dequeued) before it is moved to the dead-letter queue. Zero means
//     unlimited receive attempts.
//   - MaxReceiveQueue: dead-letter queue channel name. When MaxReceiveCount is
//     exceeded, the message is moved to this queue. Empty means the message is
//     discarded instead.
//
// See also: QueueMessage, SendQueueMessage.
type QueuePolicy struct {
	ExpirationSeconds int
	DelaySeconds      int
	MaxReceiveCount   int
	MaxReceiveQueue   string
}

// QueueMessageAttributes contains receive-side metadata populated by the server
// when a message is dequeued. This struct is nil on outbound messages and only
// present on messages returned by PollQueue or QueueDownstream.
//
// Fields:
//   - Timestamp: Unix timestamp (nanoseconds) when the message was originally
//     enqueued.
//   - Sequence: monotonically increasing sequence number assigned by the server.
//     Unique within the queue channel.
//   - MD5OfBody: MD5 hash of the message body, useful for integrity verification.
//   - ReceiveCount: number of times this message has been received (dequeued).
//     Increments on each receive; useful for detecting poison messages.
//   - ReRouted: true if this message was moved from another queue (e.g.,
//     dead-letter routing).
//   - ReRoutedFromQueue: the original queue channel name if ReRouted is true.
//   - ExpirationAt: Unix timestamp (nanoseconds) when the message expires.
//     Zero if no expiration policy is set.
//   - DelayedTo: Unix timestamp (nanoseconds) until which the message was
//     delayed. Zero if no delay policy is set.
//
// See also: QueueMessage, PollQueue.
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

// QueueMessages is a batch of queue messages.
type QueueMessages struct {
	Messages []*QueueMessage
}

// Add appends a message to the batch.
func (qms *QueueMessages) Add(msg *QueueMessage) *QueueMessages {
	qms.Messages = append(qms.Messages, msg)
	return qms
}

// QueueSendResult contains the result of a single queue message send.
// Immutable after construction. Safe to read from multiple goroutines.
//
// Fields:
//   - MessageID: the server-assigned or client-provided message identifier.
//   - SentAt: Unix timestamp (nanoseconds) when the server accepted the message.
//   - ExpirationAt: Unix timestamp (nanoseconds) when the message will expire.
//     Zero if no expiration policy is set.
//   - DelayedTo: Unix timestamp (nanoseconds) until which the message is delayed.
//     Zero if no delay policy is set.
//   - IsError: true if the server rejected this specific message. When true,
//     Error contains the reason (e.g., queue does not exist).
//   - Error: human-readable error message. Empty when IsError is false.
//
// See also: SendQueueMessage, SendQueueMessages, QueueMessage.
type QueueSendResult struct {
	MessageID    string
	SentAt       int64
	ExpirationAt int64
	DelayedTo    int64
	IsError      bool
	Error        string
}

// AckAllQueueMessagesRequest defines parameters for acknowledging all queue messages.
type AckAllQueueMessagesRequest struct {
	RequestID       string
	ClientID        string
	Channel         string
	WaitTimeSeconds int32
}

// AckAllQueueMessagesResponse contains the result of an ack-all operation.
type AckAllQueueMessagesResponse struct {
	RequestID        string
	AffectedMessages uint64
	IsError          bool
	Error            string
}
