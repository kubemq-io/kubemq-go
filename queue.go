package kubemq

import "fmt"

// QueueMessage is an outbound queue message. It is NOT safe for concurrent
// use — create a new QueueMessage for each send operation. Do not share
// QueueMessage instances across goroutines.
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
type QueuePolicy struct {
	ExpirationSeconds int
	DelaySeconds      int
	MaxReceiveCount   int
	MaxReceiveQueue   string
}

// QueueMessageAttributes contains receive-side metadata populated by the server.
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

// SendQueueMessageResult contains the result of a single queue message send.
// Immutable after construction. Safe to read from multiple goroutines.
type SendQueueMessageResult struct {
	MessageID    string
	SentAt       int64
	ExpirationAt int64
	DelayedTo    int64
	IsError      bool
	Error        string
	RefChannel   string
	RefTopic     string
	RefPartition int32
	RefHash      string
}

// ReceiveQueueMessagesRequest defines parameters for receiving queue messages.
type ReceiveQueueMessagesRequest struct {
	RequestID           string
	ClientID            string
	Channel             string
	MaxNumberOfMessages int32
	WaitTimeSeconds     int32
	IsPeak              bool
}

// ReceiveQueueMessagesResponse contains the results of a receive operation.
type ReceiveQueueMessagesResponse struct {
	RequestID        string
	Messages         []*QueueMessage
	MessagesReceived int32
	MessagesExpired  int32
	IsPeak           bool
	IsError          bool
	Error            string
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

// QueuesInfo contains information about queues.
type QueuesInfo struct {
	TotalQueue int32
	Sent       int64
	Delivered  int64
	Waiting    int64
	Queues     []*QueueInfo
}

// QueueInfo represents info about a single queue.
type QueueInfo struct {
	Name        string
	Messages    int64
	Bytes       int64
	FirstSeq    int64
	LastSeq     int64
	Sent        int64
	Delivered   int64
	Waiting     int64
	Subscribers int64
}
