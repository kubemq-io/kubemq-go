package kubemq

import (
	"fmt"

	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
)

// QueueTransactionMessage wraps a received queue message within a transaction.
type QueueTransactionMessage struct {
	Message       *QueueMessage
	TransactionID string
	RefRequestID  string
	ActiveOffsets []int64
	Metadata      map[string]string
	sendFn        func(req *QueueDownstreamRequest) error
}

// Ack acknowledges this individual message within the current transaction.
func (m *QueueTransactionMessage) Ack() error {
	if m.sendFn == nil {
		return fmt.Errorf("kubemq: no downstream send function available")
	}
	seq := m.sequenceNumber()
	if seq == 0 {
		return fmt.Errorf("kubemq: message has no sequence number")
	}
	return m.sendFn(&QueueDownstreamRequest{
		RequestType:      QueueDownstreamAckRange,
		RefTransactionID: m.TransactionID,
		SequenceRange:    []int64{int64(seq)}, //nolint:gosec // G115: sequence numbers are non-negative and fit int64
	})
}

// Reject rejects (negative-acknowledges) this individual message within the current transaction.
func (m *QueueTransactionMessage) Reject() error {
	if m.sendFn == nil {
		return fmt.Errorf("kubemq: no downstream send function available")
	}
	seq := m.sequenceNumber()
	if seq == 0 {
		return fmt.Errorf("kubemq: message has no sequence number")
	}
	return m.sendFn(&QueueDownstreamRequest{
		RequestType:      QueueDownstreamNAckRange,
		RefTransactionID: m.TransactionID,
		SequenceRange:    []int64{int64(seq)}, //nolint:gosec // G115: sequence numbers are non-negative and fit int64
	})
}

// ReQueue re-queues this individual message to the specified channel.
func (m *QueueTransactionMessage) ReQueue(channel string) error {
	if m.sendFn == nil {
		return fmt.Errorf("kubemq: no downstream send function available")
	}
	seq := m.sequenceNumber()
	if seq == 0 {
		return fmt.Errorf("kubemq: message has no sequence number")
	}
	return m.sendFn(&QueueDownstreamRequest{
		RequestType:      QueueDownstreamReQueueRange,
		RefTransactionID: m.TransactionID,
		SequenceRange:    []int64{int64(seq)}, //nolint:gosec // G115: sequence numbers are non-negative and fit int64
		ReQueueChannel:   channel,
	})
}

func (m *QueueTransactionMessage) sequenceNumber() uint64 {
	if m.Message != nil && m.Message.Attributes != nil {
		return m.Message.Attributes.Sequence
	}
	return 0
}

// QueuePollRequest configures a queue downstream poll operation.
type QueuePollRequest struct {
	Channel     string
	MaxItems    int32
	WaitTimeout int32
	// WaitTimeoutSeconds accepts a value in seconds and converts to ms for the wire protocol.
	// If set (>0), it takes precedence over WaitTimeout. WaitTimeout is kept for backward compatibility.
	WaitTimeoutSeconds int32
	AutoAck            bool
}

// QueuePollResponse contains the results of a single poll operation.
type QueuePollResponse struct {
	TransactionID string
	Messages      []*QueueMessage
	IsError       bool
	Error         string
}

// QueueDownstreamRequestType constants mirror the proto enum.
const (
	QueueDownstreamGet               int32 = 1
	QueueDownstreamAckAll            int32 = 2
	QueueDownstreamAckRange          int32 = 3
	QueueDownstreamNAckAll           int32 = 4
	QueueDownstreamNAckRange         int32 = 5
	QueueDownstreamReQueueAll        int32 = 6
	QueueDownstreamReQueueRange      int32 = 7
	QueueDownstreamActiveOffsets     int32 = 8
	QueueDownstreamTransactionStatus int32 = 9
	QueueDownstreamCloseByClient     int32 = 10
	QueueDownstreamCloseByServer     int32 = 11
)

// QueueUpstreamHandle manages a bidirectional queue upstream (send) stream.
type QueueUpstreamHandle struct {
	Send    func(requestID string, msgs []*QueueMessage) error
	Results <-chan *QueueUpstreamResult
	Done    <-chan struct{}
	Close   func()
}

// QueueUpstreamResult is the per-batch result from an upstream send.
type QueueUpstreamResult struct {
	RefRequestID string
	Results      []*SendQueueMessageResult
	IsError      bool
	Error        string
}

// QueueDownstreamHandle manages a queue downstream (receive) stream.
type QueueDownstreamHandle struct {
	Messages <-chan *QueueTransactionMessage
	Errors   <-chan error
	Send     func(req *QueueDownstreamRequest) error
	Close    func()
}

// QueueDownstreamRequest is the public request type for downstream operations.
type QueueDownstreamRequest struct {
	RequestID   string
	ClientID    string
	RequestType int32
	Channel     string
	MaxItems    int32
	WaitTimeout int32
	// WaitTimeoutSeconds accepts a value in seconds and converts to ms for the wire protocol.
	// If set (>0), it takes precedence over WaitTimeout. WaitTimeout is kept for backward compatibility.
	WaitTimeoutSeconds int32
	AutoAck            bool
	ReQueueChannel     string
	SequenceRange      []int64
	RefTransactionID   string
	Metadata           map[string]string
}

// NewQueueDownstreamSendRequest creates a send request for queue downstream operations.
func NewQueueDownstreamSendRequest() *transport.QueueDownstreamSendRequest {
	return &transport.QueueDownstreamSendRequest{}
}
