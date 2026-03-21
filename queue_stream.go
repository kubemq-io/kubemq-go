package kubemq

import (
	"fmt"
)

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

// PollRequest configures a queue downstream poll operation.
type PollRequest struct {
	Channel  string
	MaxItems int32
	// WaitTimeoutSeconds is the server-side poll timeout in seconds.
	// Minimum 1. The SDK converts to milliseconds for the wire protocol.
	WaitTimeoutSeconds int32
	AutoAck            bool
}

// PollResponse contains the results of a single poll operation.
type PollResponse struct {
	TransactionID string
	Messages      []*QueueDownstreamMessage
	IsError       bool
	Error         string

	// unexported fields for batch operations
	autoAck    bool
	sendFn     func(req settleRequest) error // captured from receiver; nil if AutoAck
	cleanupTxn func()                        // removes TransactionID from openTxns
}

// validateSettleAll checks common preconditions for batch settlement operations.
func (r *PollResponse) validateSettleAll(op string) error {
	if r.autoAck {
		return fmt.Errorf("kubemq: cannot %s: poll used AutoAck", op)
	}
	if r.sendFn == nil {
		return fmt.Errorf("kubemq: no downstream send function available")
	}
	if r.TransactionID == "" {
		return fmt.Errorf("kubemq: cannot %s: no transaction ID", op)
	}
	return nil
}

// settleAll sends a batch settlement request and cleans up the transaction on success.
func (r *PollResponse) settleAll(req settleRequest) error {
	err := r.sendFn(req)
	if err == nil && r.cleanupTxn != nil {
		r.cleanupTxn()
	}
	return err
}

// AckAll acknowledges all messages in this poll response.
func (r *PollResponse) AckAll() error {
	if len(r.Messages) == 0 {
		return nil
	}
	if err := r.validateSettleAll("ack"); err != nil {
		return err
	}
	return r.settleAll(settleRequest{
		requestType:   QueueDownstreamAckAll,
		transactionID: r.TransactionID,
	})
}

// NackAll rejects all messages in this poll response (returns them to the queue).
func (r *PollResponse) NackAll() error {
	if len(r.Messages) == 0 {
		return nil
	}
	if err := r.validateSettleAll("nack"); err != nil {
		return err
	}
	return r.settleAll(settleRequest{
		requestType:   QueueDownstreamNAckAll,
		transactionID: r.TransactionID,
	})
}

// ReQueueAll moves all messages in this poll response to the specified channel.
func (r *PollResponse) ReQueueAll(channel string) error {
	if len(r.Messages) == 0 {
		return nil
	}
	if channel == "" {
		return fmt.Errorf("kubemq: requeue channel is required")
	}
	if err := r.validateSettleAll("requeue"); err != nil {
		return err
	}
	return r.settleAll(settleRequest{
		requestType:    QueueDownstreamReQueueAll,
		transactionID:  r.TransactionID,
		reQueueChannel: channel,
	})
}

// QueueDownstreamMessage represents a single message received from a queue downstream poll.
type QueueDownstreamMessage struct {
	Message       *QueueMessage
	TransactionID string
	Sequence      uint64

	// unexported fields for settlement
	autoAck bool
	sendFn  func(req settleRequest) error // nil if AutoAck
}

// validateSettle checks common preconditions for individual message settlement.
func (m *QueueDownstreamMessage) validateSettle(op string) error {
	if m.autoAck {
		return fmt.Errorf("kubemq: cannot %s: poll used AutoAck", op)
	}
	if m.sendFn == nil {
		return fmt.Errorf("kubemq: no downstream send function available")
	}
	if m.TransactionID == "" {
		return fmt.Errorf("kubemq: message has no transaction ID")
	}
	if m.Sequence == 0 {
		return fmt.Errorf("kubemq: message has no sequence number")
	}
	return nil
}

// Ack acknowledges this individual message within the current transaction.
func (m *QueueDownstreamMessage) Ack() error {
	if err := m.validateSettle("ack"); err != nil {
		return err
	}
	return m.sendFn(settleRequest{
		requestType:   QueueDownstreamAckRange,
		transactionID: m.TransactionID,
		sequences:     []int64{int64(m.Sequence)}, //nolint:gosec // G115: sequence numbers are non-negative and fit int64
	})
}

// Nack rejects (negative-acknowledges) this individual message within the current transaction.
func (m *QueueDownstreamMessage) Nack() error {
	if err := m.validateSettle("nack"); err != nil {
		return err
	}
	return m.sendFn(settleRequest{
		requestType:   QueueDownstreamNAckRange,
		transactionID: m.TransactionID,
		sequences:     []int64{int64(m.Sequence)}, //nolint:gosec // G115: sequence numbers are non-negative and fit int64
	})
}

// ReQueue re-queues this individual message to the specified channel.
func (m *QueueDownstreamMessage) ReQueue(channel string) error {
	if err := m.validateSettle("requeue"); err != nil {
		return err
	}
	if channel == "" {
		return fmt.Errorf("kubemq: requeue channel is required")
	}
	return m.sendFn(settleRequest{
		requestType:    QueueDownstreamReQueueRange,
		transactionID:  m.TransactionID,
		sequences:      []int64{int64(m.Sequence)}, //nolint:gosec // G115: sequence numbers are non-negative and fit int64
		reQueueChannel: channel,
	})
}

// settleRequest is an internal type representing a settlement action
// (ack, nack, requeue) sent on the downstream stream.
type settleRequest struct {
	requestType    int32
	transactionID  string
	sequences      []int64
	reQueueChannel string
}

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
	Results      []*QueueSendResult
	IsError      bool
	Error        string
}
