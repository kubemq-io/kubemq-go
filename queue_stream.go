package kubemq

import (
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
)

// QueueTransactionMessage wraps a received queue message within a transaction.
type QueueTransactionMessage struct {
	Message       *QueueMessage
	TransactionID string
	RefRequestID  string
	ActiveOffsets []int64
}

// QueuePollRequest configures a queue downstream poll operation.
type QueuePollRequest struct {
	Channel     string
	MaxItems    int32
	WaitTimeout int32
	AutoAck     bool
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
)

// NewQueueDownstreamSendRequest creates a send request for queue downstream operations.
func NewQueueDownstreamSendRequest() *transport.QueueDownstreamSendRequest {
	return &transport.QueueDownstreamSendRequest{}
}
