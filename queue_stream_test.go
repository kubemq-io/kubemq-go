package kubemq

import (
	"fmt"
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewQueueDownstreamSendRequest(t *testing.T) {
	req := NewQueueDownstreamSendRequest()
	assert.NotNil(t, req)
	assert.IsType(t, &transport.QueueDownstreamSendRequest{}, req)
}

func TestQueueDownstreamRequestTypeConstants(t *testing.T) {
	assert.Equal(t, int32(1), QueueDownstreamGet)
	assert.Equal(t, int32(2), QueueDownstreamAckAll)
	assert.Equal(t, int32(4), QueueDownstreamNAckAll)
	assert.Equal(t, int32(6), QueueDownstreamReQueueAll)
	assert.Equal(t, int32(10), QueueDownstreamCloseByClient)
}

func TestQueueTransactionMessage_Ack(t *testing.T) {
	var captured *QueueDownstreamRequest
	msg := &QueueTransactionMessage{
		Message: &QueueMessage{
			Attributes: &QueueMessageAttributes{Sequence: 42},
		},
		TransactionID: "tx-1",
		sendFn: func(req *QueueDownstreamRequest) error {
			captured = req
			return nil
		},
	}

	err := msg.Ack()
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, QueueDownstreamAckRange, captured.RequestType)
	assert.Equal(t, "tx-1", captured.RefTransactionID)
	assert.Equal(t, []int64{42}, captured.SequenceRange)
}

func TestQueueTransactionMessage_Ack_NilSendFn(t *testing.T) {
	msg := &QueueTransactionMessage{
		Message: &QueueMessage{
			Attributes: &QueueMessageAttributes{Sequence: 1},
		},
	}

	err := msg.Ack()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no downstream send function")
}

func TestQueueTransactionMessage_Ack_NoSequence(t *testing.T) {
	msg := &QueueTransactionMessage{
		Message: &QueueMessage{},
		sendFn: func(req *QueueDownstreamRequest) error {
			return nil
		},
	}

	err := msg.Ack()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no sequence number")
}

func TestQueueTransactionMessage_Ack_SendError(t *testing.T) {
	sendErr := fmt.Errorf("network failure")
	msg := &QueueTransactionMessage{
		Message: &QueueMessage{
			Attributes: &QueueMessageAttributes{Sequence: 10},
		},
		TransactionID: "tx-err",
		sendFn: func(req *QueueDownstreamRequest) error {
			return sendErr
		},
	}

	err := msg.Ack()
	require.Error(t, err)
	assert.Equal(t, sendErr, err)
}

func TestQueueTransactionMessage_Reject(t *testing.T) {
	var captured *QueueDownstreamRequest
	msg := &QueueTransactionMessage{
		Message: &QueueMessage{
			Attributes: &QueueMessageAttributes{Sequence: 42},
		},
		TransactionID: "tx-1",
		sendFn: func(req *QueueDownstreamRequest) error {
			captured = req
			return nil
		},
	}

	err := msg.Reject()
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, QueueDownstreamNAckRange, captured.RequestType)
	assert.Equal(t, "tx-1", captured.RefTransactionID)
	assert.Equal(t, []int64{42}, captured.SequenceRange)
}

func TestQueueTransactionMessage_Reject_NilSendFn(t *testing.T) {
	msg := &QueueTransactionMessage{
		Message: &QueueMessage{
			Attributes: &QueueMessageAttributes{Sequence: 1},
		},
	}

	err := msg.Reject()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no downstream send function")
}

func TestQueueTransactionMessage_Reject_NoSequence(t *testing.T) {
	msg := &QueueTransactionMessage{
		Message: &QueueMessage{},
		sendFn: func(req *QueueDownstreamRequest) error {
			return nil
		},
	}

	err := msg.Reject()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no sequence number")
}

func TestQueueTransactionMessage_ReQueue(t *testing.T) {
	var captured *QueueDownstreamRequest
	msg := &QueueTransactionMessage{
		Message: &QueueMessage{
			Attributes: &QueueMessageAttributes{Sequence: 42},
		},
		TransactionID: "tx-1",
		sendFn: func(req *QueueDownstreamRequest) error {
			captured = req
			return nil
		},
	}

	err := msg.ReQueue("new-ch")
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, QueueDownstreamReQueueRange, captured.RequestType)
	assert.Equal(t, "tx-1", captured.RefTransactionID)
	assert.Equal(t, []int64{42}, captured.SequenceRange)
	assert.Equal(t, "new-ch", captured.ReQueueChannel)
}

func TestQueueTransactionMessage_ReQueue_NilSendFn(t *testing.T) {
	msg := &QueueTransactionMessage{
		Message: &QueueMessage{
			Attributes: &QueueMessageAttributes{Sequence: 1},
		},
	}

	err := msg.ReQueue("new-ch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no downstream send function")
}

func TestQueueTransactionMessage_ReQueue_NoSequence(t *testing.T) {
	msg := &QueueTransactionMessage{
		Message: &QueueMessage{},
		sendFn: func(req *QueueDownstreamRequest) error {
			return nil
		},
	}

	err := msg.ReQueue("new-ch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no sequence number")
}

func TestSequenceNumber(t *testing.T) {
	tests := []struct {
		name     string
		msg      *QueueTransactionMessage
		expected uint64
	}{
		{
			name:     "nil Message returns 0",
			msg:      &QueueTransactionMessage{},
			expected: 0,
		},
		{
			name:     "nil Attributes returns 0",
			msg:      &QueueTransactionMessage{Message: &QueueMessage{}},
			expected: 0,
		},
		{
			name: "valid Sequence returns value",
			msg: &QueueTransactionMessage{
				Message: &QueueMessage{
					Attributes: &QueueMessageAttributes{Sequence: 99},
				},
			},
			expected: 99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// sequenceNumber is unexported, so we exercise it via Ack which calls it internally.
			// For the zero-sequence cases, Ack should return a "no sequence" error.
			// For the valid case, we capture the sequence via sendFn.
			if tt.expected == 0 {
				tt.msg.sendFn = func(req *QueueDownstreamRequest) error { return nil }
				err := tt.msg.Ack()
				require.Error(t, err)
				assert.Contains(t, err.Error(), "no sequence number")
			} else {
				var captured *QueueDownstreamRequest
				tt.msg.sendFn = func(req *QueueDownstreamRequest) error {
					captured = req
					return nil
				}
				err := tt.msg.Ack()
				require.NoError(t, err)
				require.NotNil(t, captured)
				assert.Equal(t, []int64{int64(tt.expected)}, captured.SequenceRange)
			}
		})
	}
}
