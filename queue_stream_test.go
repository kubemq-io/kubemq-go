package kubemq

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueDownstreamRequestTypeConstants(t *testing.T) {
	assert.Equal(t, int32(1), QueueDownstreamGet)
	assert.Equal(t, int32(2), QueueDownstreamAckAll)
	assert.Equal(t, int32(4), QueueDownstreamNAckAll)
	assert.Equal(t, int32(6), QueueDownstreamReQueueAll)
	assert.Equal(t, int32(10), QueueDownstreamCloseByClient)
}

// ---------------------------------------------------------------------------
// Per-message settlement: Ack
// ---------------------------------------------------------------------------

func TestQueueDownstreamMessage_Ack(t *testing.T) {
	var captured settleRequest
	msg := &QueueDownstreamMessage{
		Message: &QueueMessage{
			Attributes: &QueueMessageAttributes{Sequence: 42},
		},
		TransactionID: "tx-1",
		Sequence:      42,
		sendFn: func(req settleRequest) error {
			captured = req
			return nil
		},
	}

	err := msg.Ack()
	require.NoError(t, err)
	assert.Equal(t, QueueDownstreamAckRange, captured.requestType)
	assert.Equal(t, "tx-1", captured.transactionID)
	assert.Equal(t, []int64{42}, captured.sequences)
}

func TestQueueDownstreamMessage_Ack_AutoAck(t *testing.T) {
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-1",
		Sequence:      1,
		autoAck:       true,
		sendFn:        func(req settleRequest) error { return nil },
	}

	err := msg.Ack()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AutoAck")
}

func TestQueueDownstreamMessage_Ack_NilSendFn(t *testing.T) {
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-1",
		Sequence:      1,
	}

	err := msg.Ack()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no downstream send function")
}

func TestQueueDownstreamMessage_Ack_NoTransactionID(t *testing.T) {
	msg := &QueueDownstreamMessage{
		Sequence: 1,
		sendFn:   func(req settleRequest) error { return nil },
	}

	err := msg.Ack()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no transaction ID")
}

func TestQueueDownstreamMessage_Ack_NoSequence(t *testing.T) {
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-1",
		sendFn:        func(req settleRequest) error { return nil },
	}

	err := msg.Ack()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no sequence number")
}

func TestQueueDownstreamMessage_Ack_SendError(t *testing.T) {
	sendErr := fmt.Errorf("network failure")
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-err",
		Sequence:      10,
		sendFn: func(req settleRequest) error {
			return sendErr
		},
	}

	err := msg.Ack()
	require.Error(t, err)
	assert.Equal(t, sendErr, err)
}

// ---------------------------------------------------------------------------
// Per-message settlement: Nack
// ---------------------------------------------------------------------------

func TestQueueDownstreamMessage_Nack(t *testing.T) {
	var captured settleRequest
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-1",
		Sequence:      42,
		sendFn: func(req settleRequest) error {
			captured = req
			return nil
		},
	}

	err := msg.Nack()
	require.NoError(t, err)
	assert.Equal(t, QueueDownstreamNAckRange, captured.requestType)
	assert.Equal(t, "tx-1", captured.transactionID)
	assert.Equal(t, []int64{42}, captured.sequences)
}

func TestQueueDownstreamMessage_Nack_AutoAck(t *testing.T) {
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-1",
		Sequence:      1,
		autoAck:       true,
		sendFn:        func(req settleRequest) error { return nil },
	}

	err := msg.Nack()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AutoAck")
}

func TestQueueDownstreamMessage_Nack_NilSendFn(t *testing.T) {
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-1",
		Sequence:      1,
	}

	err := msg.Nack()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no downstream send function")
}

func TestQueueDownstreamMessage_Nack_NoTransactionID(t *testing.T) {
	msg := &QueueDownstreamMessage{
		Sequence: 1,
		sendFn:   func(req settleRequest) error { return nil },
	}

	err := msg.Nack()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no transaction ID")
}

func TestQueueDownstreamMessage_Nack_NoSequence(t *testing.T) {
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-1",
		sendFn:        func(req settleRequest) error { return nil },
	}

	err := msg.Nack()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no sequence number")
}

// ---------------------------------------------------------------------------
// Per-message settlement: ReQueue
// ---------------------------------------------------------------------------

func TestQueueDownstreamMessage_ReQueue(t *testing.T) {
	var captured settleRequest
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-1",
		Sequence:      42,
		sendFn: func(req settleRequest) error {
			captured = req
			return nil
		},
	}

	err := msg.ReQueue("new-ch")
	require.NoError(t, err)
	assert.Equal(t, QueueDownstreamReQueueRange, captured.requestType)
	assert.Equal(t, "tx-1", captured.transactionID)
	assert.Equal(t, []int64{42}, captured.sequences)
	assert.Equal(t, "new-ch", captured.reQueueChannel)
}

func TestQueueDownstreamMessage_ReQueue_EmptyChannel(t *testing.T) {
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-1",
		Sequence:      1,
		sendFn:        func(req settleRequest) error { return nil },
	}

	err := msg.ReQueue("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requeue channel is required")
}

func TestQueueDownstreamMessage_ReQueue_AutoAck(t *testing.T) {
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-1",
		Sequence:      1,
		autoAck:       true,
		sendFn:        func(req settleRequest) error { return nil },
	}

	err := msg.ReQueue("new-ch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AutoAck")
}

func TestQueueDownstreamMessage_ReQueue_NilSendFn(t *testing.T) {
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-1",
		Sequence:      1,
	}

	err := msg.ReQueue("new-ch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no downstream send function")
}

func TestQueueDownstreamMessage_ReQueue_NoTransactionID(t *testing.T) {
	msg := &QueueDownstreamMessage{
		Sequence: 1,
		sendFn:   func(req settleRequest) error { return nil },
	}

	err := msg.ReQueue("new-ch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no transaction ID")
}

func TestQueueDownstreamMessage_ReQueue_NoSequence(t *testing.T) {
	msg := &QueueDownstreamMessage{
		TransactionID: "tx-1",
		sendFn:        func(req settleRequest) error { return nil },
	}

	err := msg.ReQueue("new-ch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no sequence number")
}

// ---------------------------------------------------------------------------
// Batch settlement: AckAll
// ---------------------------------------------------------------------------

func TestPollResponse_AckAll(t *testing.T) {
	var captured settleRequest
	var cleanupCalled atomic.Int32
	resp := &PollResponse{
		TransactionID: "tx-1",
		Messages: []*QueueDownstreamMessage{
			{TransactionID: "tx-1", Sequence: 1},
		},
		sendFn: func(req settleRequest) error {
			captured = req
			return nil
		},
		cleanupTxn: func() { cleanupCalled.Add(1) },
	}

	err := resp.AckAll()
	require.NoError(t, err)
	assert.Equal(t, QueueDownstreamAckAll, captured.requestType)
	assert.Equal(t, "tx-1", captured.transactionID)
	assert.Equal(t, int32(1), cleanupCalled.Load())
}

func TestPollResponse_AckAll_AutoAck(t *testing.T) {
	resp := &PollResponse{
		TransactionID: "tx-1",
		autoAck:       true,
		Messages:      []*QueueDownstreamMessage{{TransactionID: "tx-1"}},
		sendFn:        func(req settleRequest) error { return nil },
	}

	err := resp.AckAll()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AutoAck")
}

func TestPollResponse_AckAll_NilSendFn(t *testing.T) {
	resp := &PollResponse{
		TransactionID: "tx-1",
		Messages:      []*QueueDownstreamMessage{{TransactionID: "tx-1"}},
	}

	err := resp.AckAll()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no downstream send function")
}

func TestPollResponse_AckAll_EmptyMessages(t *testing.T) {
	resp := &PollResponse{
		TransactionID: "tx-1",
		Messages:      []*QueueDownstreamMessage{},
		sendFn:        func(req settleRequest) error { return nil },
	}

	err := resp.AckAll()
	require.NoError(t, err)
}

func TestPollResponse_AckAll_NoTransactionID(t *testing.T) {
	resp := &PollResponse{
		Messages: []*QueueDownstreamMessage{
			{TransactionID: "", Sequence: 1},
		},
		sendFn: func(req settleRequest) error { return nil },
	}

	err := resp.AckAll()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no transaction ID")
}

func TestPollResponse_AckAll_SendError_NoCleanup(t *testing.T) {
	var cleanupCalled atomic.Int32
	resp := &PollResponse{
		TransactionID: "tx-1",
		Messages: []*QueueDownstreamMessage{
			{TransactionID: "tx-1", Sequence: 1},
		},
		sendFn:     func(req settleRequest) error { return fmt.Errorf("send failed") },
		cleanupTxn: func() { cleanupCalled.Add(1) },
	}

	err := resp.AckAll()
	require.Error(t, err)
	assert.Equal(t, int32(0), cleanupCalled.Load())
}

// ---------------------------------------------------------------------------
// Batch settlement: NackAll
// ---------------------------------------------------------------------------

func TestPollResponse_NackAll(t *testing.T) {
	var captured settleRequest
	var cleanupCalled atomic.Int32
	resp := &PollResponse{
		TransactionID: "tx-1",
		Messages: []*QueueDownstreamMessage{
			{TransactionID: "tx-1", Sequence: 1},
		},
		sendFn: func(req settleRequest) error {
			captured = req
			return nil
		},
		cleanupTxn: func() { cleanupCalled.Add(1) },
	}

	err := resp.NackAll()
	require.NoError(t, err)
	assert.Equal(t, QueueDownstreamNAckAll, captured.requestType)
	assert.Equal(t, "tx-1", captured.transactionID)
	assert.Equal(t, int32(1), cleanupCalled.Load())
}

func TestPollResponse_NackAll_AutoAck(t *testing.T) {
	resp := &PollResponse{
		TransactionID: "tx-1",
		autoAck:       true,
		Messages:      []*QueueDownstreamMessage{{TransactionID: "tx-1"}},
		sendFn:        func(req settleRequest) error { return nil },
	}

	err := resp.NackAll()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AutoAck")
}

func TestPollResponse_NackAll_NilSendFn(t *testing.T) {
	resp := &PollResponse{
		TransactionID: "tx-1",
		Messages:      []*QueueDownstreamMessage{{TransactionID: "tx-1"}},
	}

	err := resp.NackAll()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no downstream send function")
}

func TestPollResponse_NackAll_NoTransactionID(t *testing.T) {
	resp := &PollResponse{
		Messages: []*QueueDownstreamMessage{
			{TransactionID: "", Sequence: 1},
		},
		sendFn: func(req settleRequest) error { return nil },
	}

	err := resp.NackAll()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no transaction ID")
}

// ---------------------------------------------------------------------------
// Batch settlement: ReQueueAll
// ---------------------------------------------------------------------------

func TestPollResponse_ReQueueAll(t *testing.T) {
	var captured settleRequest
	var cleanupCalled atomic.Int32
	resp := &PollResponse{
		TransactionID: "tx-1",
		Messages: []*QueueDownstreamMessage{
			{TransactionID: "tx-1", Sequence: 1},
		},
		sendFn: func(req settleRequest) error {
			captured = req
			return nil
		},
		cleanupTxn: func() { cleanupCalled.Add(1) },
	}

	err := resp.ReQueueAll("dest-ch")
	require.NoError(t, err)
	assert.Equal(t, QueueDownstreamReQueueAll, captured.requestType)
	assert.Equal(t, "tx-1", captured.transactionID)
	assert.Equal(t, "dest-ch", captured.reQueueChannel)
	assert.Equal(t, int32(1), cleanupCalled.Load())
}

func TestPollResponse_ReQueueAll_EmptyChannel(t *testing.T) {
	resp := &PollResponse{
		TransactionID: "tx-1",
		Messages: []*QueueDownstreamMessage{
			{TransactionID: "tx-1", Sequence: 1},
		},
		sendFn: func(req settleRequest) error { return nil },
	}

	err := resp.ReQueueAll("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requeue channel is required")
}

func TestPollResponse_ReQueueAll_AutoAck(t *testing.T) {
	resp := &PollResponse{
		TransactionID: "tx-1",
		autoAck:       true,
		Messages:      []*QueueDownstreamMessage{{TransactionID: "tx-1"}},
		sendFn:        func(req settleRequest) error { return nil },
	}

	err := resp.ReQueueAll("dest-ch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AutoAck")
}

func TestPollResponse_ReQueueAll_NilSendFn(t *testing.T) {
	resp := &PollResponse{
		TransactionID: "tx-1",
		Messages:      []*QueueDownstreamMessage{{TransactionID: "tx-1"}},
	}

	err := resp.ReQueueAll("dest-ch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no downstream send function")
}

func TestPollResponse_ReQueueAll_NoTransactionID(t *testing.T) {
	resp := &PollResponse{
		Messages: []*QueueDownstreamMessage{
			{TransactionID: "", Sequence: 1},
		},
		sendFn: func(req settleRequest) error { return nil },
	}

	err := resp.ReQueueAll("dest-ch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no transaction ID")
}

// ---------------------------------------------------------------------------
// PollResponse field coverage
// ---------------------------------------------------------------------------

func TestPollResponse_Fields(t *testing.T) {
	resp := &PollResponse{
		TransactionID: "tx-fields",
		Messages:      []*QueueDownstreamMessage{{TransactionID: "tx-fields", Sequence: 1}},
		IsError:       true,
		Error:         "some error",
		autoAck:       false,
	}
	assert.Equal(t, "tx-fields", resp.TransactionID)
	assert.True(t, resp.IsError)
	assert.Equal(t, "some error", resp.Error)
	assert.Len(t, resp.Messages, 1)
}
