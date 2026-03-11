package transport

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamHandle_Close(t *testing.T) {
	closed := false
	h := NewStreamHandle(nil, nil, func() { closed = true })
	h.Close()
	assert.True(t, closed)
}

func TestStreamHandle_Close_NilCloseFn(t *testing.T) {
	h := &StreamHandle{}
	h.Close()
}

func TestQueueUpstreamHandle_Send(t *testing.T) {
	var sentMsgs []*QueueMessageItem
	h := &QueueUpstreamHandle{
		SendFn: func(msgs []*QueueMessageItem) error {
			sentMsgs = msgs
			return nil
		},
	}
	err := h.Send([]*QueueMessageItem{{ID: "m1"}})
	require.NoError(t, err)
	require.Len(t, sentMsgs, 1)
	assert.Equal(t, "m1", sentMsgs[0].ID)
}

func TestQueueUpstreamHandle_Send_NilFn(t *testing.T) {
	h := &QueueUpstreamHandle{}
	err := h.Send(nil)
	require.Error(t, err)
}

func TestQueueUpstreamHandle_Close(t *testing.T) {
	closed := false
	h := &QueueUpstreamHandle{closeFn: func() { closed = true }}
	h.Close()
	assert.True(t, closed)
}

func TestQueueDownstreamHandle_Send(t *testing.T) {
	var sentReq *QueueDownstreamSendRequest
	h := &QueueDownstreamHandle{
		SendFn: func(req *QueueDownstreamSendRequest) error {
			sentReq = req
			return nil
		},
	}
	err := h.Send(&QueueDownstreamSendRequest{RequestID: "r1"})
	require.NoError(t, err)
	require.NotNil(t, sentReq)
	assert.Equal(t, "r1", sentReq.RequestID)
}

func TestQueueDownstreamHandle_Send_NilFn(t *testing.T) {
	h := &QueueDownstreamHandle{}
	err := h.Send(nil)
	require.Error(t, err)
}

func TestQueueDownstreamHandle_Close(t *testing.T) {
	closed := false
	h := &QueueDownstreamHandle{closeFn: func() { closed = true }}
	h.Close()
	assert.True(t, closed)
}

func TestQueueDownstreamHandle_Close_NilFn(t *testing.T) {
	h := &QueueDownstreamHandle{}
	h.Close()
}
