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
	var sentReqID string
	h := &QueueUpstreamHandle{
		SendFn: func(requestID string, msgs []*QueueMessageItem) error {
			sentReqID = requestID
			sentMsgs = msgs
			return nil
		},
	}
	err := h.Send("req-123", []*QueueMessageItem{{ID: "m1"}})
	require.NoError(t, err)
	require.Len(t, sentMsgs, 1)
	assert.Equal(t, "m1", sentMsgs[0].ID)
	assert.Equal(t, "req-123", sentReqID)
}

func TestQueueUpstreamHandle_Send_NilFn(t *testing.T) {
	h := &QueueUpstreamHandle{}
	err := h.Send("", nil)
	require.Error(t, err)
}

func TestQueueUpstreamHandle_Close(t *testing.T) {
	closed := false
	h := &QueueUpstreamHandle{closeFn: func() { closed = true }}
	h.Close()
	assert.True(t, closed)
}

func TestEventStreamHandle_Send_NilSendFn(t *testing.T) {
	h := &EventStreamHandle{}
	err := h.Send(&EventStreamItem{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestEventStreamHandle_Send_Success(t *testing.T) {
	var gotItem *EventStreamItem
	h := &EventStreamHandle{
		SendFn: func(item *EventStreamItem) error {
			gotItem = item
			return nil
		},
	}
	err := h.Send(&EventStreamItem{ID: "e-1", Channel: "ch"})
	require.NoError(t, err)
	assert.Equal(t, "e-1", gotItem.ID)
}

func TestEventStreamHandle_Close_NilCloseFn(t *testing.T) {
	h := &EventStreamHandle{}
	h.Close()
}

func TestEventStreamHandle_Close_WithCloseFn(t *testing.T) {
	called := false
	h := &EventStreamHandle{closeFn: func() { called = true }}
	h.Close()
	assert.True(t, called)
}

func TestQueueUpstreamHandle_Close_NilCloseFn(t *testing.T) {
	h := &QueueUpstreamHandle{}
	h.Close()
}
