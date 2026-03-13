package transport

import (
	"context"
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribeToEvents_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)
	_, err := gt.SubscribeToEvents(context.Background(), &SubscribeRequest{Channel: "test"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client closed")
}

func TestSubscribeToEventsStore_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)
	_, err := gt.SubscribeToEventsStore(context.Background(), &SubscribeRequest{Channel: "test"})
	assert.Error(t, err)
}

func TestSubscribeToCommands_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)
	_, err := gt.SubscribeToCommands(context.Background(), &SubscribeRequest{Channel: "test"})
	assert.Error(t, err)
}

func TestSubscribeToQueries_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)
	_, err := gt.SubscribeToQueries(context.Background(), &SubscribeRequest{Channel: "test"})
	assert.Error(t, err)
}

func TestSubscribeToEvents_NotReady(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.stateMachine.transition(types.StateReconnecting)
	_, err := gt.SubscribeToEvents(context.Background(), &SubscribeRequest{Channel: "test"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not ready")
}

func TestSubscribeToEvents_Success(t *testing.T) {
	lis, _ := startBufconnServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.SubscribeToEvents(ctx, &SubscribeRequest{
		Channel:  "test-events",
		Group:    "g1",
		ClientID: "sub-client",
	})
	if err != nil {
		assert.Contains(t, err.Error(), "Unimplemented")
		return
	}
	assert.NotNil(t, handle)
}

func TestQueueUpstream_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)
	_, err := gt.QueueUpstream(context.Background())
	assert.Error(t, err)
}

func TestQueueDownstream_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)
	_, err := gt.QueueDownstream(context.Background(), &QueueDownstreamRequest{Channel: "test"})
	assert.Error(t, err)
}

func TestQueueUpstream_NotReady(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.stateMachine.transition(types.StateReconnecting)
	_, err := gt.QueueUpstream(context.Background())
	assert.Error(t, err)
}

func TestQueueDownstream_NotReady(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.stateMachine.transition(types.StateReconnecting)
	_, err := gt.QueueDownstream(context.Background(), &QueueDownstreamRequest{Channel: "test"})
	assert.Error(t, err)
}
