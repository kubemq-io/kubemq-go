package kubemq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribeToEvents_RequiresHandler(t *testing.T) {
	c, _ := newTestClientWithMock(t)
	ctx := context.Background()

	_, err := c.SubscribeToEvents(ctx, "test", "")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestSubscribeToEvents_EmptyChannel(t *testing.T) {
	c, _ := newTestClientWithMock(t)
	ctx := context.Background()

	_, err := c.SubscribeToEvents(ctx, "", "",
		WithOnEvent(func(*Event) {}),
	)
	require.Error(t, err)
}

func TestSubscribeToEvents_ReceivesMessages(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh := make(chan any, 1)
	errCh := make(chan error)

	mt.OnSubscribeToEvents(func(_ context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		assert.Equal(t, "test-events", req.Channel)
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	var received atomic.Int32
	sub, err := c.SubscribeToEvents(ctx, "test-events", "",
		WithOnEvent(func(e *Event) {
			received.Add(1)
			assert.Equal(t, "evt-1", e.Id)
		}),
		WithOnError(func(err error) {}),
	)
	require.NoError(t, err)
	require.NotNil(t, sub)

	msgCh <- &transport.EventReceiveItem{
		ID:      "evt-1",
		Channel: "test-events",
		Body:    []byte("hello"),
	}

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), received.Load())

	sub.Unsubscribe()
}

func TestSubscribeToEventsStore_RequiresHandler(t *testing.T) {
	c, _ := newTestClientWithMock(t)
	ctx := context.Background()

	_, err := c.SubscribeToEventsStore(ctx, "test", "", StartFromNewEvents())
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestSubscribeToEventsStore_ReceivesMessages(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh := make(chan any, 1)
	errCh := make(chan error)

	mt.OnSubscribeToEventsStore(func(_ context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		assert.Equal(t, "store-ch", req.Channel)
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	var received atomic.Int32
	sub, err := c.SubscribeToEventsStore(ctx, "store-ch", "", StartFromNewEvents(),
		WithOnEventStoreReceive(func(e *EventStoreReceive) {
			received.Add(1)
			assert.Equal(t, "es-1", e.Id)
			assert.Equal(t, uint64(42), e.Sequence)
		}),
		WithOnError(func(err error) {}),
	)
	require.NoError(t, err)
	require.NotNil(t, sub)

	msgCh <- &transport.EventStoreReceiveItem{
		ID:       "es-1",
		Channel:  "store-ch",
		Sequence: 42,
		Body:     []byte("stored"),
	}

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), received.Load())

	sub.Unsubscribe()
}

func TestSubscribeToCommands_RequiresHandler(t *testing.T) {
	c, _ := newTestClientWithMock(t)
	ctx := context.Background()

	_, err := c.SubscribeToCommands(ctx, "test", "")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestSubscribeToCommands_ReceivesMessages(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh := make(chan any, 1)
	errCh := make(chan error)

	mt.OnSubscribeToCommands(func(_ context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		assert.Equal(t, "cmd-ch", req.Channel)
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	var received atomic.Int32
	sub, err := c.SubscribeToCommands(ctx, "cmd-ch", "",
		WithOnCommandReceive(func(cmd *CommandReceive) {
			received.Add(1)
			assert.Equal(t, "cmd-1", cmd.Id)
			assert.Equal(t, "reply-ch", cmd.ResponseTo)
		}),
		WithOnError(func(err error) {}),
	)
	require.NoError(t, err)

	msgCh <- &transport.CommandReceiveItem{
		ID:         "cmd-1",
		Channel:    "cmd-ch",
		ResponseTo: "reply-ch",
		Body:       []byte("do"),
	}

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), received.Load())

	sub.Unsubscribe()
}

func TestSubscribeToQueries_RequiresHandler(t *testing.T) {
	c, _ := newTestClientWithMock(t)
	ctx := context.Background()

	_, err := c.SubscribeToQueries(ctx, "test", "")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestSubscribeToQueries_ReceivesMessages(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh := make(chan any, 1)
	errCh := make(chan error)

	mt.OnSubscribeToQueries(func(_ context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		assert.Equal(t, "q-ch", req.Channel)
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	var received atomic.Int32
	sub, err := c.SubscribeToQueries(ctx, "q-ch", "",
		WithOnQueryReceive(func(q *QueryReceive) {
			received.Add(1)
			assert.Equal(t, "q-1", q.Id)
		}),
		WithOnError(func(err error) {}),
	)
	require.NoError(t, err)

	msgCh <- &transport.QueryReceiveItem{
		ID:         "q-1",
		Channel:    "q-ch",
		ResponseTo: "reply-q",
		Body:       []byte("fetch"),
	}

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), received.Load())

	sub.Unsubscribe()
}

func TestSubscribeToEvents_ErrorCallback(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh := make(chan any)
	errCh := make(chan error, 1)

	mt.OnSubscribeToEvents(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	var errorReceived atomic.Int32
	sub, err := c.SubscribeToEvents(ctx, "test", "",
		WithOnEvent(func(*Event) {}),
		WithOnError(func(err error) {
			errorReceived.Add(1)
		}),
	)
	require.NoError(t, err)

	errCh <- assert.AnError

	time.Sleep(100 * time.Millisecond)
	assert.GreaterOrEqual(t, errorReceived.Load(), int32(1))

	sub.Unsubscribe()
}
