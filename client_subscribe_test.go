package kubemq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/testutil"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSubscribeTestClient(t *testing.T, extraOpts ...func(*Client)) (*Client, *testutil.MockTransport) {
	t.Helper()
	mt := testutil.NewMockTransport()
	otel := middleware.NewOTelInterceptor(nil, nil, nil, "test", 0,
		middleware.CardinalityConfig{Threshold: 100}, "test")
	c := &Client{
		opts:      GetDefaultOptions(),
		transport: mt,
		otel:      otel,
	}
	for _, fn := range extraOpts {
		fn(c)
	}
	return c, mt
}

// ---------------------------------------------------------------------------
// SubscribeToEvents
// ---------------------------------------------------------------------------

func TestSubscribeToEvents_RequiresHandler(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_, err := c.SubscribeToEvents(context.Background(), "test", "")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestSubscribeToEvents_ClosedClient(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_ = c.Close()
	_, err := c.SubscribeToEvents(context.Background(), "test-ch", "",
		WithOnEvent(func(*Event) {}))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestSubscribeToEvents_InvalidChannel(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_, err := c.SubscribeToEvents(context.Background(), "", "",
		WithOnEvent(func(*Event) {}))
	require.Error(t, err)
}

func TestSubscribeToEvents_TransportError(t *testing.T) {
	c, mt := newSubscribeTestClient(t)
	mt.OnSubscribeToEvents(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return nil, fmt.Errorf("subscribe transport failure")
	})
	_, err := c.SubscribeToEvents(context.Background(), "test-ch", "",
		WithOnEvent(func(*Event) {}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "subscribe transport failure")
}

func TestSubscribeToEvents_DefaultErrorHandler(t *testing.T) {
	c, mt := newSubscribeTestClient(t, func(c *Client) {
		c.opts.logger = NewSlogAdapter(slog.Default())
	})

	msgCh := make(chan any)
	errCh := make(chan error, 1)
	mt.OnSubscribeToEvents(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	sub, err := c.SubscribeToEvents(context.Background(), "test-ch", "",
		WithOnEvent(func(*Event) {}))
	require.NoError(t, err)

	errCh <- fmt.Errorf("injected error")
	time.Sleep(150 * time.Millisecond)

	sub.Unsubscribe()
}

func TestSubscribeToEvents_ContextCancel(t *testing.T) {
	c, mt := newSubscribeTestClient(t)

	msgCh := make(chan any)
	errCh := make(chan error)
	mt.OnSubscribeToEvents(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	sub, err := c.SubscribeToEvents(ctx, "test-ch", "",
		WithOnEvent(func(*Event) {}))
	require.NoError(t, err)

	cancel()
	time.Sleep(150 * time.Millisecond)
	assert.True(t, sub.IsDone())
}

func TestSubscribeToEvents_ReceivesMessages(t *testing.T) {
	c, mt := newSubscribeTestClient(t)
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

func TestSubscribeToEvents_ErrorCallback(t *testing.T) {
	c, mt := newSubscribeTestClient(t)
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

// ---------------------------------------------------------------------------
// SubscribeToEventsStore
// ---------------------------------------------------------------------------

func TestSubscribeToEventsStore_RequiresHandler(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_, err := c.SubscribeToEventsStore(context.Background(), "test", "", StartFromNewEvents())
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestSubscribeToEventsStore_ClosedClient(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_ = c.Close()
	_, err := c.SubscribeToEventsStore(context.Background(), "test-ch", "", StartFromNewEvents(),
		WithOnEventStoreReceive(func(*EventStoreReceive) {}))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestSubscribeToEventsStore_InvalidChannel(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_, err := c.SubscribeToEventsStore(context.Background(), "", "", StartFromNewEvents(),
		WithOnEventStoreReceive(func(*EventStoreReceive) {}))
	require.Error(t, err)
}

func TestSubscribeToEventsStore_TransportError(t *testing.T) {
	c, mt := newSubscribeTestClient(t)
	mt.OnSubscribeToEventsStore(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return nil, fmt.Errorf("event store transport failure")
	})
	_, err := c.SubscribeToEventsStore(context.Background(), "test-ch", "", StartFromNewEvents(),
		WithOnEventStoreReceive(func(*EventStoreReceive) {}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "event store transport failure")
}

func TestSubscribeToEventsStore_DefaultErrorHandler(t *testing.T) {
	c, mt := newSubscribeTestClient(t, func(c *Client) {
		c.opts.logger = NewSlogAdapter(slog.Default())
	})

	msgCh := make(chan any)
	errCh := make(chan error, 1)
	mt.OnSubscribeToEventsStore(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	sub, err := c.SubscribeToEventsStore(context.Background(), "test-ch", "", StartFromNewEvents(),
		WithOnEventStoreReceive(func(*EventStoreReceive) {}))
	require.NoError(t, err)

	errCh <- fmt.Errorf("injected es error")
	time.Sleep(150 * time.Millisecond)

	sub.Unsubscribe()
}

func TestSubscribeToEventsStore_ContextCancel(t *testing.T) {
	c, mt := newSubscribeTestClient(t)

	msgCh := make(chan any)
	errCh := make(chan error)
	mt.OnSubscribeToEventsStore(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	sub, err := c.SubscribeToEventsStore(ctx, "test-ch", "", StartFromNewEvents(),
		WithOnEventStoreReceive(func(*EventStoreReceive) {}))
	require.NoError(t, err)

	cancel()
	time.Sleep(150 * time.Millisecond)
	assert.True(t, sub.IsDone())
}

func TestSubscribeToEventsStore_ReceivesMessages(t *testing.T) {
	c, mt := newSubscribeTestClient(t)
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

// ---------------------------------------------------------------------------
// SubscribeToCommands
// ---------------------------------------------------------------------------

func TestSubscribeToCommands_RequiresHandler(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_, err := c.SubscribeToCommands(context.Background(), "test", "")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestSubscribeToCommands_ClosedClient(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_ = c.Close()
	_, err := c.SubscribeToCommands(context.Background(), "test-ch", "",
		WithOnCommandReceive(func(*CommandReceive) {}))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestSubscribeToCommands_InvalidChannel(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_, err := c.SubscribeToCommands(context.Background(), "", "",
		WithOnCommandReceive(func(*CommandReceive) {}))
	require.Error(t, err)
}

func TestSubscribeToCommands_TransportError(t *testing.T) {
	c, mt := newSubscribeTestClient(t)
	mt.OnSubscribeToCommands(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return nil, fmt.Errorf("command transport failure")
	})
	_, err := c.SubscribeToCommands(context.Background(), "test-ch", "",
		WithOnCommandReceive(func(*CommandReceive) {}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "command transport failure")
}

func TestSubscribeToCommands_DefaultErrorHandler(t *testing.T) {
	c, mt := newSubscribeTestClient(t, func(c *Client) {
		c.opts.logger = NewSlogAdapter(slog.Default())
	})

	msgCh := make(chan any)
	errCh := make(chan error, 1)
	mt.OnSubscribeToCommands(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	sub, err := c.SubscribeToCommands(context.Background(), "test-ch", "",
		WithOnCommandReceive(func(*CommandReceive) {}))
	require.NoError(t, err)

	errCh <- fmt.Errorf("injected cmd error")
	time.Sleep(150 * time.Millisecond)

	sub.Unsubscribe()
}

func TestSubscribeToCommands_ContextCancel(t *testing.T) {
	c, mt := newSubscribeTestClient(t)

	msgCh := make(chan any)
	errCh := make(chan error)
	mt.OnSubscribeToCommands(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	sub, err := c.SubscribeToCommands(ctx, "test-ch", "",
		WithOnCommandReceive(func(*CommandReceive) {}))
	require.NoError(t, err)

	cancel()
	time.Sleep(150 * time.Millisecond)
	assert.True(t, sub.IsDone())
}

func TestSubscribeToCommands_ReceivesMessages(t *testing.T) {
	c, mt := newSubscribeTestClient(t)
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

// ---------------------------------------------------------------------------
// SubscribeToQueries
// ---------------------------------------------------------------------------

func TestSubscribeToQueries_RequiresHandler(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_, err := c.SubscribeToQueries(context.Background(), "test", "")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestSubscribeToQueries_ClosedClient(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_ = c.Close()
	_, err := c.SubscribeToQueries(context.Background(), "test-ch", "",
		WithOnQueryReceive(func(*QueryReceive) {}))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestSubscribeToQueries_InvalidChannel(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_, err := c.SubscribeToQueries(context.Background(), "", "",
		WithOnQueryReceive(func(*QueryReceive) {}))
	require.Error(t, err)
}

func TestSubscribeToQueries_TransportError(t *testing.T) {
	c, mt := newSubscribeTestClient(t)
	mt.OnSubscribeToQueries(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return nil, fmt.Errorf("query transport failure")
	})
	_, err := c.SubscribeToQueries(context.Background(), "test-ch", "",
		WithOnQueryReceive(func(*QueryReceive) {}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query transport failure")
}

func TestSubscribeToQueries_DefaultErrorHandler(t *testing.T) {
	c, mt := newSubscribeTestClient(t, func(c *Client) {
		c.opts.logger = NewSlogAdapter(slog.Default())
	})

	msgCh := make(chan any)
	errCh := make(chan error, 1)
	mt.OnSubscribeToQueries(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	sub, err := c.SubscribeToQueries(context.Background(), "test-ch", "",
		WithOnQueryReceive(func(*QueryReceive) {}))
	require.NoError(t, err)

	errCh <- fmt.Errorf("injected query error")
	time.Sleep(150 * time.Millisecond)

	sub.Unsubscribe()
}

func TestSubscribeToQueries_ContextCancel(t *testing.T) {
	c, mt := newSubscribeTestClient(t)

	msgCh := make(chan any)
	errCh := make(chan error)
	mt.OnSubscribeToQueries(func(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
		return transport.NewStreamHandle(msgCh, errCh, func() {}), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	sub, err := c.SubscribeToQueries(ctx, "test-ch", "",
		WithOnQueryReceive(func(*QueryReceive) {}))
	require.NoError(t, err)

	cancel()
	time.Sleep(150 * time.Millisecond)
	assert.True(t, sub.IsDone())
}

func TestSubscribeToQueries_ReceivesMessages(t *testing.T) {
	c, mt := newSubscribeTestClient(t)
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

// ---------------------------------------------------------------------------
// Send method default fallbacks
// ---------------------------------------------------------------------------

func TestSendCommand_DefaultTimeout(t *testing.T) {
	c, mt := newSubscribeTestClient(t)
	var captured time.Duration
	mt.OnSendCommand(func(_ context.Context, req *transport.SendCommandRequest) (*transport.SendCommandResult, error) {
		captured = req.Timeout
		return &transport.SendCommandResult{Executed: true}, nil
	})
	cmd := &Command{
		Channel: "cmd-ch",
		Body:    []byte("payload"),
	}
	_, err := c.SendCommand(context.Background(), cmd)
	require.NoError(t, err)
	assert.Equal(t, defaultRequestTimeout, captured)
}

func TestSendQuery_DefaultTimeout(t *testing.T) {
	c, mt := newSubscribeTestClient(t)
	var captured time.Duration
	mt.OnSendQuery(func(_ context.Context, req *transport.SendQueryRequest) (*transport.SendQueryResult, error) {
		captured = req.Timeout
		return &transport.SendQueryResult{Executed: true}, nil
	})
	q := &Query{
		Channel: "q-ch",
		Body:    []byte("payload"),
	}
	_, err := c.SendQuery(context.Background(), q)
	require.NoError(t, err)
	assert.Equal(t, defaultRequestTimeout, captured)
}

func TestSendCommand_DefaultChannel(t *testing.T) {
	c, mt := newSubscribeTestClient(t, func(c *Client) {
		c.opts.defaultChannel = "default-cmd-ch"
	})
	var captured string
	mt.OnSendCommand(func(_ context.Context, req *transport.SendCommandRequest) (*transport.SendCommandResult, error) {
		captured = req.Channel
		return &transport.SendCommandResult{Executed: true}, nil
	})
	cmd := &Command{
		Body:    []byte("payload"),
		Timeout: 5 * time.Second,
	}
	_, err := c.SendCommand(context.Background(), cmd)
	require.NoError(t, err)
	assert.Equal(t, "default-cmd-ch", captured)
}

func TestSendQuery_DefaultChannel(t *testing.T) {
	c, mt := newSubscribeTestClient(t, func(c *Client) {
		c.opts.defaultChannel = "default-q-ch"
	})
	var captured string
	mt.OnSendQuery(func(_ context.Context, req *transport.SendQueryRequest) (*transport.SendQueryResult, error) {
		captured = req.Channel
		return &transport.SendQueryResult{Executed: true}, nil
	})
	q := &Query{
		Body:    []byte("payload"),
		Timeout: 5 * time.Second,
	}
	_, err := c.SendQuery(context.Background(), q)
	require.NoError(t, err)
	assert.Equal(t, "default-q-ch", captured)
}

func TestSendEventStore_DefaultChannel(t *testing.T) {
	c, mt := newSubscribeTestClient(t, func(c *Client) {
		c.opts.defaultChannel = "default-es-ch"
	})
	var captured string
	mt.OnSendEventStore(func(_ context.Context, req *transport.SendEventStoreRequest) (*transport.SendEventStoreResult, error) {
		captured = req.Channel
		return &transport.SendEventStoreResult{Sent: true}, nil
	})
	es := &EventStore{
		Body: []byte("payload"),
	}
	_, err := c.SendEventStore(context.Background(), es)
	require.NoError(t, err)
	assert.Equal(t, "default-es-ch", captured)
}

// ---------------------------------------------------------------------------
// Queue edge cases
// ---------------------------------------------------------------------------

func TestSendQueueMessage_EmptyResults(t *testing.T) {
	c, mt := newSubscribeTestClient(t)
	mt.OnSendQueueMessages(func(_ context.Context, _ *transport.SendQueueMessagesRequest) (*transport.SendQueueMessagesResult, error) {
		return &transport.SendQueueMessagesResult{
			Results: []*transport.SendQueueMessageResultItem{},
		}, nil
	})
	msg := &QueueMessage{
		Channel: "q-ch",
		Body:    []byte("payload"),
	}
	_, err := c.SendQueueMessage(context.Background(), msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "returned no results")
}

func TestPing_ClosedClient(t *testing.T) {
	c, _ := newSubscribeTestClient(t)
	_ = c.Close()
	_, err := c.Ping(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestClose_NilTransport(t *testing.T) {
	c := &Client{
		opts: GetDefaultOptions(),
		otel: middleware.NewOTelInterceptor(nil, nil, nil, "test", 0,
			middleware.CardinalityConfig{Threshold: 100}, "test"),
	}
	assert.NotPanics(t, func() {
		err := c.Close()
		assert.NoError(t, err)
	})
}
