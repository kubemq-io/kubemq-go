package kubemq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/testutil"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSendTestClient(t *testing.T) (*Client, *testutil.MockTransport) {
	t.Helper()
	mt := testutil.NewMockTransport()
	otel := middleware.NewOTelInterceptor(nil, nil, nil, "test", 0,
		middleware.CardinalityConfig{Threshold: 100}, "test")
	c := &Client{
		opts:      GetDefaultOptions(),
		transport: mt,
		otel:      otel,
	}
	return c, mt
}

func TestSendEvent_Success(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnSendEvent(func(_ context.Context, req *transport.SendEventRequest) error {
		assert.Equal(t, "test-ch", req.Channel)
		assert.Equal(t, []byte("hi"), req.Body)
		assert.Equal(t, "m", req.Metadata)
		return nil
	})
	err := c.SendEvent(context.Background(),
		NewEvent().SetChannel("test-ch").SetBody([]byte("hi")).SetMetadata("m"))
	assert.NoError(t, err)
}

func TestSendEvent_ValidationError(t *testing.T) {
	c, _ := newSendTestClient(t)
	err := c.SendEvent(context.Background(), NewEvent().SetBody([]byte("hi")))
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestSendEvent_TransportError(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnSendEvent(func(_ context.Context, _ *transport.SendEventRequest) error {
		return errors.New("transport failure")
	})
	err := c.SendEvent(context.Background(),
		NewEvent().SetChannel("test-ch").SetBody([]byte("hi")))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transport failure")
}

func TestSendEvent_DefaultChannel(t *testing.T) {
	c, mt := newSendTestClient(t)
	c.opts.defaultChannel = "default-ch"
	mt.OnSendEvent(func(_ context.Context, req *transport.SendEventRequest) error {
		assert.Equal(t, "default-ch", req.Channel)
		return nil
	})
	err := c.SendEvent(context.Background(),
		NewEvent().SetBody([]byte("hi")))
	assert.NoError(t, err)
}

func TestSendEventStore_Success(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnSendEventStore(func(_ context.Context, req *transport.SendEventStoreRequest) (*transport.SendEventStoreResult, error) {
		assert.Equal(t, "store-ch", req.Channel)
		return &transport.SendEventStoreResult{ID: "id-1", Sent: true}, nil
	})
	result, err := c.SendEventStore(context.Background(),
		NewEventStore().SetChannel("store-ch").SetBody([]byte("data")))
	require.NoError(t, err)
	assert.True(t, result.Sent)
	assert.Equal(t, "id-1", result.Id)
}

func TestSendCommand_Success(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnSendCommand(func(_ context.Context, req *transport.SendCommandRequest) (*transport.SendCommandResult, error) {
		assert.Equal(t, "cmd-ch", req.Channel)
		return &transport.SendCommandResult{Executed: true, CommandID: "cmd-1"}, nil
	})
	resp, err := c.SendCommand(context.Background(),
		NewCommand().SetChannel("cmd-ch").SetBody([]byte("do")).SetTimeout(5*time.Second))
	require.NoError(t, err)
	assert.True(t, resp.Executed)
	assert.Equal(t, "cmd-1", resp.CommandId)
}

func TestSendCommand_ValidationError(t *testing.T) {
	c, _ := newSendTestClient(t)
	_, err := c.SendCommand(context.Background(),
		NewCommand().SetBody([]byte("do")).SetTimeout(5*time.Second))
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestSendQuery_Success(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnSendQuery(func(_ context.Context, req *transport.SendQueryRequest) (*transport.SendQueryResult, error) {
		assert.Equal(t, "q-ch", req.Channel)
		return &transport.SendQueryResult{Executed: true, QueryID: "q-1", Body: []byte("answer")}, nil
	})
	resp, err := c.SendQuery(context.Background(),
		NewQuery().SetChannel("q-ch").SetBody([]byte("ask")).SetTimeout(5*time.Second))
	require.NoError(t, err)
	assert.True(t, resp.Executed)
	assert.Equal(t, "q-1", resp.QueryId)
	assert.Equal(t, []byte("answer"), resp.Body)
}

func TestSendCommandResponse_Success(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnSendResponse(func(_ context.Context, req *transport.SendResponseRequest) error {
		assert.Equal(t, "req-1", req.RequestID)
		assert.Equal(t, "reply-to", req.ResponseTo)
		return nil
	})
	err := c.SendCommandResponse(context.Background(),
		NewCommandReply().SetRequestId("req-1").SetResponseTo("reply-to"))
	assert.NoError(t, err)
}

func TestSendCommandResponse_ValidationError(t *testing.T) {
	c, _ := newSendTestClient(t)
	err := c.SendCommandResponse(context.Background(), NewCommandReply())
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestSendQueryResponse_Success(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnSendResponse(func(_ context.Context, req *transport.SendResponseRequest) error {
		assert.Equal(t, "req-2", req.RequestID)
		assert.Equal(t, "reply-to-2", req.ResponseTo)
		return nil
	})
	err := c.SendQueryResponse(context.Background(),
		NewQueryReply().SetRequestId("req-2").SetResponseTo("reply-to-2"))
	assert.NoError(t, err)
}

func TestSendQueryResponse_ValidationError(t *testing.T) {
	c, _ := newSendTestClient(t)
	err := c.SendQueryResponse(context.Background(), NewQueryReply())
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestSendQueueMessage_Success(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnSendQueueMessage(func(_ context.Context, req *transport.QueueMessageItem) (*transport.QueueSendResultItem, error) {
		assert.Equal(t, "q-ch", req.Channel)
		return &transport.QueueSendResultItem{MessageID: "msg-1", SentAt: 100}, nil
	})
	result, err := c.SendQueueMessage(context.Background(),
		NewQueueMessage().SetChannel("q-ch").SetBody([]byte("payload")))
	require.NoError(t, err)
	assert.Equal(t, "msg-1", result.MessageID)
}

func TestSendQueueMessages_Success(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnSendQueueMessages(func(_ context.Context, req *transport.SendQueueMessagesRequest) (*transport.SendQueueMessagesResult, error) {
		require.Len(t, req.Messages, 2)
		return &transport.SendQueueMessagesResult{
			Results: []*transport.QueueSendResultItem{
				{MessageID: "msg-1"},
				{MessageID: "msg-2"},
			},
		}, nil
	})
	msgs := []*QueueMessage{
		NewQueueMessage().SetChannel("q-ch").SetBody([]byte("a")),
		NewQueueMessage().SetChannel("q-ch").SetBody([]byte("b")),
	}
	results, err := c.SendQueueMessages(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, results, 2)
}

func TestAckAllQueueMessages_Success(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnAckAllQueueMessages(func(_ context.Context, req *transport.AckAllQueueMessagesReq) (*transport.AckAllQueueMessagesResp, error) {
		assert.Equal(t, "q-ch", req.Channel)
		return &transport.AckAllQueueMessagesResp{
			AffectedMessages: 5,
		}, nil
	})
	resp, err := c.AckAllQueueMessages(context.Background(), &AckAllQueueMessagesRequest{
		Channel:         "q-ch",
		WaitTimeSeconds: 5,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(5), resp.AffectedMessages)
}

func TestClient_State(t *testing.T) {
	c, _ := newSendTestClient(t)
	assert.Equal(t, StateReady, c.State())
}

func TestClient_Close(t *testing.T) {
	c, _ := newSendTestClient(t)
	err := c.Close()
	assert.NoError(t, err)
	assert.Equal(t, StateClosed, c.State())
}

func TestClient_Ping(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnPing(func(_ context.Context) (*transport.ServerInfoResult, error) {
		return &transport.ServerInfoResult{Host: "test-host", Version: "2.0.0"}, nil
	})
	info, err := c.Ping(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "test-host", info.Host)
	assert.Equal(t, "2.0.0", info.Version)
}

func TestClient_NewCommand(t *testing.T) {
	c, _ := newSendTestClient(t)
	c.opts.clientId = "test-client"
	cmd := c.NewCommand()
	assert.NotNil(t, cmd)
	assert.Equal(t, "test-client", cmd.ClientId)
	assert.Equal(t, defaultRequestTimeout, cmd.Timeout)
	assert.NotNil(t, cmd.Tags)
}

func TestClient_NewCommandReply(t *testing.T) {
	c, _ := newSendTestClient(t)
	c.opts.clientId = "test-client"
	resp := c.NewCommandReply()
	assert.NotNil(t, resp)
	assert.Equal(t, "test-client", resp.ClientId)
	assert.NotNil(t, resp.Tags)
}

func TestClient_NewQueryReply(t *testing.T) {
	c, _ := newSendTestClient(t)
	c.opts.clientId = "test-client"
	resp := c.NewQueryReply()
	assert.NotNil(t, resp)
	assert.Equal(t, "test-client", resp.ClientId)
	assert.NotNil(t, resp.Tags)
}

func TestClient_NewQuery(t *testing.T) {
	c, _ := newSendTestClient(t)
	c.opts.clientId = "test-client"
	q := c.NewQuery()
	assert.NotNil(t, q)
	assert.Equal(t, "test-client", q.ClientId)
	assert.Equal(t, defaultRequestTimeout, q.Timeout)
	assert.NotNil(t, q.Tags)
}

func TestClient_NewEvent(t *testing.T) {
	c, _ := newSendTestClient(t)
	c.opts.clientId = "test-client"
	e := c.NewEvent()
	assert.NotNil(t, e)
	assert.Equal(t, "test-client", e.ClientId)
	assert.NotNil(t, e.Tags)
}

func TestClient_NewEventStore(t *testing.T) {
	c, _ := newSendTestClient(t)
	c.opts.clientId = "test-client"
	es := c.NewEventStore()
	assert.NotNil(t, es)
	assert.Equal(t, "test-client", es.ClientId)
	assert.NotNil(t, es.Tags)
}

func TestClient_NewQueueMessage(t *testing.T) {
	c, _ := newSendTestClient(t)
	c.opts.clientId = "test-client"
	qm := c.NewQueueMessage()
	assert.NotNil(t, qm)
	assert.Equal(t, "test-client", qm.ClientID)
	assert.NotNil(t, qm.Tags)
}

func TestSendEventStore_NotSent(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnSendEventStore(func(_ context.Context, _ *transport.SendEventStoreRequest) (*transport.SendEventStoreResult, error) {
		return &transport.SendEventStoreResult{
			ID:   "es-fail",
			Sent: false,
			Err:  errors.New("store error"),
		}, nil
	})
	result, err := c.SendEventStore(context.Background(),
		NewEventStore().SetChannel("store-ch").SetBody([]byte("data")))
	require.NoError(t, err)
	assert.False(t, result.Sent)
	assert.Equal(t, "es-fail", result.Id)
	require.Error(t, result.Err)
	assert.Contains(t, result.Err.Error(), "store error")
}

func TestClient_NewQueueMessages(t *testing.T) {
	c, _ := newSendTestClient(t)
	qms := c.NewQueueMessages()
	assert.NotNil(t, qms)
	assert.NotNil(t, qms.Messages)
	assert.Empty(t, qms.Messages)
}

func TestClient_CheckClosed(t *testing.T) {
	c, _ := newSendTestClient(t)
	assert.NoError(t, c.checkClosed())

	_ = c.Close()
	err := c.checkClosed()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrClientClosed)
}
