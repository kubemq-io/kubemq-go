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

func newCovTestClient(t *testing.T) (*Client, *testutil.MockTransport) {
	t.Helper()
	mt := testutil.NewMockTransport()
	otel := middleware.NewOTelInterceptor(nil, nil, nil, "test", 0,
		middleware.CardinalityConfig{Threshold: 100}, "test")
	c := &Client{
		opts:      GetDefaultOptions(),
		transport: mt,
		otel:      otel,
	}
	c.opts.clientId = "cov-test"
	return c, mt
}

func TestNewClient_ValidationError(t *testing.T) {
	_, err := NewClient(context.Background(),
		WithAddress("", 0),
	)
	assert.Error(t, err)
}

func TestNewClient_TransportError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := NewClient(ctx,
		WithAddress("192.0.2.1", 50000), // RFC 5737 TEST-NET - unreachable
		WithCheckConnection(true),
	)
	assert.Error(t, err)
}

func TestSendEventStore_TransportError(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.OnSendEventStore(func(_ context.Context, _ *transport.SendEventStoreRequest) (*transport.SendEventStoreResult, error) {
		return nil, errors.New("transport error")
	})
	_, err := c.SendEventStore(context.Background(), NewEventStore().
		SetChannel("test").
		SetBody([]byte("body")))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transport error")
}

func TestSendQuery_TransportError(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.OnSendQuery(func(_ context.Context, _ *transport.SendQueryRequest) (*transport.SendQueryResult, error) {
		return nil, errors.New("query transport error")
	})
	_, err := c.SendQuery(context.Background(), NewQuery().
		SetChannel("test").
		SetBody([]byte("body")).
		SetTimeout(5*time.Second))
	assert.Error(t, err)
}

func TestSendCommand_TransportError(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.OnSendCommand(func(_ context.Context, _ *transport.SendCommandRequest) (*transport.SendCommandResult, error) {
		return nil, errors.New("cmd transport error")
	})
	_, err := c.SendCommand(context.Background(), NewCommand().
		SetChannel("test").
		SetBody([]byte("body")).
		SetTimeout(5*time.Second))
	assert.Error(t, err)
}

func TestSendQueueMessage_ClosedClient(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.Close()
	_, err := c.SendQueueMessage(context.Background(), NewQueueMessage().
		SetChannel("test").
		SetBody([]byte("body")))
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestSendQueueMessage_DefaultChannel(t *testing.T) {
	c, mt := newCovTestClient(t)
	c.opts.defaultChannel = "default-q"
	var captured string
	mt.OnSendQueueMessage(func(_ context.Context, req *transport.QueueMessageItem) (*transport.SendQueueMessageResultItem, error) {
		captured = req.Channel
		return &transport.SendQueueMessageResultItem{MessageID: "1"}, nil
	})
	_, err := c.SendQueueMessage(context.Background(), NewQueueMessage().
		SetBody([]byte("body")))
	require.NoError(t, err)
	assert.Equal(t, "default-q", captured)
}

func TestSendQueueMessages_WithPolicy(t *testing.T) {
	c, mt := newCovTestClient(t)
	var capturedPolicy *transport.QueueMessagePolicy
	mt.OnSendQueueMessages(func(_ context.Context, req *transport.SendQueueMessagesRequest) (*transport.SendQueueMessagesResult, error) {
		if len(req.Messages) > 0 {
			capturedPolicy = req.Messages[0].Policy
		}
		return &transport.SendQueueMessagesResult{
			Results: []*transport.SendQueueMessageResultItem{{MessageID: "1"}},
		}, nil
	})
	msg := NewQueueMessage().
		SetChannel("test").
		SetBody([]byte("body")).
		SetExpirationSeconds(60).
		SetDelaySeconds(10).
		SetMaxReceiveCount(3).
		SetMaxReceiveQueue("dlq")
	results, err := c.SendQueueMessages(context.Background(), []*QueueMessage{msg})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NotNil(t, capturedPolicy)
	assert.Equal(t, 60, capturedPolicy.ExpirationSeconds)
	assert.Equal(t, 10, capturedPolicy.DelaySeconds)
}

func TestAckAllQueueMessages_ClosedClient(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.Close()
	_, err := c.AckAllQueueMessages(context.Background(), &AckAllQueueMessagesRequest{Channel: "test"})
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestAckAllQueueMessages_TransportError(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.OnAckAllQueueMessages(func(_ context.Context, _ *transport.AckAllQueueMessagesReq) (*transport.AckAllQueueMessagesResp, error) {
		return nil, errors.New("ack error")
	})
	_, err := c.AckAllQueueMessages(context.Background(), &AckAllQueueMessagesRequest{Channel: "test"})
	assert.Error(t, err)
}

func TestAckAllQueueMessages_EmptyChannel(t *testing.T) {
	c, _ := newCovTestClient(t)
	resp, err := c.AckAllQueueMessages(context.Background(), &AckAllQueueMessagesRequest{
		Channel: "",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestReceiveQueueMessages_ClosedClient(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.Close()
	_, err := c.ReceiveQueueMessages(context.Background(), &ReceiveQueueMessagesRequest{Channel: "test"})
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestReceiveQueueMessages_WithPolicyAndAttributes(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.OnReceiveQueueMessages(func(_ context.Context, _ *transport.ReceiveQueueMessagesReq) (*transport.ReceiveQueueMessagesResp, error) {
		return &transport.ReceiveQueueMessagesResp{
			RequestID:        "req-1",
			MessagesReceived: 1,
			Messages: []*transport.QueueMessageItem{
				{
					ID:      "msg-1",
					Channel: "test",
					Body:    []byte("hello"),
					Policy:  &transport.QueueMessagePolicy{ExpirationSeconds: 30},
					Attributes: &transport.QueueMessageAttributes{
						Timestamp:    1234567890,
						Sequence:     1,
						ReceiveCount: 0,
					},
				},
			},
		}, nil
	})
	resp, err := c.ReceiveQueueMessages(context.Background(), &ReceiveQueueMessagesRequest{Channel: "test", MaxNumberOfMessages: 1, WaitTimeSeconds: 5})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.NotNil(t, resp.Messages[0].Policy)
	assert.Equal(t, 30, resp.Messages[0].Policy.ExpirationSeconds)
	assert.NotNil(t, resp.Messages[0].Attributes)
	assert.Equal(t, int64(1234567890), resp.Messages[0].Attributes.Timestamp)
}

func TestSendResponse_ClosedClient(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.Close()
	err := c.SendResponse(context.Background(), NewResponse().
		SetRequestId("req-1").
		SetResponseTo("resp-to"))
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestSendEvent_ClosedClient(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.Close()
	err := c.SendEvent(context.Background(), NewEvent().
		SetChannel("test").
		SetBody([]byte("body")))
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestCreateChannel_ClosedClient(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.Close()
	err := c.CreateChannel(context.Background(), "test", ChannelTypeEvents)
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestDeleteChannel_ClosedClient(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.Close()
	err := c.DeleteChannel(context.Background(), "test", ChannelTypeEvents)
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestNewClientFromAddress_InvalidAddress(t *testing.T) {
	_, err := NewClientFromAddress("invalid")
	assert.Error(t, err)
}

func TestNewClientFromAddress_EmptyAddress(t *testing.T) {
	_, err := NewClientFromAddress("")
	assert.Error(t, err)
}

func TestSendQueueMessages_ClosedClient(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.Close()
	_, err := c.SendQueueMessages(context.Background(), []*QueueMessage{
		NewQueueMessage().SetChannel("test").SetBody([]byte("body")),
	})
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestReceiveQueueMessages_WithAttributesAndPolicy(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnReceiveQueueMessages(func(_ context.Context, req *transport.ReceiveQueueMessagesReq) (*transport.ReceiveQueueMessagesResp, error) {
		return &transport.ReceiveQueueMessagesResp{
			Messages: []*transport.QueueMessageItem{
				{
					ID: "m1", Channel: "q-ch", Body: []byte("data"),
					Policy:     &transport.QueueMessagePolicy{ExpirationSeconds: 30, DelaySeconds: 5},
					Attributes: &transport.QueueMessageAttributes{Timestamp: 100, Sequence: 1},
				},
			},
			MessagesReceived: 1,
		}, nil
	})
	resp, err := c.ReceiveQueueMessages(context.Background(), &ReceiveQueueMessagesRequest{
		Channel: "q-ch", MaxNumberOfMessages: 10, WaitTimeSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	require.NotNil(t, resp.Messages[0].Policy)
	assert.Equal(t, 30, resp.Messages[0].Policy.ExpirationSeconds)
	require.NotNil(t, resp.Messages[0].Attributes)
	assert.Equal(t, uint64(1), resp.Messages[0].Attributes.Sequence)
}

func TestReceiveQueueMessages_EmptyChannel(t *testing.T) {
	c, _ := newSendTestClient(t)
	_, err := c.ReceiveQueueMessages(context.Background(), &ReceiveQueueMessagesRequest{
		MaxNumberOfMessages: 10, WaitTimeSeconds: 5,
	})
	assert.NoError(t, err)
}

func TestSendQueueMessage_WithPolicy(t *testing.T) {
	c, mt := newSendTestClient(t)
	mt.OnSendQueueMessage(func(_ context.Context, req *transport.QueueMessageItem) (*transport.SendQueueMessageResultItem, error) {
		require.NotNil(t, req.Policy)
		assert.Equal(t, 60, req.Policy.ExpirationSeconds)
		return &transport.SendQueueMessageResultItem{MessageID: "m1"}, nil
	})
	msg := c.NewQueueMessage()
	msg.Channel = "q-ch"
	msg.Body = []byte("data")
	msg.Policy = &QueuePolicy{ExpirationSeconds: 60}
	_, err := c.SendQueueMessage(context.Background(), msg)
	assert.NoError(t, err)
}

func TestReceiveQueueMessages_InvalidMaxMessages(t *testing.T) {
	c, _ := newSendTestClient(t)
	_, err := c.ReceiveQueueMessages(context.Background(), &ReceiveQueueMessagesRequest{
		Channel: "q-ch", MaxNumberOfMessages: 0, WaitTimeSeconds: 5,
	})
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "MaxNumberOfMessages")
}

func TestReceiveQueueMessages_InvalidChannel(t *testing.T) {
	c, _ := newSendTestClient(t)
	_, err := c.ReceiveQueueMessages(context.Background(), &ReceiveQueueMessagesRequest{
		Channel: "invalid*channel", MaxNumberOfMessages: 10, WaitTimeSeconds: 5,
	})
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "wildcard")
}

func TestAckAllQueueMessages_InvalidChannel(t *testing.T) {
	c, _ := newSendTestClient(t)
	_, err := c.AckAllQueueMessages(context.Background(), &AckAllQueueMessagesRequest{
		Channel: "invalid*channel", WaitTimeSeconds: 5,
	})
	require.Error(t, err)
}
