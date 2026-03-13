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
	mt.OnSendQueueMessages(func(_ context.Context, req *transport.SendQueueMessagesRequest) (*transport.SendQueueMessagesResult, error) {
		if len(req.Messages) > 0 {
			captured = req.Messages[0].Channel
		}
		return &transport.SendQueueMessagesResult{
			Results: []*transport.SendQueueMessageResultItem{{MessageID: "1"}},
		}, nil
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

func TestQueuesInfo_ClosedClient(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.Close()
	_, err := c.QueuesInfo(context.Background(), "")
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestQueuesInfo_TransportError(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.OnQueuesInfo(func(_ context.Context, _ string) (*transport.QueuesInfoResult, error) {
		return nil, errors.New("info error")
	})
	_, err := c.QueuesInfo(context.Background(), "")
	assert.Error(t, err)
}

func TestQueuesInfo_WithQueues(t *testing.T) {
	c, mt := newCovTestClient(t)
	mt.OnQueuesInfo(func(_ context.Context, _ string) (*transport.QueuesInfoResult, error) {
		return &transport.QueuesInfoResult{
			TotalQueue: 2,
			Sent:       100,
			Delivered:  80,
			Waiting:    20,
			Queues: []*transport.QueueInfoItem{
				{Name: "q1", Messages: 10, Bytes: 1024, FirstSeq: 1, LastSeq: 10, Sent: 50, Delivered: 40, Waiting: 10, Subscribers: 2},
				{Name: "q2", Messages: 5, Bytes: 512, FirstSeq: 1, LastSeq: 5, Sent: 50, Delivered: 40, Waiting: 10, Subscribers: 1},
			},
		}, nil
	})
	info, err := c.QueuesInfo(context.Background(), "")
	require.NoError(t, err)
	assert.Equal(t, int32(2), info.TotalQueue)
	require.Len(t, info.Queues, 2)
	assert.Equal(t, "q1", info.Queues[0].Name)
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
