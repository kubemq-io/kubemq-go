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

func newConvenienceTestClient(t *testing.T) (*Client, *testutil.MockTransport) {
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

func TestPublishEvent_Success(t *testing.T) {
	c, mt := newConvenienceTestClient(t)
	mt.OnSendEvent(func(_ context.Context, req *transport.SendEventRequest) error {
		assert.Equal(t, "events-ch", req.Channel)
		assert.Equal(t, []byte("payload"), req.Body)
		return nil
	})
	err := c.PublishEvent(context.Background(), "events-ch", []byte("payload"))
	assert.NoError(t, err)
}

func TestPublishEventStore_Success(t *testing.T) {
	c, mt := newConvenienceTestClient(t)
	mt.OnSendEventStore(func(_ context.Context, req *transport.SendEventStoreRequest) (*transport.SendEventStoreResult, error) {
		assert.Equal(t, "store-ch", req.Channel)
		return &transport.SendEventStoreResult{Sent: true, ID: "es-1"}, nil
	})
	result, err := c.PublishEventStore(context.Background(), "store-ch", []byte("payload"))
	require.NoError(t, err)
	assert.True(t, result.Sent)
}

func TestSendQueueMessageSimple_Success(t *testing.T) {
	c, mt := newConvenienceTestClient(t)
	mt.OnSendQueueMessage(func(_ context.Context, req *transport.QueueMessageItem) (*transport.QueueSendResultItem, error) {
		assert.Equal(t, "q-ch", req.Channel)
		return &transport.QueueSendResultItem{MessageID: "qm-1", SentAt: 123}, nil
	})
	result, err := c.SendQueueMessageSimple(context.Background(), "q-ch", []byte("data"))
	require.NoError(t, err)
	assert.Equal(t, "qm-1", result.MessageID)
}

func TestSendCommandSimple_Success(t *testing.T) {
	c, mt := newConvenienceTestClient(t)
	mt.OnSendCommand(func(_ context.Context, req *transport.SendCommandRequest) (*transport.SendCommandResult, error) {
		assert.Equal(t, "cmd-ch", req.Channel)
		return &transport.SendCommandResult{Executed: true, CommandID: "c-1"}, nil
	})
	resp, err := c.SendCommandSimple(context.Background(), "cmd-ch", []byte("do"), 10*time.Second)
	require.NoError(t, err)
	assert.True(t, resp.Executed)
}

func TestSendQuerySimple_Success(t *testing.T) {
	c, mt := newConvenienceTestClient(t)
	mt.OnSendQuery(func(_ context.Context, req *transport.SendQueryRequest) (*transport.SendQueryResult, error) {
		assert.Equal(t, "q-ch", req.Channel)
		return &transport.SendQueryResult{Executed: true, QueryID: "q-1", Body: []byte("result")}, nil
	})
	resp, err := c.SendQuerySimple(context.Background(), "q-ch", []byte("ask"), 10*time.Second)
	require.NoError(t, err)
	assert.True(t, resp.Executed)
	assert.Equal(t, []byte("result"), resp.Body)
}

// --- Additional convenience function tests ---

func TestWithTags_MultipleTags(t *testing.T) {
	tags := map[string]string{"env": "prod", "region": "us-east", "team": "platform"}
	opt := WithTags(tags)

	e := &Event{}
	opt.applyPublish(e)
	assert.Len(t, e.Tags, 3)
	assert.Equal(t, "prod", e.Tags["env"])
	assert.Equal(t, "us-east", e.Tags["region"])
	assert.Equal(t, "platform", e.Tags["team"])
}

func TestWithTags_AppliedToBothEventAndStore(t *testing.T) {
	tags := map[string]string{"env": "staging"}
	opt := WithTags(tags)

	e := &Event{}
	es := &EventStore{}
	opt.applyPublish(e)
	opt.applyPublishStore(es)
	assert.Equal(t, tags, e.Tags)
	assert.Equal(t, tags, es.Tags)
}

func TestWithID_AppliedToBothEventAndStore(t *testing.T) {
	opt := WithID("shared-id-456")

	e := &Event{}
	es := &EventStore{}
	opt.applyPublish(e)
	opt.applyPublishStore(es)
	assert.Equal(t, "shared-id-456", e.Id)
	assert.Equal(t, "shared-id-456", es.Id)
}

func TestWithMaxReceive_PreservesExistingPolicy(t *testing.T) {
	opt := WithMaxReceive(5, "dlq")

	m := &QueueMessage{Policy: &QueuePolicy{ExpirationSeconds: 100}}
	opt.applyQueueSend(m)
	assert.Equal(t, 5, m.Policy.MaxReceiveCount)
	assert.Equal(t, "dlq", m.Policy.MaxReceiveQueue)
	assert.Equal(t, 100, m.Policy.ExpirationSeconds)
}

func TestPublishEvent_WithOptions(t *testing.T) {
	c, mt := newConvenienceTestClient(t)
	mt.OnSendEvent(func(_ context.Context, req *transport.SendEventRequest) error {
		assert.Equal(t, "ch", req.Channel)
		assert.Equal(t, "test-meta", req.Metadata)
		assert.Equal(t, "custom-id", req.ID)
		assert.Equal(t, map[string]string{"k": "v"}, req.Tags)
		return nil
	})
	err := c.PublishEvent(context.Background(), "ch", []byte("body"),
		WithMetadata("test-meta"),
		WithID("custom-id"),
		WithTags(map[string]string{"k": "v"}),
	)
	assert.NoError(t, err)
}

func TestPublishEventStore_WithOptions(t *testing.T) {
	c, mt := newConvenienceTestClient(t)
	mt.OnSendEventStore(func(_ context.Context, req *transport.SendEventStoreRequest) (*transport.SendEventStoreResult, error) {
		assert.Equal(t, "store-ch", req.Channel)
		assert.Equal(t, "store-meta", req.Metadata)
		assert.Equal(t, "es-id", req.ID)
		return &transport.SendEventStoreResult{Sent: true, ID: "es-id"}, nil
	})
	result, err := c.PublishEventStore(context.Background(), "store-ch", []byte("body"),
		WithMetadata("store-meta"),
		WithID("es-id"),
	)
	require.NoError(t, err)
	assert.True(t, result.Sent)
}

func TestSendQueueMessageSimple_WithOptions(t *testing.T) {
	c, mt := newConvenienceTestClient(t)
	mt.OnSendQueueMessage(func(_ context.Context, req *transport.QueueMessageItem) (*transport.QueueSendResultItem, error) {
		assert.Equal(t, "q-ch", req.Channel)
		assert.Equal(t, 30, req.Policy.ExpirationSeconds)
		assert.Equal(t, 5, req.Policy.DelaySeconds)
		assert.Equal(t, 3, req.Policy.MaxReceiveCount)
		assert.Equal(t, "dlq", req.Policy.MaxReceiveQueue)
		return &transport.QueueSendResultItem{MessageID: "qm-1", SentAt: 123}, nil
	})
	result, err := c.SendQueueMessageSimple(context.Background(), "q-ch", []byte("data"),
		WithExpiration(30),
		WithDelay(5),
		WithMaxReceive(3, "dlq"),
	)
	require.NoError(t, err)
	assert.Equal(t, "qm-1", result.MessageID)
}

func TestParseAddress_NegativePort(t *testing.T) {
	_, _, err := parseAddress("localhost:-1")
	assert.Error(t, err)
}

func TestParseAddress_IPv6(t *testing.T) {
	host, port, err := parseAddress("[::1]:50000")
	require.NoError(t, err)
	assert.Equal(t, "::1", host)
	assert.Equal(t, 50000, port)
}

func TestPublishOption_NilFn(t *testing.T) {
	opt := &publishOption{fn: nil, fnStore: nil}
	e := &Event{}
	es := &EventStore{}
	opt.applyPublish(e)
	opt.applyPublishStore(es)
}

func TestParseAddress_EmptyString(t *testing.T) {
	_, _, err := parseAddress("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "address is required")
}

func TestParseAddress_MissingPort(t *testing.T) {
	_, _, err := parseAddress("localhost")
	assert.Error(t, err)
}

func TestParseAddress_EmptyHost(t *testing.T) {
	_, _, err := parseAddress(":50000")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "host is required")
}

func TestParseAddress_PortTooHigh(t *testing.T) {
	_, _, err := parseAddress("localhost:70000")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "port must be between")
}

func TestParseAddress_PortZero(t *testing.T) {
	_, _, err := parseAddress("localhost:0")
	assert.Error(t, err)
}

func TestPublishEvent_ValidationError(t *testing.T) {
	c, _ := newConvenienceTestClient(t)
	err := c.PublishEvent(context.Background(), "", []byte("body"))
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestPublishEventStore_ValidationError(t *testing.T) {
	c, _ := newConvenienceTestClient(t)
	_, err := c.PublishEventStore(context.Background(), "", []byte("body"))
	require.Error(t, err)
}

func TestSendQueueMessageSimple_ValidationError(t *testing.T) {
	c, _ := newConvenienceTestClient(t)
	_, err := c.SendQueueMessageSimple(context.Background(), "", []byte("body"))
	require.Error(t, err)
}

func TestSendCommandSimple_ValidationError(t *testing.T) {
	c, _ := newConvenienceTestClient(t)
	_, err := c.SendCommandSimple(context.Background(), "", []byte("body"), 10*time.Second)
	require.Error(t, err)
}

func TestSendQuerySimple_ValidationError(t *testing.T) {
	c, _ := newConvenienceTestClient(t)
	_, err := c.SendQuerySimple(context.Background(), "", []byte("body"), 10*time.Second)
	require.Error(t, err)
}

func TestSendCommandSimple_TimeoutZero(t *testing.T) {
	c, _ := newConvenienceTestClient(t)
	_, err := c.SendCommandSimple(context.Background(), "cmd-ch", []byte("body"), 0)
	require.Error(t, err)
}

func TestSendQuerySimple_TimeoutZero(t *testing.T) {
	c, _ := newConvenienceTestClient(t)
	_, err := c.SendQuerySimple(context.Background(), "q-ch", []byte("body"), 0)
	require.Error(t, err)
}
