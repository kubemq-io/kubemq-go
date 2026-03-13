package kubemq

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateChannel_Empty(t *testing.T) {
	err := validateChannel("")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.ErrorIs(t, err, ErrValidation)
}

func TestValidateChannel_Valid(t *testing.T) {
	valids := []string{
		"orders",
		"orders.events",
		"orders-v2",
		"orders_events",
		"orders/events",
		"orders.>",
		"orders.*",
		"A.B.C",
	}
	for _, ch := range valids {
		t.Run(ch, func(t *testing.T) {
			assert.NoError(t, validateChannel(ch))
		})
	}
}

func TestValidateChannel_Invalid(t *testing.T) {
	invalids := []struct {
		name    string
		channel string
	}{
		{"spaces", "orders events"},
		{"special chars", "orders@events"},
		{"tab", "orders\tevents"},
		{"newline", "orders\nevents"},
		{"too long", strings.Repeat("a", 257)},
	}
	for _, tt := range invalids {
		t.Run(tt.name, func(t *testing.T) {
			assert.Error(t, validateChannel(tt.channel))
		})
	}
}

func TestValidateClientID_Empty(t *testing.T) {
	err := validateClientID("")
	assert.Error(t, err)
}

func TestValidateClientID_Valid(t *testing.T) {
	assert.NoError(t, validateClientID("order-service-1"))
	assert.NoError(t, validateClientID("550e8400-e29b-41d4-a716-446655440000"))
}

func TestValidateMessageBody_OK(t *testing.T) {
	body := []byte("hello")
	assert.NoError(t, validateMessageBody(body, maxBodySize))
}

func TestValidateMessageBody_TooLarge(t *testing.T) {
	body := make([]byte, maxBodySize+1)
	err := validateMessageBody(body, maxBodySize)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "exceeds maximum")
}

func TestValidateMessageBody_DefaultMaxSize(t *testing.T) {
	body := make([]byte, 100)
	assert.NoError(t, validateMessageBody(body, 0))
}

func TestValidateTags_Valid(t *testing.T) {
	tags := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	assert.NoError(t, validateTags(tags))
}

func TestValidateTags_EmptyKey(t *testing.T) {
	tags := map[string]string{
		"": "value",
	}
	err := validateTags(tags)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "tag key must be non-empty")
}

func TestValidateTags_KeyTooLong(t *testing.T) {
	tags := map[string]string{
		strings.Repeat("k", maxTagKeyLength+1): "value",
	}
	err := validateTags(tags)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "exceeds maximum length")
}

func TestValidateTags_ValueTooLong(t *testing.T) {
	tags := map[string]string{
		"key": strings.Repeat("v", maxTagValueLength+1),
	}
	err := validateTags(tags)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "exceeds maximum length")
}

func TestValidateTags_Nil(t *testing.T) {
	assert.NoError(t, validateTags(nil))
}

func TestValidateEvent_Valid(t *testing.T) {
	e := &Event{Channel: "orders", Body: []byte("hi")}
	assert.NoError(t, validateEvent(e, nil))
}

func TestValidateEvent_EmptyChannel(t *testing.T) {
	e := &Event{Channel: "", Body: []byte("hi")}
	err := validateEvent(e, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestValidateEvent_DefaultChannelFallback(t *testing.T) {
	e := &Event{Body: []byte("hi")}
	opts := GetDefaultOptions()
	opts.defaultChannel = "fallback"
	assert.NoError(t, validateEvent(e, opts))
}

func TestValidateEventStore_Valid(t *testing.T) {
	es := &EventStore{Channel: "orders", Body: []byte("hi")}
	assert.NoError(t, validateEventStore(es, nil))
}

func TestValidateEventStore_EmptyChannel(t *testing.T) {
	es := &EventStore{Channel: "", Body: []byte("hi")}
	err := validateEventStore(es, nil)
	require.Error(t, err)
}

func TestValidateCommand_Valid(t *testing.T) {
	cmd := &Command{Channel: "orders", Body: []byte("hi"), Timeout: 5 * time.Second}
	assert.NoError(t, validateCommand(cmd, nil))
}

func TestValidateCommand_NoTimeout(t *testing.T) {
	cmd := &Command{Channel: "orders", Body: []byte("hi")}
	err := validateCommand(cmd, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "command timeout must be positive")
}

func TestValidateCommand_NoChannel(t *testing.T) {
	cmd := &Command{Body: []byte("hi"), Timeout: 5 * time.Second}
	err := validateCommand(cmd, nil)
	require.Error(t, err)
}

func TestValidateQuery_Valid(t *testing.T) {
	q := &Query{Channel: "orders", Body: []byte("hi"), Timeout: 5 * time.Second}
	assert.NoError(t, validateQuery(q, nil))
}

func TestValidateQuery_NoTimeout(t *testing.T) {
	q := &Query{Channel: "orders", Body: []byte("hi")}
	err := validateQuery(q, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "query timeout must be positive")
}

func TestValidateQueueMessage_Valid(t *testing.T) {
	msg := &QueueMessage{Channel: "orders", Body: []byte("hi")}
	assert.NoError(t, validateQueueMessage(msg, nil))
}

func TestValidateQueueMessage_NoChannel(t *testing.T) {
	msg := &QueueMessage{Body: []byte("hi")}
	err := validateQueueMessage(msg, nil)
	require.Error(t, err)
}

func TestValidateQueueMessage_NegativeExpiration(t *testing.T) {
	msg := &QueueMessage{
		Channel: "orders",
		Body:    []byte("hi"),
		Policy:  &QueuePolicy{ExpirationSeconds: -1},
	}
	err := validateQueueMessage(msg, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "expiration seconds must be non-negative")
}

func TestValidateQueueMessage_NegativeDelay(t *testing.T) {
	msg := &QueueMessage{
		Channel: "orders",
		Body:    []byte("hi"),
		Policy:  &QueuePolicy{DelaySeconds: -1},
	}
	err := validateQueueMessage(msg, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "delay seconds must be non-negative")
}

func TestValidateQueueMessage_NegativeMaxReceive(t *testing.T) {
	msg := &QueueMessage{
		Channel: "orders",
		Body:    []byte("hi"),
		Policy:  &QueuePolicy{MaxReceiveCount: -1},
	}
	err := validateQueueMessage(msg, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "max receive count must be non-negative")
}

func TestValidateResponse_Valid(t *testing.T) {
	r := &Response{RequestId: "req-1", ResponseTo: "ch"}
	assert.NoError(t, validateResponse(r))
}

func TestValidateResponse_NoRequestId(t *testing.T) {
	r := &Response{ResponseTo: "ch"}
	err := validateResponse(r)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "requestId is required")
}

func TestValidateResponse_NoResponseTo(t *testing.T) {
	r := &Response{RequestId: "req-1"}
	err := validateResponse(r)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "responseTo channel is required")
}

func TestValidation_BeforeNetwork(t *testing.T) {
	err := validateEvent(&Event{Channel: "", Body: []byte("x")}, nil)
	require.Error(t, err)
}
