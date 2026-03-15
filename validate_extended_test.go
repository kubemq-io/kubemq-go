package kubemq

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateChannel_InvalidUTF8(t *testing.T) {
	err := validateChannel("\xff\xfe")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "invalid UTF-8")
}

func TestValidateClientID_InvalidUTF8(t *testing.T) {
	err := validateClientID("\xff\xfe")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "invalid UTF-8")
}

func TestValidateEvent_BodyTooLarge_CustomMaxSize(t *testing.T) {
	opts := GetDefaultOptions()
	opts.maxSendMsgSize = 10
	e := &Event{Channel: "orders", Body: make([]byte, 20)}
	err := validateEvent(e, opts)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "exceeds maximum")
}

func TestValidateEventStore_BodyTooLarge_CustomMaxSize(t *testing.T) {
	opts := GetDefaultOptions()
	opts.maxSendMsgSize = 10
	es := &EventStore{Channel: "orders", Body: make([]byte, 20)}
	err := validateEventStore(es, opts)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "exceeds maximum")
}

func TestValidateCommand_BodyTooLarge_CustomMaxSize(t *testing.T) {
	opts := GetDefaultOptions()
	opts.maxSendMsgSize = 10
	cmd := &Command{Channel: "orders", Body: make([]byte, 20), Timeout: 5 * time.Second}
	err := validateCommand(cmd, opts)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "exceeds maximum")
}

func TestValidateQuery_BodyTooLarge_CustomMaxSize(t *testing.T) {
	opts := GetDefaultOptions()
	opts.maxSendMsgSize = 10
	q := &Query{Channel: "orders", Body: make([]byte, 20), Timeout: 5 * time.Second}
	err := validateQuery(q, opts)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "exceeds maximum")
}

func TestValidateQueueMessage_BodyTooLarge_CustomMaxSize(t *testing.T) {
	opts := GetDefaultOptions()
	opts.maxSendMsgSize = 10
	msg := &QueueMessage{Channel: "orders", Body: make([]byte, 20)}
	err := validateQueueMessage(msg, opts)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "exceeds maximum")
}

func TestValidateEvent_TagEmptyKey(t *testing.T) {
	e := &Event{
		Channel: "orders",
		Body:    []byte("hi"),
		Tags:    map[string]string{"": "value"},
	}
	err := validateEvent(e, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "tag key must be non-empty")
}

func TestValidateCommand_DefaultChannelFromOpts(t *testing.T) {
	opts := GetDefaultOptions()
	opts.defaultChannel = "fallback"
	cmd := &Command{Body: []byte("hi"), Timeout: 5 * time.Second}
	err := validateCommand(cmd, opts)
	assert.NoError(t, err)
}

func TestValidateQuery_DefaultChannelFromOpts(t *testing.T) {
	opts := GetDefaultOptions()
	opts.defaultChannel = "fallback"
	q := &Query{Body: []byte("hi"), Timeout: 5 * time.Second}
	err := validateQuery(q, opts)
	assert.NoError(t, err)
}

func TestValidateQueueMessage_DefaultChannelFromOpts(t *testing.T) {
	opts := GetDefaultOptions()
	opts.defaultChannel = "fallback"
	msg := &QueueMessage{Body: []byte("hi")}
	err := validateQueueMessage(msg, opts)
	assert.NoError(t, err)
}

func TestValidateContent_EmptyBothMetadataAndBody(t *testing.T) {
	err := validateContent("", nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "at least one of metadata or body")
}

func TestValidateContent_EmptyBothMetadataAndEmptyBody(t *testing.T) {
	err := validateContent("", []byte{})
	require.Error(t, err)
}

func TestValidateContent_HasMetadataOnly(t *testing.T) {
	err := validateContent("some-meta", nil)
	assert.NoError(t, err)
}

func TestValidateContent_HasBodyOnly(t *testing.T) {
	err := validateContent("", []byte("data"))
	assert.NoError(t, err)
}

func TestValidateQueueReceive_MaxMsgZero(t *testing.T) {
	err := validateQueueReceive(0, 10)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "MaxNumberOfMessages")
}

func TestValidateQueueReceive_MaxMsgTooHigh(t *testing.T) {
	err := validateQueueReceive(1025, 10)
	require.Error(t, err)
}

func TestValidateQueueReceive_NegativeWait(t *testing.T) {
	err := validateQueueReceive(10, -1)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "WaitTimeSeconds")
}

func TestValidateQueueReceive_WaitTooHigh(t *testing.T) {
	err := validateQueueReceive(10, 3601)
	require.Error(t, err)
}

func TestValidateQueueReceive_ValidBoundary(t *testing.T) {
	assert.NoError(t, validateQueueReceive(1, 0))
	assert.NoError(t, validateQueueReceive(1024, 3600))
}

func TestValidateChannelStrict_Wildcard(t *testing.T) {
	err := validateChannelStrict("orders.*")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "wildcard")
}

func TestValidateChannelStrict_MultiLevelWildcard(t *testing.T) {
	err := validateChannelStrict("orders.>")
	require.Error(t, err)
	var kErr2 *KubeMQError
	require.True(t, errors.As(err, &kErr2))
	assert.Contains(t, kErr2.Message, "wildcard")
}

func TestValidateEventStore_EmptyChannelNoDefault(t *testing.T) {
	es := &EventStore{Body: []byte("data")}
	err := validateEventStore(es, nil)
	require.Error(t, err)
}

func TestValidateEventStore_EmptyContent(t *testing.T) {
	es := &EventStore{Channel: "ch"}
	err := validateEventStore(es, nil)
	require.Error(t, err)
}

func TestValidateCommand_TimeoutZero(t *testing.T) {
	cmd := &Command{Channel: "cmd-ch", Body: []byte("data"), Timeout: 0}
	err := validateCommand(cmd, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "timeout must be positive")
}

func TestValidateCommand_NegativeTimeout(t *testing.T) {
	cmd := &Command{Channel: "cmd-ch", Body: []byte("data"), Timeout: -1}
	err := validateCommand(cmd, nil)
	require.Error(t, err)
}

func TestValidateCommand_EmptyChannelNoDefault(t *testing.T) {
	cmd := &Command{Body: []byte("data"), Timeout: time.Second}
	err := validateCommand(cmd, nil)
	require.Error(t, err)
}

func TestValidateCommand_EmptyContent(t *testing.T) {
	cmd := &Command{Channel: "cmd-ch", Timeout: time.Second}
	err := validateCommand(cmd, nil)
	require.Error(t, err)
}

func TestValidateQuery_TimeoutZero(t *testing.T) {
	q := &Query{Channel: "q-ch", Body: []byte("data"), Timeout: 0}
	err := validateQuery(q, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "timeout must be positive")
}

func TestValidateQuery_CacheKeyWithZeroTTL(t *testing.T) {
	q := &Query{Channel: "q-ch", Body: []byte("data"), Timeout: time.Second, CacheKey: "key", CacheTTL: 0}
	err := validateQuery(q, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "CacheTTL must be > 0")
}

func TestValidateQuery_EmptyContent(t *testing.T) {
	q := &Query{Channel: "q-ch", Timeout: time.Second}
	err := validateQuery(q, nil)
	require.Error(t, err)
}

func TestValidateQueueMessage_NegativeExpirationExtended(t *testing.T) {
	msg := &QueueMessage{Channel: "q-ch", Body: []byte("data"), Policy: &QueuePolicy{ExpirationSeconds: -1}}
	err := validateQueueMessage(msg, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "expiration seconds")
}

func TestValidateQueueMessage_NegativeDelayExtended(t *testing.T) {
	msg := &QueueMessage{Channel: "q-ch", Body: []byte("data"), Policy: &QueuePolicy{DelaySeconds: -1}}
	err := validateQueueMessage(msg, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "delay seconds")
}

func TestValidateQueueMessage_NegativeMaxReceiveExtended(t *testing.T) {
	msg := &QueueMessage{Channel: "q-ch", Body: []byte("data"), Policy: &QueuePolicy{MaxReceiveCount: -1}}
	err := validateQueueMessage(msg, nil)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "max receive count")
}

func TestValidateEvent_EmptyChannelNoDefault(t *testing.T) {
	e := &Event{Body: []byte("data")}
	err := validateEvent(e, nil)
	require.Error(t, err)
}

func TestValidateEvent_EmptyContent(t *testing.T) {
	e := &Event{Channel: "ch"}
	err := validateEvent(e, nil)
	require.Error(t, err)
}

func TestValidateEvent_CustomMaxSize(t *testing.T) {
	opts := GetDefaultOptions()
	opts.maxSendMsgSize = 10
	e := &Event{Channel: "ch", Body: make([]byte, 11)}
	err := validateEvent(e, opts)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "exceeds maximum")
}

func TestValidateChannel_TooLong(t *testing.T) {
	long := strings.Repeat("a", 257)
	err := validateChannel(long)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "exceeds maximum length")
}

func TestValidateChannel_InvalidChars(t *testing.T) {
	err := validateChannel("ch with spaces")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "invalid characters")
}

func TestValidateChannel_TrailingDot(t *testing.T) {
	err := validateChannel("orders.")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Contains(t, kErr.Message, "cannot end with '.'")
}
