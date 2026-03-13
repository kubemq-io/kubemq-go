package kubemq

import (
	"errors"
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
