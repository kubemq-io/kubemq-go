package kubemq

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrNoTransport(t *testing.T) {
	err := errNoTransport("SendEvent")
	require.Error(t, err)
	var kErr *KubeMQError
	require.ErrorAs(t, err, &kErr)
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Equal(t, "SendEvent", kErr.Operation)
}

func TestBufferFullError_Format(t *testing.T) {
	err := &BufferFullError{BufferSize: 100, QueuedCount: 100}
	assert.Contains(t, err.Error(), "buffer full")
	assert.Contains(t, err.Error(), "100")
}

func TestStreamBrokenError_Details(t *testing.T) {
	err := &StreamBrokenError{UnacknowledgedIDs: []string{"a", "b"}}
	assert.Contains(t, err.Error(), "stream broken")
	assert.Contains(t, err.Error(), "2 unacknowledged")
}

func TestTransportError(t *testing.T) {
	cause := fmt.Errorf("connection refused")
	err := &TransportError{Op: "dial", Cause: cause}
	assert.Contains(t, err.Error(), "dial")
	assert.Contains(t, err.Error(), "connection refused")
	assert.Equal(t, cause, err.Unwrap())
}

func TestHandlerError(t *testing.T) {
	cause := fmt.Errorf("panic in handler")
	err := &HandlerError{Handler: "onEvent", Cause: cause}
	assert.Contains(t, err.Error(), "onEvent")
	assert.Contains(t, err.Error(), "panic in handler")
	assert.Equal(t, cause, err.Unwrap())
}

func TestStreamReconnector(t *testing.T) {
	sr := newStreamReconnector(RetryPolicy{}, nil)
	require.NotNil(t, sr)

	sr.trackMessage("msg-1")
	sr.trackMessage("msg-2")

	ids := sr.unacknowledgedIDs()
	assert.Len(t, ids, 2)

	sr.ackMessage("msg-1")
	ids = sr.unacknowledgedIDs()
	assert.Len(t, ids, 1)

	var brokenErr error
	sr.onStreamBroken(func(err error) {
		brokenErr = err
	})
	require.Error(t, brokenErr)
	assert.Contains(t, brokenErr.Error(), "1 unacknowledged")
}

func TestStreamReconnector_OnStreamBroken_NoMessages(t *testing.T) {
	sr := newStreamReconnector(RetryPolicy{}, nil)

	called := false
	sr.onStreamBroken(func(err error) {
		called = true
	})
	assert.False(t, called)
}

func TestSubscribeOptions(t *testing.T) {
	cfg := &subscribeConfig{}

	WithOnError(func(error) {})(cfg)
	assert.NotNil(t, cfg.onError)

	WithOnEvent(func(*Event) {})(cfg)
	assert.NotNil(t, cfg.onEvent)

	WithOnEventStoreReceive(func(*EventStoreReceive) {})(cfg)
	assert.NotNil(t, cfg.onEventStoreReceive)

	WithOnCommandReceive(func(*CommandReceive) {})(cfg)
	assert.NotNil(t, cfg.onCommandReceive)

	WithOnQueryReceive(func(*QueryReceive) {})(cfg)
	assert.NotNil(t, cfg.onQueryReceive)
}

func TestSubscriptionParamsFromOption(t *testing.T) {
	kind, value := subscriptionParamsFromOption(StartFromNewEvents())
	assert.Equal(t, subscribeStartNewOnly, kind)
	assert.Equal(t, int64(0), value)

	kind, _ = subscriptionParamsFromOption(StartFromFirstEvent())
	assert.Equal(t, subscribeStartFromFirst, kind)

	kind, _ = subscriptionParamsFromOption(StartFromLastEvent())
	assert.Equal(t, subscribeStartFromLast, kind)

	kind, value = subscriptionParamsFromOption(StartFromSequence(100))
	assert.Equal(t, subscribeStartAtSequence, kind)
	assert.Equal(t, int64(100), value)
}

func TestDefaultTimeoutConstants(t *testing.T) {
	assert.NotZero(t, DefaultSendTimeout)
	assert.NotZero(t, DefaultSubscribeTimeout)
	assert.NotZero(t, DefaultRPCTimeout)
	assert.NotZero(t, DefaultQueueRecvTimeout)
	assert.NotZero(t, DefaultQueuePollTimeout)
}
