package kubemq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseAddress_Valid(t *testing.T) {
	host, port, err := parseAddress("localhost:50000")
	require.NoError(t, err)
	assert.Equal(t, "localhost", host)
	assert.Equal(t, 50000, port)
}

func TestParseAddress_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{"empty", ""},
		{"no port", "localhost"},
		{"invalid port", "localhost:abc"},
		{"port zero", "localhost:0"},
		{"port too high", "localhost:99999"},
		{"no host", ":50000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parseAddress(tt.address)
			assert.Error(t, err)
		})
	}
}

func TestParseAddress_IPHost(t *testing.T) {
	host, port, err := parseAddress("192.168.1.1:8080")
	require.NoError(t, err)
	assert.Equal(t, "192.168.1.1", host)
	assert.Equal(t, 8080, port)
}

func TestDefaultOptions_UniqueClientId(t *testing.T) {
	o1 := GetDefaultOptions()
	o2 := GetDefaultOptions()
	assert.Empty(t, o1.clientId)
	assert.Empty(t, o2.clientId)
}

func TestPublishOption_WithMetadata(t *testing.T) {
	event := &Event{Channel: "ch", Body: []byte("body")}
	opt := WithMetadata("meta")
	opt.applyPublish(event)
	assert.Equal(t, "meta", event.Metadata)
}

func TestPublishOption_WithTags(t *testing.T) {
	event := &Event{Channel: "ch", Body: []byte("body")}
	opt := WithTags(map[string]string{"k": "v"})
	opt.applyPublish(event)
	assert.Equal(t, "v", event.Tags["k"])
}

func TestPublishOption_WithID(t *testing.T) {
	event := &Event{Channel: "ch", Body: []byte("body")}
	opt := WithID("my-id")
	opt.applyPublish(event)
	assert.Equal(t, "my-id", event.Id)
}

func TestPublishOption_ApplyToEventStore(t *testing.T) {
	es := &EventStore{Channel: "ch", Body: []byte("body")}
	opt := WithMetadata("store-meta")
	opt.applyPublishStore(es)
	assert.Equal(t, "store-meta", es.Metadata)
}

func TestQueueSendOption_WithExpiration(t *testing.T) {
	msg := &QueueMessage{Channel: "ch", Body: []byte("body")}
	opt := WithExpiration(3600)
	opt.applyQueueSend(msg)
	require.NotNil(t, msg.Policy)
	assert.Equal(t, 3600, msg.Policy.ExpirationSeconds)
}

func TestQueueSendOption_WithDelay(t *testing.T) {
	msg := &QueueMessage{Channel: "ch", Body: []byte("body")}
	opt := WithDelay(10)
	opt.applyQueueSend(msg)
	require.NotNil(t, msg.Policy)
	assert.Equal(t, 10, msg.Policy.DelaySeconds)
}

func TestQueueSendOption_WithMaxReceive(t *testing.T) {
	msg := &QueueMessage{Channel: "ch", Body: []byte("body")}
	opt := WithMaxReceive(3, "orders.dlq")
	opt.applyQueueSend(msg)
	require.NotNil(t, msg.Policy)
	assert.Equal(t, 3, msg.Policy.MaxReceiveCount)
	assert.Equal(t, "orders.dlq", msg.Policy.MaxReceiveQueue)
}
