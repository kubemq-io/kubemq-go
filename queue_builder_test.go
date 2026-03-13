package kubemq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueMessage_Setters(t *testing.T) {
	qm := NewQueueMessage().
		SetId("qm1").
		SetClientId("client1").
		SetChannel("queue-ch").
		SetMetadata("meta").
		SetBody([]byte("body")).
		SetTags(map[string]string{"env": "test"}).
		AddTag("v", "2")

	assert.Equal(t, "qm1", qm.ID)
	assert.Equal(t, "client1", qm.ClientID)
	assert.Equal(t, "queue-ch", qm.Channel)
	assert.Equal(t, "meta", qm.Metadata)
	assert.Equal(t, []byte("body"), qm.Body)
	assert.Equal(t, "test", qm.Tags["env"])
	assert.Equal(t, "2", qm.Tags["v"])
}

func TestQueueMessage_SetTags_DefensiveCopy(t *testing.T) {
	original := map[string]string{"a": "1"}
	qm := NewQueueMessage().SetTags(original)
	original["a"] = "modified"
	assert.Equal(t, "1", qm.Tags["a"])
}

func TestQueueMessage_AddTag_NilInit(t *testing.T) {
	qm := &QueueMessage{}
	qm.AddTag("key", "val")
	assert.Equal(t, "val", qm.Tags["key"])
}

func TestQueueMessage_SetPolicy(t *testing.T) {
	p := &QueuePolicy{ExpirationSeconds: 60, DelaySeconds: 5, MaxReceiveCount: 3, MaxReceiveQueue: "dlq"}
	qm := NewQueueMessage().SetPolicy(p)
	assert.Equal(t, 60, qm.Policy.ExpirationSeconds)
	assert.Equal(t, "dlq", qm.Policy.MaxReceiveQueue)
}

func TestQueueMessage_SetExpirationSeconds(t *testing.T) {
	qm := NewQueueMessage().SetExpirationSeconds(120)
	require.NotNil(t, qm.Policy)
	assert.Equal(t, 120, qm.Policy.ExpirationSeconds)
}

func TestQueueMessage_SetDelaySeconds(t *testing.T) {
	qm := NewQueueMessage().SetDelaySeconds(30)
	require.NotNil(t, qm.Policy)
	assert.Equal(t, 30, qm.Policy.DelaySeconds)
}

func TestQueueMessage_SetMaxReceiveCount(t *testing.T) {
	qm := NewQueueMessage().SetMaxReceiveCount(5)
	require.NotNil(t, qm.Policy)
	assert.Equal(t, 5, qm.Policy.MaxReceiveCount)
}

func TestQueueMessage_SetMaxReceiveQueue(t *testing.T) {
	qm := NewQueueMessage().SetMaxReceiveQueue("dead-letter")
	require.NotNil(t, qm.Policy)
	assert.Equal(t, "dead-letter", qm.Policy.MaxReceiveQueue)
}

func TestQueueMessage_Validate_Valid(t *testing.T) {
	qm := NewQueueMessage().SetChannel("queue-ch").SetBody([]byte("data"))
	assert.NoError(t, qm.Validate())
}

func TestQueueMessage_Validate_MissingChannel(t *testing.T) {
	qm := NewQueueMessage()
	err := qm.Validate()
	require.Error(t, err)
}

func TestQueueMessage_String(t *testing.T) {
	qm := NewQueueMessage().SetId("qm1").SetChannel("ch")
	s := qm.String()
	assert.Contains(t, s, "qm1")
	assert.Contains(t, s, "ch")
}

func TestQueueMessages_Add(t *testing.T) {
	qms := &QueueMessages{Messages: []*QueueMessage{}}
	qms.Add(NewQueueMessage().SetId("1")).Add(NewQueueMessage().SetId("2"))
	assert.Len(t, qms.Messages, 2)
	assert.Equal(t, "1", qms.Messages[0].ID)
	assert.Equal(t, "2", qms.Messages[1].ID)
}
