package kubemq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEventStore_Builder(t *testing.T) {
	es := NewEventStore()
	require.NotNil(t, es)
	assert.Empty(t, es.Id)
}

func TestEventStore_Setters(t *testing.T) {
	es := NewEventStore().
		SetId("es1").
		SetClientId("client1").
		SetChannel("store-ch").
		SetMetadata("meta").
		SetBody([]byte("data")).
		SetTags(map[string]string{"env": "test"}).
		AddTag("version", "1")

	assert.Equal(t, "es1", es.Id)
	assert.Equal(t, "client1", es.ClientId)
	assert.Equal(t, "store-ch", es.Channel)
	assert.Equal(t, "meta", es.Metadata)
	assert.Equal(t, []byte("data"), es.Body)
	assert.Equal(t, "test", es.Tags["env"])
	assert.Equal(t, "1", es.Tags["version"])
}

func TestEventStore_SetTags_DefensiveCopy(t *testing.T) {
	original := map[string]string{"a": "1"}
	es := NewEventStore().SetTags(original)
	original["a"] = "modified"
	assert.Equal(t, "1", es.Tags["a"])
}

func TestEventStore_AddTag_NilInit(t *testing.T) {
	es := &EventStore{}
	es.AddTag("key", "val")
	assert.Equal(t, "val", es.Tags["key"])
}

func TestEventStore_Validate_Valid(t *testing.T) {
	es := NewEventStore().SetChannel("orders").SetBody([]byte("data"))
	assert.NoError(t, es.Validate())
}

func TestEventStore_Validate_MissingChannel(t *testing.T) {
	es := NewEventStore()
	err := es.Validate()
	require.Error(t, err)
}

func TestEventStoreReceive_String(t *testing.T) {
	esr := &EventStoreReceive{Id: "esr1", Channel: "ch", Sequence: 42}
	s := esr.String()
	assert.Contains(t, s, "esr1")
	assert.Contains(t, s, "42")
}

func TestStartFromTime(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	opt := StartFromTime(ts)
	kind, val := subscriptionParamsFromOption(opt)
	assert.Equal(t, subscribeStartAtTime, kind)
	assert.Equal(t, ts.UnixNano(), val)
}

func TestStartFromTimeDelta(t *testing.T) {
	opt := StartFromTimeDelta(60 * time.Second)
	kind, val := subscriptionParamsFromOption(opt)
	assert.Equal(t, subscribeStartAtTimeDelta, kind)
	assert.Equal(t, int64(60), val)
}
