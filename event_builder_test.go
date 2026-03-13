package kubemq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEvent_Builder(t *testing.T) {
	e := NewEvent()
	require.NotNil(t, e)
	assert.Empty(t, e.Id)
}

func TestEvent_Setters(t *testing.T) {
	e := NewEvent().
		SetId("e1").
		SetClientId("client1").
		SetChannel("events-ch").
		SetMetadata("meta").
		SetBody([]byte("payload")).
		SetTags(map[string]string{"env": "prod"}).
		AddTag("version", "2")

	assert.Equal(t, "e1", e.Id)
	assert.Equal(t, "client1", e.ClientId)
	assert.Equal(t, "events-ch", e.Channel)
	assert.Equal(t, "meta", e.Metadata)
	assert.Equal(t, []byte("payload"), e.Body)
	assert.Equal(t, "prod", e.Tags["env"])
	assert.Equal(t, "2", e.Tags["version"])
}

func TestEvent_SetTags_DefensiveCopy(t *testing.T) {
	original := map[string]string{"a": "1"}
	e := NewEvent().SetTags(original)
	original["a"] = "modified"
	assert.Equal(t, "1", e.Tags["a"])
}

func TestEvent_AddTag_NilInit(t *testing.T) {
	e := &Event{}
	e.AddTag("key", "val")
	assert.Equal(t, "val", e.Tags["key"])
}

func TestEvent_Validate_Valid(t *testing.T) {
	e := NewEvent().SetChannel("orders").SetBody([]byte("data"))
	assert.NoError(t, e.Validate())
}

func TestEvent_Validate_MissingChannel(t *testing.T) {
	e := NewEvent()
	err := e.Validate()
	require.Error(t, err)
	var kErr *KubeMQError
	require.ErrorAs(t, err, &kErr)
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestEvent_String(t *testing.T) {
	e := NewEvent().SetId("e1").SetChannel("ch")
	s := e.String()
	assert.Contains(t, s, "e1")
	assert.Contains(t, s, "ch")
}
