package kubemq

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCommandReply_Builder(t *testing.T) {
	r := NewCommandReply()
	require.NotNil(t, r)
	assert.Empty(t, r.RequestId)
}

func TestCommandReply_Setters(t *testing.T) {
	now := time.Now()
	testErr := errors.New("test error")
	r := NewCommandReply().
		SetRequestId("req1").
		SetResponseTo("resp-ch").
		SetMetadata("meta").
		SetBody([]byte("body")).
		SetTags(map[string]string{"k": "v"}).
		SetClientId("client1").
		SetError(testErr).
		SetExecutedAt(now)

	assert.Equal(t, "req1", r.RequestId)
	assert.Equal(t, "resp-ch", r.ResponseTo)
	assert.Equal(t, "meta", r.Metadata)
	assert.Equal(t, []byte("body"), r.Body)
	assert.Equal(t, "v", r.Tags["k"])
	assert.Equal(t, "client1", r.ClientId)
	assert.Equal(t, testErr, r.Err)
	assert.Equal(t, now, r.ExecutedAt)
}

func TestCommandReply_SetTags_DefensiveCopy(t *testing.T) {
	original := map[string]string{"a": "1"}
	r := NewCommandReply().SetTags(original)
	original["a"] = "modified"
	assert.Equal(t, "1", r.Tags["a"])
}

func TestCommandReply_SetTags_Nil(t *testing.T) {
	r := NewCommandReply().SetTags(nil)
	assert.Nil(t, r.Tags)
}

func TestCommandReply_Validate_Valid(t *testing.T) {
	r := NewCommandReply().SetRequestId("req1").SetResponseTo("resp-ch")
	assert.NoError(t, r.Validate())
}

func TestCommandReply_Validate_MissingRequestId(t *testing.T) {
	r := NewCommandReply().SetResponseTo("resp-ch")
	err := r.Validate()
	require.Error(t, err)
}

func TestCommandReply_Validate_MissingResponseTo(t *testing.T) {
	r := NewCommandReply().SetRequestId("req1")
	err := r.Validate()
	require.Error(t, err)
}

func TestCommandReply_String(t *testing.T) {
	r := NewCommandReply().SetRequestId("req1").SetResponseTo("ch")
	s := r.String()
	assert.Contains(t, s, "req1")
	assert.Contains(t, s, "ch")
}

func TestCommandReply_String_WithError(t *testing.T) {
	r := NewCommandReply().SetRequestId("req1").SetResponseTo("ch").SetError(errors.New("fail"))
	s := r.String()
	assert.Contains(t, s, "fail")
}

func TestNewQueryReply_Builder(t *testing.T) {
	r := NewQueryReply()
	require.NotNil(t, r)
	assert.Empty(t, r.RequestId)
	assert.False(t, r.CacheHit)
}

func TestQueryReply_Setters(t *testing.T) {
	now := time.Now()
	testErr := errors.New("test error")
	r := NewQueryReply().
		SetRequestId("req1").
		SetResponseTo("resp-ch").
		SetMetadata("meta").
		SetBody([]byte("body")).
		SetTags(map[string]string{"k": "v"}).
		SetClientId("client1").
		SetError(testErr).
		SetExecutedAt(now)

	assert.Equal(t, "req1", r.RequestId)
	assert.Equal(t, "resp-ch", r.ResponseTo)
	assert.Equal(t, "meta", r.Metadata)
	assert.Equal(t, []byte("body"), r.Body)
	assert.Equal(t, "v", r.Tags["k"])
	assert.Equal(t, "client1", r.ClientId)
	assert.Equal(t, testErr, r.Err)
	assert.Equal(t, now, r.ExecutedAt)
}

func TestQueryReply_Validate_Valid(t *testing.T) {
	r := NewQueryReply().SetRequestId("req1").SetResponseTo("resp-ch")
	assert.NoError(t, r.Validate())
}

func TestQueryReply_Validate_MissingRequestId(t *testing.T) {
	r := NewQueryReply().SetResponseTo("resp-ch")
	err := r.Validate()
	require.Error(t, err)
}

func TestQueryReply_String(t *testing.T) {
	r := NewQueryReply().SetRequestId("req1").SetResponseTo("ch")
	s := r.String()
	assert.Contains(t, s, "req1")
	assert.Contains(t, s, "CacheHit")
}
