package kubemq

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewResponse_Builder(t *testing.T) {
	r := NewResponse()
	require.NotNil(t, r)
	assert.Empty(t, r.RequestId)
}

func TestResponse_Setters(t *testing.T) {
	now := time.Now()
	testErr := errors.New("test error")
	r := NewResponse().
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

func TestResponse_SetTags_DefensiveCopy(t *testing.T) {
	original := map[string]string{"a": "1"}
	r := NewResponse().SetTags(original)
	original["a"] = "modified"
	assert.Equal(t, "1", r.Tags["a"])
}

func TestResponse_SetTags_Nil(t *testing.T) {
	r := NewResponse().SetTags(nil)
	assert.Nil(t, r.Tags)
}

func TestResponse_Validate_Valid(t *testing.T) {
	r := NewResponse().SetRequestId("req1").SetResponseTo("resp-ch")
	assert.NoError(t, r.Validate())
}

func TestResponse_Validate_MissingRequestId(t *testing.T) {
	r := NewResponse().SetResponseTo("resp-ch")
	err := r.Validate()
	require.Error(t, err)
}

func TestResponse_Validate_MissingResponseTo(t *testing.T) {
	r := NewResponse().SetRequestId("req1")
	err := r.Validate()
	require.Error(t, err)
}

func TestResponse_String(t *testing.T) {
	r := NewResponse().SetRequestId("req1").SetResponseTo("ch")
	s := r.String()
	assert.Contains(t, s, "req1")
	assert.Contains(t, s, "ch")
}

func TestResponse_String_WithError(t *testing.T) {
	r := NewResponse().SetRequestId("req1").SetResponseTo("ch").SetError(errors.New("fail"))
	s := r.String()
	assert.Contains(t, s, "fail")
}
