package kubemq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewQuery(t *testing.T) {
	q := NewQuery()
	require.NotNil(t, q)
	assert.Empty(t, q.Id)
}

func TestQuery_Setters(t *testing.T) {
	q := NewQuery().
		SetId("q1").
		SetClientId("client1").
		SetChannel("orders").
		SetMetadata("meta").
		SetBody([]byte("data")).
		SetTimeout(5*time.Second).
		SetCacheKey("cache-key").
		SetCacheTTL(30*time.Second).
		SetTags(map[string]string{"k": "v"}).
		AddTag("k2", "v2")

	assert.Equal(t, "q1", q.Id)
	assert.Equal(t, "client1", q.ClientId)
	assert.Equal(t, "orders", q.Channel)
	assert.Equal(t, "meta", q.Metadata)
	assert.Equal(t, []byte("data"), q.Body)
	assert.Equal(t, 5*time.Second, q.Timeout)
	assert.Equal(t, "cache-key", q.CacheKey)
	assert.Equal(t, 30*time.Second, q.CacheTTL)
	assert.Equal(t, "v", q.Tags["k"])
	assert.Equal(t, "v2", q.Tags["k2"])
}

func TestQuery_SetTags_DefensiveCopy(t *testing.T) {
	original := map[string]string{"a": "1"}
	q := NewQuery().SetTags(original)
	original["a"] = "modified"
	assert.Equal(t, "1", q.Tags["a"])
}

func TestQuery_AddTag_NilInit(t *testing.T) {
	q := &Query{}
	q.AddTag("key", "val")
	assert.Equal(t, "val", q.Tags["key"])
}

func TestQuery_Validate_Valid(t *testing.T) {
	q := NewQuery().SetChannel("orders").SetTimeout(5 * time.Second).SetBody([]byte("data"))
	assert.NoError(t, q.Validate())
}

func TestQuery_Validate_MissingChannel(t *testing.T) {
	q := NewQuery().SetTimeout(5 * time.Second)
	err := q.Validate()
	require.Error(t, err)
}

func TestQuery_Validate_MissingTimeout(t *testing.T) {
	q := NewQuery().SetChannel("orders")
	err := q.Validate()
	require.Error(t, err)
}

func TestQueryReceive_String(t *testing.T) {
	qr := &QueryReceive{Id: "q1", Channel: "ch", ResponseTo: "resp"}
	s := qr.String()
	assert.Contains(t, s, "q1")
	assert.Contains(t, s, "ch")
}

func TestQueryResponse_String(t *testing.T) {
	qr := &QueryResponse{QueryId: "q1", Executed: true, CacheHit: true}
	s := qr.String()
	assert.Contains(t, s, "q1")
	assert.Contains(t, s, "true")
}
