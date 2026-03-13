package kubemq

import (
	"fmt"
	"time"
)

// Query is an outbound query request. It is NOT safe for concurrent use —
// create a new Query for each send operation.
type Query struct {
	Id       string
	Channel  string
	Metadata string
	Body     []byte
	Timeout  time.Duration
	ClientId string
	CacheKey string
	CacheTTL time.Duration
	Tags     map[string]string
	Span     []byte
}

// NewQuery creates an empty Query.
func NewQuery() *Query {
	return &Query{}
}

// SetId sets the query request ID.
func (q *Query) SetId(id string) *Query {
	q.Id = id
	return q
}

// SetClientId sets the client identifier for this query.
func (q *Query) SetClientId(clientId string) *Query {
	q.ClientId = clientId
	return q
}

// SetChannel sets the target channel for this query.
func (q *Query) SetChannel(channel string) *Query {
	q.Channel = channel
	return q
}

// SetMetadata sets the query metadata.
func (q *Query) SetMetadata(metadata string) *Query {
	q.Metadata = metadata
	return q
}

// SetBody sets the query body payload.
func (q *Query) SetBody(body []byte) *Query {
	q.Body = body
	return q
}

// SetTags replaces all tags on this query.
func (q *Query) SetTags(tags map[string]string) *Query {
	q.Tags = make(map[string]string, len(tags))
	for key, value := range tags {
		q.Tags[key] = value
	}
	return q
}

// AddTag adds a single key-value tag to this query.
func (q *Query) AddTag(key, value string) *Query {
	if q.Tags == nil {
		q.Tags = map[string]string{}
	}
	q.Tags[key] = value
	return q
}

// SetTimeout sets the timeout for waiting for a query response.
func (q *Query) SetTimeout(timeout time.Duration) *Query {
	q.Timeout = timeout
	return q
}

// SetCacheKey sets the cache key for this query response.
func (q *Query) SetCacheKey(cacheKey string) *Query {
	q.CacheKey = cacheKey
	return q
}

// SetCacheTTL sets the cache time-to-live for this query response.
func (q *Query) SetCacheTTL(ttl time.Duration) *Query {
	q.CacheTTL = ttl
	return q
}

// Validate checks all required fields and constraints.
// Called automatically before send operations; can also be called explicitly.
func (q *Query) Validate() error {
	return validateQuery(q, nil)
}

// QueryReceive is a received query request delivered to subscription
// callbacks. It is safe to read from multiple goroutines but must not be
// modified after receipt.
type QueryReceive struct {
	Id         string
	Channel    string
	ClientId   string
	Metadata   string
	Body       []byte
	ResponseTo string
	Tags       map[string]string
	Span       []byte
}

// String returns a human-readable representation of the received query.
func (qr *QueryReceive) String() string {
	return fmt.Sprintf("Id: %s, ClientId: %s, Channel: %s, Metadata: %s, Body: %s, ResponseTo: %s, Tags: %v",
		qr.Id, qr.ClientId, qr.Channel, qr.Metadata, string(qr.Body), qr.ResponseTo, qr.Tags)
}

// QueryResponse contains the result of a query execution.
// Immutable after construction. Safe to read from multiple goroutines.
type QueryResponse struct {
	QueryId          string
	Executed         bool
	ExecutedAt       time.Time
	Metadata         string
	ResponseClientId string
	Body             []byte
	CacheHit         bool
	Error            string
	Tags             map[string]string
}

// String returns a human-readable representation of the query response.
func (qr *QueryResponse) String() string {
	return fmt.Sprintf("QueryId: %s, Executed: %v, ExecutedAt: %s, Metadata: %s, ResponseClientId: %s, Body: %s, CacheHit: %v, Error: %s, Tags: %v",
		qr.QueryId, qr.Executed, qr.ExecutedAt.String(), qr.Metadata, qr.ResponseClientId, string(qr.Body), qr.CacheHit, qr.Error, qr.Tags)
}
