package kubemq

import (
	"context"
	"time"
)

type Query struct {
	Id        string
	Channel   string
	Metadata  string
	Body      []byte
	Timeout   time.Duration
	ClientId  string
	CacheKey  string
	CacheTTL  time.Duration
	Tags      map[string]string
	transport Transport
	trace     *Trace
}

func NewQuery() *Query {
	return &Query{}
}

// SetId - set query requestId, otherwise new random uuid will be set
func (q *Query) SetId(id string) *Query {
	q.Id = id
	return q
}

// SetClientId - set query ClientId - mandatory if default client was not set
func (q *Query) SetClientId(clientId string) *Query {
	q.ClientId = clientId
	return q
}

// SetChannel - set query channel - mandatory if default channel was not set
func (q *Query) SetChannel(channel string) *Query {
	q.Channel = channel
	return q
}

// SetMetadata - set query metadata - mandatory if body field is empty
func (q *Query) SetMetadata(metadata string) *Query {
	q.Metadata = metadata
	return q
}

// SetBody - set query body - mandatory if metadata field is empty
func (q *Query) SetBody(body []byte) *Query {
	q.Body = body
	return q
}

// SetTags - set key value tags to query message
func (q *Query) SetTags(tags map[string]string) *Query {
	q.Tags = map[string]string{}
	for key, value := range tags {
		q.Tags[key] = value
	}
	return q
}

// AddTag - add key value tags to query message
func (q *Query) AddTag(key, value string) *Query {
	if q.Tags == nil {
		q.Tags = map[string]string{}
	}
	q.Tags[key] = value
	return q
}

// SetTimeout - set timeout for query to be returned. if timeout expired , send query will result with an error
func (q *Query) SetTimeout(timeout time.Duration) *Query {
	q.Timeout = timeout
	return q
}

// SetCacheKey - set cache key to retrieve already stored query response, otherwise the response for this query will be stored in cache for future query requests
func (q *Query) SetCacheKey(cacheKey string) *Query {
	q.CacheKey = cacheKey
	return q
}

// SetCacheTTL - set cache time to live for the this query cache key response to be retrieved already stored query response, if not set default cacheTTL will be set
func (q *Query) SetCacheTTL(ttl time.Duration) *Query {
	q.CacheTTL = ttl
	return q
}

// Send - sending query request , waiting for response or timeout
func (q *Query) Send(ctx context.Context) (*QueryResponse, error) {
	if q.transport == nil {
		return nil, ErrNoTransportDefined
	}
	return q.transport.SendQuery(ctx, q)
}

// AddTrace - add tracing support to query
func (q *Query) AddTrace(name string) *Trace {
	q.trace = CreateTrace(name)
	return q.trace
}

type QueryReceive struct {
	Id         string
	Channel    string
	ClientId   string
	Metadata   string
	Body       []byte
	ResponseTo string
	Tags       map[string]string
}

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
