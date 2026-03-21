package kubemq

import (
	"context"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/kubemq-io/kubemq-go/v2/pkg/uuid"
	"go.opentelemetry.io/otel/trace"
)

// SubscribeToQueries subscribes to queries on the given channel, returning a
// Subscription that delivers received queries via the WithOnQueryReceive
// handler and supports Unsubscribe(). Use SendQueryResponse to reply to received queries.
//
// Parameters:
//   - ctx: parent context. Cancelling ctx tears down the subscription.
//   - channel: the exact channel name to subscribe to (required, no wildcards).
//   - group: enables load-balanced consumption across query handlers. When
//     multiple subscribers share the same group on the same channel, each
//     query is delivered to exactly one subscriber. Pass "" to receive all
//     queries (broadcast).
//   - opts: subscription options. WithOnQueryReceive is required — it sets the
//     callback for each received query. The handler must call SendQueryResponse to
//     reply before Query.Timeout expires. WithOnError is optional.
//
// Returns a *Subscription on success. The subscription automatically reconnects
// on transient errors when auto-reconnect is enabled.
//
// Possible errors:
//   - VALIDATION: channel is empty, or WithOnQueryReceive handler is not provided
//   - TRANSIENT: temporary network issue establishing the subscription (retryable)
//   - AUTHENTICATION: invalid or missing auth token
//   - AUTHORIZATION: insufficient permissions for this channel
//   - CANCELLATION: ctx was cancelled before subscription was established
//
// See also: QueryReceive, SendQuery, SendQueryResponse, Subscription.
func (c *Client) SubscribeToQueries(ctx context.Context, channel, group string, opts ...SubscribeOption) (*Subscription, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateChannelStrict(channel); err != nil {
		return nil, err
	}
	cfg := &subscribeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.onError == nil {
		cfg.onError = c.defaultErrorHandler
	}
	if cfg.onQueryReceive == nil {
		return nil, &KubeMQError{
			Code:      ErrCodeValidation,
			Message:   "WithOnQueryReceive handler is required for SubscribeToQueries",
			Operation: "SubscribeToQueries",
			Channel:   channel,
			Cause:     ErrValidation,
		}
	}

	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	handle, err := c.transport.SubscribeToQueries(subCtx, &transport.SubscribeRequest{
		Channel:  channel,
		Group:    group,
		ClientID: c.opts.clientId,
	})
	if err != nil {
		cancel()
		return nil, err
	}

	go func() {
		defer close(done)
		for {
			select {
			case msg, ok := <-handle.Messages:
				if !ok {
					return
				}
				if q, ok := msg.(*transport.QueryReceiveItem); ok {
					cfg.onQueryReceive(&QueryReceive{
						Id:         q.ID,
						ClientId:   q.ClientID,
						Channel:    q.Channel,
						Metadata:   q.Metadata,
						Body:       q.Body,
						ResponseTo: q.ResponseTo,
						Tags:       q.Tags,
						Span:       q.Span,
					})
				}
			case err, ok := <-handle.Errors:
				if !ok {
					return
				}
				cfg.onError(err)
			case <-subCtx.Done():
				handle.Close()
				return
			}
		}
	}()

	return newSubscription(uuid.New(), cancel, done), nil
}

// SendQuery sends a query to a subscriber on the given channel and blocks
// until a response is received or the timeout expires. Queries implement a
// request-reply pattern with optional response caching: exactly one subscriber
// must handle the query and call SendQueryResponse to reply.
//
// Parameters:
//   - ctx: controls the overall deadline. If ctx expires before a response
//     arrives, a context.DeadlineExceeded error is returned.
//   - query: the query to send.
//   - Query.Channel: target channel (required unless WithDefaultChannel is set).
//   - Query.Timeout: server-side timeout for waiting for a subscriber response.
//     If zero, defaults to 5s. If no subscriber responds within this duration,
//     the server returns a TIMEOUT error.
//   - Query.CacheKey: optional. When set, the server caches the response under
//     this key. Subsequent queries with the same CacheKey return the cached
//     response without invoking the subscriber (until CacheTTL expires).
//   - Query.CacheTTL: time-to-live for cached responses. Only meaningful when
//     CacheKey is set.
//   - Query.Id: auto-generated UUID if empty.
//   - Query.ClientId: auto-populated from client defaults if empty.
//   - At least one of Body or Metadata must be set.
//
// Returns a *QueryResponse on success. Check QueryResponse.Executed to
// determine if the subscriber successfully processed the query.
// QueryResponse.CacheHit is true when the response was served from cache.
// QueryResponse.Body and QueryResponse.Metadata contain the subscriber's reply.
//
// Possible errors:
//   - VALIDATION: channel is empty, body and metadata are both nil/empty
//   - TRANSIENT: temporary network issue (retryable)
//   - TIMEOUT: no subscriber responded within Query.Timeout or ctx expired
//   - AUTHENTICATION: invalid or missing auth token
//   - AUTHORIZATION: insufficient permissions for this channel
//   - NOT_FOUND: no subscriber is listening on the channel
//   - CANCELLATION: ctx was cancelled before a response arrived
//
// See also: Query, QueryResponse, SubscribeToQueries, SendQueryResponse.
func (c *Client) SendQuery(ctx context.Context, query *Query) (*QueryResponse, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	if query.Channel == "" && c.opts.defaultChannel != "" {
		query.Channel = c.opts.defaultChannel
	}
	if query.ClientId == "" {
		query.ClientId = c.opts.clientId
	}
	if query.Timeout == 0 {
		query.Timeout = defaultRequestTimeout
	}
	if query.Id == "" {
		query.Id = uuid.New()
	}
	if err := validateQuery(query, c.opts); err != nil {
		return nil, err
	}
	ctx, finish := c.otel.StartSpan(ctx, middleware.SpanConfig{
		Operation: "send_query",
		Channel:   query.Channel,
		SpanKind:  trace.SpanKindClient,
		ClientID:  query.ClientId,
		MessageID: query.Id,
		BodySize:  len(query.Body),
	})
	req := &transport.SendQueryRequest{
		ID:       query.Id,
		ClientID: query.ClientId,
		Channel:  query.Channel,
		Metadata: query.Metadata,
		Body:     query.Body,
		Timeout:  query.Timeout,
		CacheKey: query.CacheKey,
		CacheTTL: query.CacheTTL,
		Tags:     query.Tags,
		Span:     query.Span,
	}
	result, err := c.transport.SendQuery(ctx, req)
	finish(err)
	if err != nil {
		return nil, err
	}
	return &QueryResponse{
		QueryId:          result.QueryID,
		Executed:         result.Executed,
		ExecutedAt:       result.ExecutedAt,
		Metadata:         result.Metadata,
		ResponseClientId: result.ResponseClientID,
		Body:             result.Body,
		CacheHit:         result.CacheHit,
		Error:            result.Error,
		Tags:             result.Tags,
	}, nil
}

// SendQueryResponse sends a response to a received query. This must be
// called by the subscriber within the sender's Timeout to deliver the reply.
//
// Parameters:
//   - ctx: controls the deadline for the send operation.
//   - response: the response to send. QueryReply.RequestId must match the
//     original QueryReceive.Id. QueryReply.ResponseTo must match the
//     QueryReceive.ResponseTo routing address.
//     QueryReply.ClientId is auto-populated from client defaults if empty.
//
// Possible errors:
//   - VALIDATION: RequestId or ResponseTo is empty
//   - TRANSIENT: temporary network issue (retryable)
//   - TIMEOUT: operation exceeded ctx deadline
//   - AUTHENTICATION: invalid or missing auth token
//
// See also: QueryReply, QueryReceive, SubscribeToQueries.
func (c *Client) SendQueryResponse(ctx context.Context, response *QueryReply) error {
	if err := c.checkClosed(); err != nil {
		return err
	}
	if response.ClientId == "" {
		response.ClientId = c.opts.clientId
	}
	if err := validateQueryReply(response); err != nil {
		return err
	}
	req := &transport.SendResponseRequest{
		RequestID:  response.RequestId,
		ResponseTo: response.ResponseTo,
		Metadata:   response.Metadata,
		Body:       response.Body,
		ClientID:   response.ClientId,
		ExecutedAt: response.ExecutedAt,
		Err:        response.Err,
		Tags:       response.Tags,
	}
	return c.transport.SendResponse(ctx, req)
}

// NewQueryReply creates a new QueryReply pre-populated with client defaults.
func (c *Client) NewQueryReply() *QueryReply {
	return &QueryReply{
		ClientId: c.opts.clientId,
		Tags:     map[string]string{},
	}
}

// NewQuery creates a new Query pre-populated with client defaults.
func (c *Client) NewQuery() *Query {
	return &Query{
		ClientId: c.opts.clientId,
		Channel:  c.opts.defaultChannel,
		Timeout:  defaultRequestTimeout,
		CacheTTL: c.opts.defaultCacheTTL,
		Tags:     map[string]string{},
	}
}
