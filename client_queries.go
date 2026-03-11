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
// handler and supports Unsubscribe(). Use SendResponse to reply to received queries.
//
// The group parameter enables load-balanced consumption across query handlers.
func (c *Client) SubscribeToQueries(ctx context.Context, channel, group string, opts ...SubscribeOption) (*Subscription, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateChannel(channel); err != nil {
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

// SendQuery sends a query and waits for a response or timeout.
// Validates the query before sending.
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
