package kubemq

import (
	"context"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/kubemq-io/kubemq-go/v2/pkg/uuid"
	"go.opentelemetry.io/otel/trace"
)

// SubscribeToEvents subscribes to events on the given channel, returning a
// Subscription that delivers events via the WithOnEvent handler and supports Unsubscribe().
//
// Wildcard patterns are supported in the channel parameter:
//
//   - "orders.*"  matches "orders.created", "orders.updated", etc. (single-level)
//   - "orders.>"  matches "orders.created", "orders.us.created", etc. (multi-level)
//   - "*"         matches all single-segment channels
//
// The group parameter enables load-balanced consumption: when multiple
// subscribers share the same group on the same channel, each message is
// delivered to exactly one subscriber in the group.
func (c *Client) SubscribeToEvents(ctx context.Context, channel, group string, opts ...SubscribeOption) (*Subscription, error) {
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
	if cfg.onEvent == nil {
		return nil, &KubeMQError{
			Code:      ErrCodeValidation,
			Message:   "WithOnEvent handler is required for SubscribeToEvents",
			Operation: "SubscribeToEvents",
			Channel:   channel,
			Cause:     ErrValidation,
		}
	}

	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	handle, err := c.transport.SubscribeToEvents(subCtx, &transport.SubscribeRequest{
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
				if e, ok := msg.(*transport.EventReceiveItem); ok {
					cfg.onEvent(&Event{
						Id:       e.ID,
						Channel:  e.Channel,
						Metadata: e.Metadata,
						Body:     e.Body,
						Tags:     e.Tags,
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

// SendEvent sends a fire-and-forget event to the specified channel.
// Validates the event before sending.
func (c *Client) SendEvent(ctx context.Context, event *Event) error {
	if err := c.checkClosed(); err != nil {
		return err
	}
	if event.Channel == "" && c.opts.defaultChannel != "" {
		event.Channel = c.opts.defaultChannel
	}
	if event.ClientId == "" {
		event.ClientId = c.opts.clientId
	}
	if err := validateEvent(event, c.opts); err != nil {
		return err
	}
	ctx, finish := c.otel.StartSpan(ctx, middleware.SpanConfig{
		Operation: "publish",
		Channel:   event.Channel,
		SpanKind:  trace.SpanKindProducer,
		ClientID:  event.ClientId,
		MessageID: event.Id,
		BodySize:  len(event.Body),
	})
	req := &transport.SendEventRequest{
		ID:       event.Id,
		ClientID: event.ClientId,
		Channel:  event.Channel,
		Metadata: event.Metadata,
		Body:     event.Body,
		Tags:     event.Tags,
	}
	err := c.transport.SendEvent(ctx, req)
	finish(err)
	return err
}

// NewEvent creates a new Event pre-populated with client defaults.
func (c *Client) NewEvent() *Event {
	return &Event{
		ClientId: c.opts.clientId,
		Channel:  c.opts.defaultChannel,
		Tags:     map[string]string{},
	}
}
