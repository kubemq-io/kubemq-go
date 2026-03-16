package kubemq

import (
	"context"
	"fmt"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/kubemq-io/kubemq-go/v2/pkg/uuid"
	"go.opentelemetry.io/otel/trace"
)

// SubscribeToEvents subscribes to events on the given channel, returning a
// Subscription that delivers events via the WithOnEvent handler and supports Unsubscribe().
//
// Parameters:
//   - ctx: parent context. Cancelling ctx tears down the subscription (equivalent
//     to calling Subscription.Cancel). The subscription goroutine will drain and
//     the Subscription.Done channel will close.
//   - channel: the channel name to subscribe to (must not be empty).
//     Wildcard patterns are supported:
//   - "orders.*"  matches "orders.created", "orders.updated", etc. (single-level)
//   - "orders.>"  matches "orders.created", "orders.us.created", etc. (multi-level)
//   - "*"         matches all single-segment channels
//   - group: enables load-balanced consumption. When multiple subscribers share
//     the same group on the same channel, each event is delivered to exactly one
//     subscriber in the group (competing consumers). Pass "" for broadcast mode
//     where every subscriber receives every event.
//   - opts: subscription options. WithOnEvent is required — it sets the callback
//     invoked for each received event. The callback is called sequentially on a
//     dedicated goroutine; blocking the callback delays subsequent events.
//     WithOnError is optional — if not set, errors are logged at ERROR level.
//
// Returns a *Subscription on success. Call Subscription.Unsubscribe() or
// Subscription.Cancel() to stop receiving events. The subscription automatically
// reconnects on transient errors when the client has auto-reconnect enabled.
//
// Possible errors:
//   - VALIDATION: channel is empty, or WithOnEvent handler is not provided
//   - TRANSIENT: temporary network issue establishing the subscription (retryable)
//   - AUTHENTICATION: invalid or missing auth token
//   - AUTHORIZATION: insufficient permissions for this channel
//   - CANCELLATION: ctx was cancelled before subscription was established
//
// See also: Event, SendEvent, Subscription, WithOnEvent, WithOnError.
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
// The event is delivered to all subscribers on the channel (or to one subscriber
// per group for grouped subscriptions). There is no delivery confirmation — if
// no subscriber is connected, the event is silently dropped.
//
// Parameters:
//   - ctx: controls the deadline for the send operation. If the context expires
//     before the event is sent, a TIMEOUT error is returned.
//   - event: the event to send. Event.Channel is required unless the client was
//     created with WithDefaultChannel. Event.Id and Event.ClientId are
//     auto-populated if empty. At least one of Body or Metadata must be set.
//
// The event is validated before sending. If validation fails, the event is not
// sent and a VALIDATION error is returned immediately (no network call).
//
// Possible errors:
//   - VALIDATION: channel is empty, body and metadata are both nil/empty
//   - TRANSIENT: temporary network issue (retryable)
//   - TIMEOUT: operation exceeded ctx deadline
//   - AUTHENTICATION: invalid or missing auth token
//   - AUTHORIZATION: insufficient permissions for this channel
//   - BACKPRESSURE: server or client buffer is full (retryable after backoff)
//
// See also: Event, SubscribeToEvents, SendEventStream.
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
	if event.Id == "" {
		event.Id = uuid.New()
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

// EventStreamHandle manages a bidirectional event send stream.
// Events sent via Send are fire-and-forget; only errors are reported via Errors.
type EventStreamHandle struct {
	Send   func(event *Event) error
	Errors <-chan error
	Done   <-chan struct{}
	Close  func()
}

// SendEventStream opens a bidirectional stream for high-throughput event publishing.
// Events are fire-and-forget; results are only returned for errors.
func (c *Client) SendEventStream(ctx context.Context) (*EventStreamHandle, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	handle, err := c.transport.SendEventsStream(ctx)
	if err != nil {
		return nil, err
	}
	errCh := make(chan error, 16)
	go func() {
		for r := range handle.Results {
			if r != nil && !r.Sent && r.Error != "" {
				select {
				case errCh <- fmt.Errorf("event %s: %s", r.EventID, r.Error):
				default:
				}
			}
		}
		close(errCh)
	}()
	return &EventStreamHandle{
		Send: func(event *Event) error {
			if event.Id == "" {
				event.Id = uuid.New()
			}
			if event.ClientId == "" {
				event.ClientId = c.opts.clientId
			}
			return handle.Send(&transport.EventStreamItem{
				ID:       event.Id,
				ClientID: event.ClientId,
				Channel:  event.Channel,
				Metadata: event.Metadata,
				Body:     event.Body,
				Tags:     event.Tags,
				Store:    false,
			})
		},
		Errors: errCh,
		Done:   handle.Done,
		Close:  handle.Close,
	}, nil
}

// NewEvent creates a new Event pre-populated with client defaults.
func (c *Client) NewEvent() *Event {
	return &Event{
		ClientId: c.opts.clientId,
		Channel:  c.opts.defaultChannel,
		Tags:     map[string]string{},
	}
}
