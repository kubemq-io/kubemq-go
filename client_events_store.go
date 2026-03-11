package kubemq

import (
	"context"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/kubemq-io/kubemq-go/v2/pkg/uuid"
	"go.opentelemetry.io/otel/trace"
)

// SubscribeToEventsStore subscribes to persistent events on the given channel.
//
// Wildcard patterns are supported in the channel parameter (same syntax as
// SubscribeToEvents — see that method's documentation for details).
//
// The startOpt parameter controls replay position: StartFromNewEvents(),
// StartFromFirstEvent(), StartFromLastEvent(), StartFromSequence(n),
// StartFromTime(t), or StartFromTimeDelta(d).
func (c *Client) SubscribeToEventsStore(ctx context.Context, channel, group string, startOpt SubscriptionOption, opts ...SubscribeOption) (*Subscription, error) {
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
	if cfg.onEventStoreReceive == nil {
		return nil, &KubeMQError{
			Code:      ErrCodeValidation,
			Message:   "WithOnEventStoreReceive handler is required for SubscribeToEventsStore",
			Operation: "SubscribeToEventsStore",
			Channel:   channel,
			Cause:     ErrValidation,
		}
	}

	subType, subValue := subscriptionParamsFromOption(startOpt)

	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	handle, err := c.transport.SubscribeToEventsStore(subCtx, &transport.SubscribeRequest{
		Channel:           channel,
		Group:             group,
		ClientID:          c.opts.clientId,
		SubscriptionType:  subType,
		SubscriptionValue: subValue,
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
				if e, ok := msg.(*transport.EventStoreReceiveItem); ok {
					cfg.onEventStoreReceive(&EventStoreReceive{
						Id:       e.ID,
						Sequence: e.Sequence,
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

// SendEventStore sends an event to the event store channel.
// Validates the event store message before sending.
func (c *Client) SendEventStore(ctx context.Context, event *EventStore) (*EventStoreResult, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	if event.Channel == "" && c.opts.defaultChannel != "" {
		event.Channel = c.opts.defaultChannel
	}
	if event.ClientId == "" {
		event.ClientId = c.opts.clientId
	}
	if err := validateEventStore(event, c.opts); err != nil {
		return nil, err
	}
	ctx, finish := c.otel.StartSpan(ctx, middleware.SpanConfig{
		Operation: "publish",
		Channel:   event.Channel,
		SpanKind:  trace.SpanKindProducer,
		ClientID:  event.ClientId,
		MessageID: event.Id,
		BodySize:  len(event.Body),
	})
	req := &transport.SendEventStoreRequest{
		ID:       event.Id,
		ClientID: event.ClientId,
		Channel:  event.Channel,
		Metadata: event.Metadata,
		Body:     event.Body,
		Tags:     event.Tags,
	}
	result, err := c.transport.SendEventStore(ctx, req)
	finish(err)
	if err != nil {
		return nil, err
	}
	return &EventStoreResult{
		Id:   result.ID,
		Sent: result.Sent,
		Err:  result.Err,
	}, nil
}

// NewEventStore creates a new EventStore pre-populated with client defaults.
func (c *Client) NewEventStore() *EventStore {
	return &EventStore{
		ClientId: c.opts.clientId,
		Channel:  c.opts.defaultChannel,
		Tags:     map[string]string{},
	}
}
