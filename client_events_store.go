package kubemq

import (
	"context"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/kubemq-io/kubemq-go/v2/pkg/uuid"
	"go.opentelemetry.io/otel/trace"
)

// SubscribeToEventsStore subscribes to persistent events on the given channel.
// Unlike SubscribeToEvents, event store subscriptions replay historical events
// from a configurable start position and guarantee at-least-once delivery.
//
// Parameters:
//   - ctx: parent context. Cancelling ctx tears down the subscription.
//   - channel: the exact channel name to subscribe to (required). Wildcard
//     patterns are NOT supported for event store subscriptions.
//   - group: enables load-balanced consumption. When multiple subscribers share
//     the same group on the same channel, each event is delivered to exactly one
//     subscriber. Pass "" for broadcast mode.
//   - startOpt: controls the replay start position (required). One of:
//   - StartFromNewEvents(): only receive new events published after subscribing.
//   - StartFromFirstEvent(): replay all stored events from the beginning.
//   - StartFromLastEvent(): replay the last stored event, then continue.
//   - StartFromSequence(n): replay from sequence number n onward (n must be > 0).
//   - StartFromTime(t): replay events stored at or after time t.
//   - StartFromTimeDelta(d): replay events from now minus duration d.
//   - opts: subscription options. WithOnEventStoreReceive is required — it sets
//     the callback for each received event. WithOnError is optional.
//
// Returns a *Subscription on success. The subscription automatically reconnects
// on transient errors when auto-reconnect is enabled.
//
// Possible errors:
//   - VALIDATION: channel is empty, startOpt is undefined, sequence/time value
//     is invalid, or WithOnEventStoreReceive handler is not provided
//   - TRANSIENT: temporary network issue establishing the subscription (retryable)
//   - AUTHENTICATION: invalid or missing auth token
//   - AUTHORIZATION: insufficient permissions for this channel
//   - CANCELLATION: ctx was cancelled before subscription was established
//
// See also: EventStoreReceive, SendEventStore, EventStore, Subscription,
// StartFromNewEvents, StartFromFirstEvent, StartFromSequence.
func (c *Client) SubscribeToEventsStore(ctx context.Context, channel, group string, startOpt SubscriptionOption, opts ...SubscribeOption) (*Subscription, error) {
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
	if subType == 0 {
		return nil, &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "events store subscription type must not be Undefined (use StartFromNewEvents, StartFromFirstEvent, etc.)",
			Cause:   ErrValidation,
		}
	}
	if subType == subscribeStartAtSequence && subValue <= 0 {
		return nil, &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "StartAtSequence value must be > 0",
			Cause:   ErrValidation,
		}
	}
	if subType == subscribeStartAtTime && subValue <= 0 {
		return nil, &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "StartAtTime value must be > 0",
			Cause:   ErrValidation,
		}
	}
	if subType == subscribeStartAtTimeDelta && subValue <= 0 {
		return nil, &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "StartAtTimeDelta value must be > 0",
			Cause:   ErrValidation,
		}
	}

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
						Id:        e.ID,
						Sequence:  e.Sequence,
						Timestamp: time.Unix(0, e.Timestamp),
						Channel:   e.Channel,
						Metadata:  e.Metadata,
						Body:      e.Body,
						Tags:      e.Tags,
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

// SendEventStore sends a persistent event to the event store channel. Unlike
// SendEvent, the server persists the message and subscribers can replay it from
// any point in history.
//
// Parameters:
//   - ctx: controls the deadline for the send operation. If the context expires
//     before the server confirms persistence, a TIMEOUT error is returned.
//   - event: the event store message to send. EventStore.Channel is required
//     unless WithDefaultChannel is set. EventStore.Id and EventStore.ClientId
//     are auto-populated if empty. At least one of Body or Metadata must be set.
//
// Returns an *EventStoreResult containing the confirmation. Check
// EventStoreResult.Sent to verify the event was persisted. If
// EventStoreResult.Err is non-nil, the server failed to persist the event.
//
// Possible errors:
//   - VALIDATION: channel is empty, body and metadata are both nil/empty
//   - TRANSIENT: temporary network issue (retryable)
//   - TIMEOUT: operation exceeded ctx deadline
//   - AUTHENTICATION: invalid or missing auth token
//   - AUTHORIZATION: insufficient permissions for this channel
//   - BACKPRESSURE: server or client buffer is full (retryable after backoff)
//
// See also: EventStore, EventStoreResult, SubscribeToEventsStore,
// SendEventStoreStream.
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
	if event.Id == "" {
		event.Id = uuid.New()
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

// EventStoreStreamHandle manages a bidirectional event store send stream.
// Each sent event receives a confirmation via Results.
type EventStoreStreamHandle struct {
	Send    func(event *EventStore) error
	Results <-chan *EventStreamResult
	Done    <-chan struct{}
	Close   func()
}

// SendEventStoreStream opens a bidirectional stream for high-throughput event store publishing.
// Each sent event receives a confirmation Result.
func (c *Client) SendEventStoreStream(ctx context.Context) (*EventStoreStreamHandle, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	handle, err := c.transport.SendEventsStream(ctx)
	if err != nil {
		return nil, err
	}
	pubResultCh := make(chan *EventStreamResult, 16)
	go func() {
		for r := range handle.Results {
			if r != nil {
				select {
				case pubResultCh <- &EventStreamResult{
					EventID: r.EventID,
					Sent:    r.Sent,
					Error:   r.Error,
				}:
				default:
				}
			}
		}
		close(pubResultCh)
	}()
	return &EventStoreStreamHandle{
		Send: func(event *EventStore) error {
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
				Store:    true,
			})
		},
		Results: pubResultCh,
		Done:    handle.Done,
		Close:   handle.Close,
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
