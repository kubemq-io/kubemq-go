package kubemq

import (
	"fmt"
	"time"
)

// EventStore is an outbound event-store message. Unlike Event, event-store
// messages are persisted by the server and can be replayed by subscribers from
// any point in history. It is NOT safe for concurrent use — create a new
// EventStore for each send operation.
//
// Fields:
//   - Id: unique message identifier. Auto-generated UUID if empty at send time.
//   - Channel: target channel name (required unless WithDefaultChannel is set).
//     Wildcards are not supported for publishing.
//   - Metadata: arbitrary string metadata stored with the message.
//   - Body: binary payload. At least one of Body or Metadata must be non-empty.
//   - ClientId: sender identifier. Auto-populated from client defaults if empty.
//   - Tags: key-value string pairs stored alongside the message.
//
// See also: SendEventStore, SubscribeToEventsStore, EventStoreResult, Event.
type EventStore struct {
	Id       string
	Channel  string
	Metadata string
	Body     []byte
	ClientId string
	Tags     map[string]string
}

// NewEventStore creates an empty EventStore.
func NewEventStore() *EventStore {
	return &EventStore{}
}

// SetId sets the event store message ID.
func (es *EventStore) SetId(id string) *EventStore {
	es.Id = id
	return es
}

// SetClientId sets the client identifier for this event store message.
func (es *EventStore) SetClientId(clientId string) *EventStore {
	es.ClientId = clientId
	return es
}

// SetMetadata sets the event store metadata.
func (es *EventStore) SetMetadata(metadata string) *EventStore {
	es.Metadata = metadata
	return es
}

// SetChannel sets the target channel for this event store message.
func (es *EventStore) SetChannel(channel string) *EventStore {
	es.Channel = channel
	return es
}

// SetBody sets the event store body payload.
func (es *EventStore) SetBody(body []byte) *EventStore {
	es.Body = body
	return es
}

// SetTags replaces all tags on this event store message.
func (es *EventStore) SetTags(tags map[string]string) *EventStore {
	es.Tags = make(map[string]string, len(tags))
	for key, value := range tags {
		es.Tags[key] = value
	}
	return es
}

// AddTag adds a single key-value tag to this event store message.
func (es *EventStore) AddTag(key, value string) *EventStore {
	if es.Tags == nil {
		es.Tags = map[string]string{}
	}
	es.Tags[key] = value
	return es
}

// Validate checks all required fields and constraints.
// Called automatically before send operations; can also be called explicitly.
func (es *EventStore) Validate() error {
	return validateEventStore(es, nil)
}

// EventStoreResult contains the result of an event store send operation
// returned by SendEventStore. Immutable after construction. Safe to read from
// multiple goroutines.
//
// Fields:
//   - Id: the EventStore.Id of the sent message, echoed back for correlation.
//   - Sent: true if the server persisted the event successfully.
//   - Err: non-nil if the server rejected or failed to persist the event.
//
// See also: SendEventStore, EventStore.
type EventStoreResult struct {
	Id   string
	Sent bool
	Err  error
}

// EventStoreReceive is a received event store message delivered to subscription
// callbacks. It is safe to read from multiple goroutines but must not be
// modified after receipt.
//
// Fields:
//   - Id: the original EventStore.Id set by the publisher.
//   - Sequence: monotonically increasing sequence number assigned by the server.
//     Unique within the channel. Useful for resuming subscriptions via
//     StartFromSequence.
//   - Timestamp: server-assigned time when the event was persisted.
//   - Channel: the channel this event was published to.
//   - Metadata: string metadata from the publisher.
//   - Body: binary payload from the publisher.
//   - ClientId: the publisher's client identifier.
//   - Tags: key-value pairs from the publisher.
//
// See also: SubscribeToEventsStore, EventStore, StartFromSequence.
type EventStoreReceive struct {
	Id        string
	Sequence  uint64
	Timestamp time.Time
	Channel   string
	Metadata  string
	Body      []byte
	ClientId  string
	Tags      map[string]string
}

// String returns a human-readable representation of the received event store message.
func (es *EventStoreReceive) String() string {
	return fmt.Sprintf("Id: %s, Sequence: %d, Timestamp: %s, Channel: %s, Metadata: %s, Body: %s, ClientId: %s, Tags: %s",
		es.Id, es.Sequence, es.Timestamp.String(), es.Channel, es.Metadata, es.Body, es.ClientId, es.Tags)
}

// SubscriptionOption configures event store subscription start position.
type SubscriptionOption interface {
	apply(*subscriptionOption)
}

type subscriptionOption struct {
	kind  int32
	value int64
}

type funcSubscriptionOptions struct {
	fn func(*subscriptionOption)
}

func (fo *funcSubscriptionOptions) apply(o *subscriptionOption) {
	fo.fn(o)
}

func newFuncSubscriptionOption(f func(*subscriptionOption)) *funcSubscriptionOptions {
	return &funcSubscriptionOptions{fn: f}
}

const (
	subscribeStartNewOnly     int32 = 1
	subscribeStartFromFirst   int32 = 2
	subscribeStartFromLast    int32 = 3
	subscribeStartAtSequence  int32 = 4
	subscribeStartAtTime      int32 = 5
	subscribeStartAtTimeDelta int32 = 6
)

// StartFromNewEvents starts the subscription with only new events.
func StartFromNewEvents() SubscriptionOption {
	return newFuncSubscriptionOption(func(o *subscriptionOption) {
		o.kind = subscribeStartNewOnly
	})
}

// StartFromFirstEvent replays all stored events from the beginning.
func StartFromFirstEvent() SubscriptionOption {
	return newFuncSubscriptionOption(func(o *subscriptionOption) {
		o.kind = subscribeStartFromFirst
	})
}

// StartFromLastEvent replays the last event and continues with new ones.
func StartFromLastEvent() SubscriptionOption {
	return newFuncSubscriptionOption(func(o *subscriptionOption) {
		o.kind = subscribeStartFromLast
	})
}

// StartFromSequence replays events starting at a specific sequence number.
func StartFromSequence(sequence int) SubscriptionOption {
	return newFuncSubscriptionOption(func(o *subscriptionOption) {
		o.kind = subscribeStartAtSequence
		o.value = int64(sequence)
	})
}

// StartFromTime replays events from a specific point in time.
func StartFromTime(since time.Time) SubscriptionOption {
	return newFuncSubscriptionOption(func(o *subscriptionOption) {
		o.kind = subscribeStartAtTime
		o.value = since.UnixNano()
	})
}

// StartFromTimeDelta replays events from now minus the specified duration.
func StartFromTimeDelta(delta time.Duration) SubscriptionOption {
	return newFuncSubscriptionOption(func(o *subscriptionOption) {
		o.kind = subscribeStartAtTimeDelta
		o.value = int64(delta.Seconds())
	})
}

// subscriptionParamsFromOption extracts (kind, value) for transport SubscribeRequest.
func subscriptionParamsFromOption(opt SubscriptionOption) (kind int32, value int64) {
	o := &subscriptionOption{}
	opt.apply(o)
	return o.kind, o.value
}
