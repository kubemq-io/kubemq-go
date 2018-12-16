package kubemq

import (
	"context"
	"github.com/kubemq-io/kubemq-go/pb"
	"time"
)

type EventStore struct {
	Id        string
	Channel   string
	Metadata  string
	Body      []byte
	ClientId  string
	transport Transport
}

// SetId - set event store id otherwise new random uuid will be set
func (es *EventStore) SetId(id string) *EventStore {
	es.Id = id
	return es
}

// SetClientId - set event store ClientId - mandatory if default client was not set
func (es *EventStore) SetClientId(clientId string) *EventStore {
	es.ClientId = clientId
	return es
}

// SetMetadata - set event store metadata - mandatory if body field was not set
func (es *EventStore) SetMetadata(metadata string) *EventStore {
	es.Metadata = metadata
	return es
}

// SetChannel - set event store channel - mandatory if default channel was not set
func (es *EventStore) SetChannel(channel string) *EventStore {
	es.Channel = channel
	return es
}

// SetBody - set event store body - mandatory if metadata field was not set
func (es *EventStore) SetBody(body []byte) *EventStore {
	es.Body = body
	return es
}

func (es *EventStore) Send(ctx context.Context) (*EventStoreResult, error) {
	return es.transport.SendEventStore(ctx, es)
}

type EventStoreResult struct {
	Id   string
	Sent bool
	Err  error
}

type EventStoreReceive struct {
	Id        string
	Sequence  uint64
	Timestamp time.Time
	Channel   string
	Metadata  string
	Body      []byte
	ClientId  string
}

type SubscriptionOption interface {
	apply(*subscriptionOption)
}

type subscriptionOption struct {
	kind  pb.Subscribe_EventsStoreType
	value int64
}

type funcSubscriptionOptions struct {
	fn func(*subscriptionOption)
}

func (fo *funcSubscriptionOptions) apply(o *subscriptionOption) {
	fo.fn(o)
}

func newFuncSubscriptionOption(f func(*subscriptionOption)) *funcSubscriptionOptions {
	return &funcSubscriptionOptions{
		fn: f,
	}
}

// StartFromNewEvents - start event store subscription with only new events
func StartFromNewEvents() SubscriptionOption {
	return newFuncSubscriptionOption(func(o *subscriptionOption) {
		o.kind = pb.StartNewOnly
	})
}

// StartFromFirstEvent - replay all the stored events from the first available sequence and continue stream new events from this point
func StartFromFirstEvent() SubscriptionOption {
	return newFuncSubscriptionOption(func(o *subscriptionOption) {
		o.kind = pb.StartFromFirst
	})
}

// StartFromLastEvent - replay last event and continue stream new events from this point
func StartFromLastEvent() SubscriptionOption {
	return newFuncSubscriptionOption(func(o *subscriptionOption) {
		o.kind = pb.StartFromLast
	})
}

// StartFromSequence - replay events from specific event sequence number and continue stream new events from this point
func StartFromSequence(sequence int) SubscriptionOption {
	return newFuncSubscriptionOption(func(o *subscriptionOption) {
		o.kind = pb.StartAtSequence
		o.value = int64(sequence)
	})
}

// StartFromTime - replay events from specific time continue stream new events from this point
func StartFromTime(since time.Time) SubscriptionOption {
	return newFuncSubscriptionOption(func(o *subscriptionOption) {
		o.kind = pb.StartAtTime
		o.value = since.Unix()
	})
}

// StartFromTimeDelta - replay events from specific current time - delta duration in seconds, continue stream new events from this point
func StartFromTimeDelta(delta time.Duration) SubscriptionOption {
	return newFuncSubscriptionOption(func(o *subscriptionOption) {
		o.kind = pb.StartAtTimeDelta
		o.value = int64(delta.Seconds())
	})
}
