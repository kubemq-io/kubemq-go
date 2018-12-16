package kubemq

import (
	"context"
)

type Transport interface {
	SendEvent(ctx context.Context, event *Event) error
	StreamEvents(ctx context.Context, eventsCh chan *Event, errCh chan error)
	SubscribeToEvents(ctx context.Context, channel, group string, errCh chan error) (<-chan *Event, error)
	SendEventStore(ctx context.Context, eventStore *EventStore) (*EventStoreResult, error)
	StreamEventsStore(ctx context.Context, eventsCh chan *EventStore, eventsResultCh chan *EventStoreResult, errCh chan error)
	SubscribeToEventsStore(ctx context.Context, channel, group string, errCh chan error, opt SubscriptionOption) (<-chan *EventStoreReceive, error)
	SendCommand(ctx context.Context, command *Command) (*CommandResponse, error)
	SubscribeToCommands(ctx context.Context, channel, group string, errCh chan error) (<-chan *CommandReceive, error)
	SendQuery(ctx context.Context, query *Query) (*QueryResponse, error)
	SubscribeToQueries(ctx context.Context, channel, group string, errCh chan error) (<-chan *QueryReceive, error)
	SendResponse(ctx context.Context, response *Response) error
	Close() error
}
