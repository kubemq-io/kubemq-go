package kubemq

import "context"

type EventsStoreClient struct {
	client *Client
}

func NewEventsStoreClient(ctx context.Context, op ...Option) (*EventsStoreClient, error) {
	client, err := NewClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	return &EventsStoreClient{
		client: client,
	}, nil
}

func (es *EventsStoreClient) Send(ctx context.Context, message *EventStore) (*EventStoreResult, error) {
	return es.client.SetEventStore(message).Send(ctx)
}

func (es *EventsStoreClient) Stream(ctx context.Context, eventsCh chan *EventStore, eventsResultCh chan *EventStoreResult, errCh chan error) {
	es.client.StreamEventsStore(ctx, eventsCh, eventsResultCh, errCh)
}

func (es *EventsStoreClient) Subscribe(ctx context.Context, channel, group string, errCh chan error, opt SubscriptionOption) (<-chan *EventStoreReceive, error) {
	return es.client.SubscribeToEventsStore(ctx, channel, group, errCh, opt)
}
