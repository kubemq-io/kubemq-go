package kubemq

import "context"

type EventsClient struct {
	client *Client
}

func NewEventsClient(ctx context.Context, op ...Option) (*EventsClient, error) {
	client, err := NewClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	return &EventsClient{
		client: client,
	}, nil
}

func (e *EventsClient) Send(ctx context.Context, message *Event) error {
	return e.client.SetEvent(message).Send(ctx)
}

func (e *EventsClient) Stream(ctx context.Context, eventsCh chan *Event, errCh chan error) {
	e.client.StreamEvents(ctx, eventsCh, errCh)
}

func (e *EventsClient) Subscribe(ctx context.Context, channel, group string, errCh chan error) (<-chan *Event, error) {
	return e.client.SubscribeToEvents(ctx, channel, group, errCh)
}
