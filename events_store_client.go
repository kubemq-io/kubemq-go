package kubemq

import (
	"context"
	"fmt"
)

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

func (es *EventsStoreClient) Stream(ctx context.Context, onResult func(result *EventStoreResult, err error)) (func(msg *EventStore) error, error) {
	if onResult == nil {
		return nil, fmt.Errorf("events stream result callback function is required")
	}
	errCh := make(chan error, 1)
	eventsCh := make(chan *EventStore, 1)

	sendFunc := func(msg *EventStore) error {
		select {
		case eventsCh <- msg:
			return nil

		case <-ctx.Done():
			return fmt.Errorf("context canceled during events message sending")
		}
	}
	go func() {
		eventsResultCh := make(chan *EventStoreResult, 1)
		es.client.StreamEventsStore(ctx, eventsCh, eventsResultCh, errCh)
		for {
			select {
			case result := <-eventsResultCh:
				onResult(result, nil)
			case err := <-errCh:
				onResult(nil, err)
			case <-ctx.Done():
				return
			}
		}
	}()
	return sendFunc, nil
}

func (es *EventsStoreClient) Subscribe(ctx context.Context, channel, group string, onEvent func(msg *EventStoreReceive, err error), opt SubscriptionOption) error {
	if onEvent == nil {
		return fmt.Errorf("events store subscription callback function is required")
	}
	errCh := make(chan error, 1)
	eventsCh, err := es.client.SubscribeToEventsStore(ctx, channel, group, errCh, opt)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case msg := <-eventsCh:
				onEvent(msg, nil)
			case err := <-errCh:
				onEvent(nil, err)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (es *EventsStoreClient) Close() error {
	return es.client.Close()
}
