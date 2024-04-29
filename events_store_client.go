package kubemq

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/common"
)

// EventsStoreClient is a struct that holds a client instance.
type EventsStoreClient struct {
	client *Client
}

// EventsStoreSubscription is a struct that holds the subscription details.
type EventsStoreSubscription struct {
	Channel          string
	Group            string
	ClientId         string
	SubscriptionType SubscriptionOption
}

// Complete is a method that completes the subscription with the provided options.
func (es *EventsStoreSubscription) Complete(opts *Options) *EventsStoreSubscription {
	if es.ClientId == "" {
		es.ClientId = opts.clientId
	}
	return es
}

// Validate is a method that validates the subscription details.
func (es *EventsStoreSubscription) Validate() error {
	if es.Channel == "" {
		return fmt.Errorf("events store subscription must have a channel")
	}
	if es.ClientId == "" {
		return fmt.Errorf("events store subscription must have a clientId")
	}
	if es.SubscriptionType == nil {
		return fmt.Errorf("events store subscription must have a subscription type")
	}
	return nil
}

// NewEventsStoreClient is a function that creates a new EventsStoreClient.
func NewEventsStoreClient(ctx context.Context, op ...Option) (*EventsStoreClient, error) {
	client, err := NewClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	return &EventsStoreClient{
		client: client,
	}, nil
}

// Send is a method that sends an event to the store.
func (es *EventsStoreClient) Send(ctx context.Context, message *EventStore) (*EventStoreResult, error) {
	if err := es.isClientReady(); err != nil {
		return nil, err
	}
	message.transport = es.client.transport
	return es.client.SetEventStore(message).Send(ctx)
}

// Stream is a method that streams events from the store.
func (es *EventsStoreClient) Stream(ctx context.Context, onResult func(result *EventStoreResult, err error)) (func(msg *EventStore) error, error) {
	if err := es.isClientReady(); err != nil {
		return nil, err
	}
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
	eventsResultCh := make(chan *EventStoreResult, 1)
	go es.client.StreamEventsStore(ctx, eventsCh, eventsResultCh, errCh)
	go func() {
		for {
			select {
			case result, ok := <-eventsResultCh:
				if !ok {
					return
				}
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

// Subscribe is a method that subscribes to events from the store.
func (es *EventsStoreClient) Subscribe(ctx context.Context, request *EventsStoreSubscription, onEvent func(msg *EventStoreReceive, err error)) error {
	if err := es.isClientReady(); err != nil {
		return err
	}
	if onEvent == nil {
		return fmt.Errorf("events store subscription callback function is required")
	}
	if err := request.Complete(es.client.opts).Validate(); err != nil {
		return err
	}
	errCh := make(chan error, 1)
	eventsCh, err := es.client.SubscribeToEventsStoreWithRequest(ctx, request, errCh)
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

// Create is a method that creates a new channel in the events store.
func (es *EventsStoreClient) Create(ctx context.Context, channel string) error {
	return CreateChannel(ctx, es.client, es.client.opts.clientId, channel, "events_store")
}

// Delete is a method that deletes a channel from the events store.
func (es *EventsStoreClient) Delete(ctx context.Context, channel string) error {
	return DeleteChannel(ctx, es.client, es.client.opts.clientId, channel, "events_store")
}

// List is a method that lists all channels in the events store.
func (es *EventsStoreClient) List(ctx context.Context, search string) ([]*common.PubSubChannel, error) {
	return ListPubSubChannels(ctx, es.client, es.client.opts.clientId, "events_store", search)
}

// Close is a method that closes the client connection.
func (es *EventsStoreClient) Close() error {
	if err := es.isClientReady(); err != nil {
		return err
	}
	return es.client.Close()
}

// isClientReady is a method that checks if the client is ready.
func (es *EventsStoreClient) isClientReady() error {
	if es.client == nil {
		return fmt.Errorf("client is not initialized")
	}
	return nil
}
