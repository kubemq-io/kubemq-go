package kubemq

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/common"
)

// EventsMessageHandler is a function type that takes in a pointer to an Event object and does not return anything.
type EventsMessageHandler func(*Event)

// EventsErrorsHandler is a type representing a function that handles errors for events.
type EventsErrorsHandler func(error)

// EventsClient is a client for interacting with events.
// It encapsulates a client for making API requests.
type EventsClient struct {
	client *Client
}

// EventsSubscription represents a subscription to events by channel and group.
type EventsSubscription struct {
	Channel  string
	Group    string
	ClientId string
}

// Complete sets the ClientId of the EventsSubscription if it is empty.
// It takes an *Options argument to retrieve the clientId value. If the ClientId
// is already set in the EventsSubscription, it will not be overridden.
// It returns a pointer to the modified EventsSubscription.
func (es *EventsSubscription) Complete(opts *Options) *EventsSubscription {
	if es.ClientId == "" {
		es.ClientId = opts.clientId
	}
	return es
}

// Validate checks if the EventsSubscription has a non-empty Channel and ClientId.
// If either of them is empty, it returns an error.
// Otherwise, it returns nil.
func (es *EventsSubscription) Validate() error {
	if es.Channel == "" {
		return fmt.Errorf("events subscription must have a channel")
	}
	if es.ClientId == "" {
		return fmt.Errorf("events subscription must have a clientId")
	}
	return nil
}

// NewEventsClient creates an instance of EventsClient by calling NewClient and returning EventsClient{client}
//
// Parameters:
// - ctx: The context.Context to be used in NewClient call.
// - op: Optional parameters of type Option to be passed to NewClient.
//
// Returns a pointer to EventsClient and an error if NewClient call fails.
func NewEventsClient(ctx context.Context, op ...Option) (*EventsClient, error) {
	client, err := NewClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	return &EventsClient{
		client: client,
	}, nil
}

// Check if the client is ready
func (e *EventsClient) Send(ctx context.Context, message *Event) error {
	if err := e.isClientReady(); err != nil {
		return err
	}
	message.transport = e.client.transport
	return e.client.SetEvent(message).Send(ctx)
}

// Stream sends events from client to server and receives events from server to client.
// It takes a context as input, which can be used to cancel the streaming process.
// It also takes an onError function callback, which will be called when an error occurs during the streaming process.
// The method returns a sendFunc function, which can be used to send events to the server,
// and an error, which will be non-nil if the client is not ready or if the onError callback is not provided.
// The sendFunc function takes an event message as input and returns an error.
// It sends the event to the server through a channel, and if the context is cancelled before the event is sent,
// it returns an error indicating that the context was cancelled during event message sending.
// The method starts two goroutines, one for sending events to the server and one for receiving events from the server.
// The sending goroutine sends events to the server by accepting events from the eventsCh channel.
// The receiving goroutine receives errors from the errCh channel and calls the onError callback for each error received.
// It also checks if the context is cancelled and stops the receiving goroutine if it is.
// The method returns the sendFunc function and a nil error.
func (e *EventsClient) Stream(ctx context.Context, onError func(err error)) (func(msg *Event) error, error) {
	if err := e.isClientReady(); err != nil {
		return nil, err
	}
	if onError == nil {
		return nil, fmt.Errorf("events stream error callback function is required")
	}
	errCh := make(chan error, 1)
	eventsCh := make(chan *Event, 1)
	sendFunc := func(msg *Event) error {
		select {
		case eventsCh <- msg:
			return nil

		case <-ctx.Done():
			return fmt.Errorf("context canceled during events message sending")
		}
	}
	go e.client.StreamEvents(ctx, eventsCh, errCh)
	go func() {
		for {
			select {
			case err := <-errCh:
				onError(err)
			case <-ctx.Done():
				return
			}
		}
	}()
	return sendFunc, nil
}

// Subscribe subscribes to events using the provided EventsSubscription and callback function.
// It checks if the client is ready and if the callback function is provided.
// It validates the subscription request.
// It creates a channel for errors, subscribes to events with the request and initializes an events channel.
// It starts a goroutine to listen for events or errors and calls the callback function accordingly.
// If the context is canceled, it returns.
// It returns an error if any.
func (e *EventsClient) Subscribe(ctx context.Context, request *EventsSubscription, onEvent func(msg *Event, err error)) error {
	if err := e.isClientReady(); err != nil {
		return err
	}
	if onEvent == nil {
		return fmt.Errorf("events subscription callback function is required")
	}
	if err := request.Complete(e.client.opts).Validate(); err != nil {
		return err
	}
	errCh := make(chan error, 1)
	eventsCh, err := e.client.SubscribeToEventsWithRequest(ctx, request, errCh)
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

// Create creates a new event channel with the specified channel name.
// It sends a create-channel request to the KubeMQ server using the provided context and client.
// The channelType parameter specifies the type of the channel ('events' in this case).
// It returns an error if an error occurs during the creation of the channel.
func (e *EventsClient) Create(ctx context.Context, channel string) error {
	return CreateChannel(ctx, e.client, e.client.opts.clientId, channel, "events")
}

// Delete deletes a channel from the events client.
// It sends a delete channel request with the specified channel ID and type to the client.
// Returns an error if the delete channel request fails or if there is an error deleting the channel.
//
// Example usage:
//
//	err := eventsClient.Delete(ctx, "events.A")
//	if err != nil {
//	  log.Fatal(err)
//	}
func (e *EventsClient) Delete(ctx context.Context, channel string) error {
	return DeleteChannel(ctx, e.client, e.client.opts.clientId, channel, "events")
}

// List retrieves a list of PubSubChannels based on the provided search string.
// It calls ListPubSubChannels function with the given context, EventsClient's client,
// client ID, channel type "events", and the search string.
// It returns a slice of PubSubChannel pointers and an error.
func (e *EventsClient) List(ctx context.Context, search string) ([]*common.PubSubChannel, error) {
	return ListPubSubChannels(ctx, e.client, e.client.opts.clientId, "events", search)
}

// Close closes the EventsClient by invoking the Close method on its underlying Client.
// It returns an error if there was a problem closing the client.
func (e *EventsClient) Close() error {
	return e.client.Close()
}

// isClientReady checks if the client is initialized. If the client is not initialized,
// it returns an error indicating that the client is not ready. Otherwise, it returns nil.
func (e *EventsClient) isClientReady() error {
	if e.client == nil {
		return fmt.Errorf("client is not initialized")
	}
	return nil
}
