package kubemq

import (
	"context"
	"fmt"
)

type EventsMessageHandler func(*Event)
type EventsErrorsHandler func(error)

type EventsClient struct {
	client *Client
}

type EventsSubscription struct {
	Channel  string
	Group    string
	ClientId string
}

func (es *EventsSubscription) Complete(opts *Options) *EventsSubscription {
	if es.ClientId == "" {
		es.ClientId = opts.clientId
	}
	return es
}
func (es *EventsSubscription) Validate() error {
	if es.Channel == "" {
		return fmt.Errorf("events subscription must have a channel")
	}
	if es.ClientId == "" {
		return fmt.Errorf("events subscription must have a clientId")
	}
	return nil
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
	if err:=e.isClientReady();err!=nil{
		return err
	}
	message.transport = e.client.transport
	return e.client.SetEvent(message).Send(ctx)
}

func (e *EventsClient) Stream(ctx context.Context, onError func(err error)) (func(msg *Event) error, error) {
	if err:=e.isClientReady();err!=nil{
		return nil,err
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

func (e *EventsClient) Subscribe(ctx context.Context, request *EventsSubscription, onEvent func(msg *Event, err error)) error {
	if err:=e.isClientReady();err!=nil{
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

func (e *EventsClient) Close() error {
	return e.client.Close()
}

func (e *EventsClient) isClientReady() error {
	if e.client==nil {
		return fmt.Errorf("client is not initialized")
	}
	return nil
}
