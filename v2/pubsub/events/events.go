package events

import (
	"context"
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
)

type EventsClient struct {
	kubemqClient pb.KubemqClient
	clientId     string
}

func NewEventsClient(kubemqClient pb.KubemqClient, clientId string) *EventsClient {
	return &EventsClient{
		kubemqClient: kubemqClient,
		clientId:     clientId}
}

func (e *EventsClient) Send(ctx context.Context, event *Event) error {
	if err := event.validate(); err != nil {
		return err
	}
	result, err := e.kubemqClient.SendEvent(ctx, event.toEvent(e.clientId))
	if err != nil {
		return err
	}
	if !result.Sent {
		return fmt.Errorf("%s", result.Error)
	}
	return nil
}

func (e *EventsClient) Subscribe(ctx context.Context, request *EventsSubscription, errCh chan error) {
	pbRequest := &pb.Subscribe{
		SubscribeTypeData: pb.Subscribe_Events,
		ClientID:          e.clientId,
		Channel:           request.Channel,
		Group:             request.Group,
	}
	stream, err := e.kubemqClient.SubscribeToEvents(ctx, pbRequest)
	if err != nil {
		errCh <- err
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				event, err := stream.Recv()
				if err != nil {
					errCh <- err
					return
				}
				if event == nil {
					continue
				}
				request.OnReceiveEvent(fromEvent(event))
			}
		}
	}()
}
