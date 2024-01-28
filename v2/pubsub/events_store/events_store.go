package events_store

import (
	"context"
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
)

type EventsStoreClient struct {
	kubemqClient pb.KubemqClient
	clientId     string
}

func NewEventsStoreClient(kubemqClient pb.KubemqClient, clientId string) *EventsStoreClient {
	return &EventsStoreClient{
		kubemqClient: kubemqClient,
		clientId:     clientId}
}

func (e *EventsStoreClient) Send(ctx context.Context, event *EventStore) error {
	if err := event.validate(); err != nil {
		return err
	}
	result, err := e.kubemqClient.SendEvent(ctx, event.toEventStore(e.clientId))
	if err != nil {
		return err
	}
	if !result.Sent {
		return fmt.Errorf("%s", result.Error)
	}
	return nil
}

func (e *EventsStoreClient) Subscribe(ctx context.Context, request *EventStoreSubscription, errCh chan error) {
	pbRequest := &pb.Subscribe{
		SubscribeTypeData: pb.Subscribe_EventsStore,
		ClientID:          e.clientId,
		Channel:           request.Channel,
		Group:             request.Group,
	}
	switch request.StartAt {
	case StartAtTypeUndefined:
		pbRequest.EventsStoreTypeData = pb.Subscribe_StartNewOnly
	case StartAtTypeFromNew:
		pbRequest.EventsStoreTypeData = pb.Subscribe_StartNewOnly
	case StartAtTypeFromFirst:
		pbRequest.EventsStoreTypeData = pb.Subscribe_StartFromFirst
	case StartAtTypeFromLast:
		pbRequest.EventsStoreTypeData = pb.Subscribe_StartFromLast
	case StartAtTypeFromSequence:
		pbRequest.EventsStoreTypeData = pb.Subscribe_StartAtSequence
		pbRequest.EventsStoreTypeValue = request.StartAtSequenceValue
	case StartAtTypeFromTime:
		pbRequest.EventsStoreTypeData = pb.Subscribe_StartAtTime
		pbRequest.EventsStoreTypeValue = request.StartAtTimeValue.UnixNano()
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
				request.OnReceiveEvent(fromEventReceived(event))
			}
		}
	}()
}
