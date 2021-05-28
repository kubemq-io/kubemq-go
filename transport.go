package kubemq

import (
	"context"

	pb "github.com/kubemq-io/protobuf/go"
)

type Transport interface {
	Ping(ctx context.Context) (*ServerInfo, error)
	SendEvent(ctx context.Context, event *Event) error
	StreamEvents(ctx context.Context, eventsCh chan *Event, errCh chan error)
	SubscribeToEvents(ctx context.Context, request *EventsSubscription, errCh chan error) (<-chan *Event, error)
	SendEventStore(ctx context.Context, eventStore *EventStore) (*EventStoreResult, error)
	StreamEventsStore(ctx context.Context, eventsCh chan *EventStore, eventsResultCh chan *EventStoreResult, errCh chan error)
	SubscribeToEventsStore(ctx context.Context, request *EventsStoreSubscription, errCh chan error) (<-chan *EventStoreReceive, error)
	SendCommand(ctx context.Context, command *Command) (*CommandResponse, error)
	SubscribeToCommands(ctx context.Context, request *CommandsSubscription, errCh chan error) (<-chan *CommandReceive, error)
	SendQuery(ctx context.Context, query *Query) (*QueryResponse, error)
	SubscribeToQueries(ctx context.Context, request *QueriesSubscription, errCh chan error) (<-chan *QueryReceive, error)
	SendResponse(ctx context.Context, response *Response) error
	SendQueueMessage(ctx context.Context, msg *QueueMessage) (*SendQueueMessageResult, error)
	SendQueueMessages(ctx context.Context, msg []*QueueMessage) ([]*SendQueueMessageResult, error)
	ReceiveQueueMessages(ctx context.Context, req *ReceiveQueueMessagesRequest) (*ReceiveQueueMessagesResponse, error)
	AckAllQueueMessages(ctx context.Context, req *AckAllQueueMessagesRequest) (*AckAllQueueMessagesResponse, error)
	StreamQueueMessage(ctx context.Context, reqCh chan *pb.StreamQueueMessagesRequest, resCh chan *pb.StreamQueueMessagesResponse, errCh chan error, doneCh chan bool)
	QueuesInfo(ctx context.Context, filter string) (*QueuesInfo, error)
	GetGRPCRawClient() (pb.KubemqClient, error)
	Close() error
}
