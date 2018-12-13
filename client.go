package kubemq

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/kubemq-io/go/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"time"
)

const (
	defaultRequestTimeout = time.Second * 5
)

type Client struct {
	opts       *Options
	grpcConn   *grpc.ClientConn
	grpcClient pb.KubemqClient
}

func generateUUID() string {
	return uuid.New().String()
}

func getGrpcConn(ctx context.Context, opts *Options) (conn *grpc.ClientConn, err error) {
	var connOptions []grpc.DialOption
	if opts.isSecured {
		creds, err := credentials.NewClientTLSFromFile(opts.certFile, opts.serverOverrideDomain)
		if err != nil {
			return nil, fmt.Errorf("could not load tls cert: %s", err)
		}
		connOptions = append(connOptions, grpc.WithTransportCredentials(creds))
	} else {
		connOptions = append(connOptions, grpc.WithInsecure())
	}
	address := fmt.Sprintf("%s:%d", opts.host, opts.port)
	conn, err = grpc.DialContext(ctx, address, connOptions...)
	if err != nil {
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
			if conn != nil {
				conn.Close()
			}
		}
	}()
	return conn, nil

}

// NewClient - create client instance to be use to communicate with KubeMQ server
func NewClient(ctx context.Context, op ...Option) (*Client, error) {
	opts := GetDefaultOptions()
	for _, o := range op {
		o.apply(opts)
	}

	conn, err := getGrpcConn(ctx, opts)
	if err != nil {
		return nil, err
	}

	client := &Client{
		opts:       opts,
		grpcConn:   conn,
		grpcClient: pb.NewKubemqClient(conn),
	}
	return client, nil
}

// Close - closing client connection. any on going transactions will be aborted
func (c *Client) Close() error {
	return c.grpcConn.Close()
}

// E - create an empty event object
func (c *Client) E() *Event {
	return &Event{
		Id:       generateUUID(),
		Channel:  c.opts.defaultChannel,
		Metadata: "",
		Body:     nil,
		ClientId: c.opts.clientId,
		client:   c.grpcClient,
	}
}

// ES - create an empty event store object
func (c *Client) ES() *EventStore {
	return &EventStore{
		Id:       generateUUID(),
		Channel:  c.opts.defaultChannel,
		Metadata: "",
		Body:     nil,
		ClientId: c.opts.clientId,
		client:   c.grpcClient,
	}
}

// StreamEvents - send stream of events in a single call
func (c *Client) StreamEvents(ctx context.Context, eventsCh chan *Event, errCh chan error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.grpcClient.SendEventsStream(streamCtx)
	if err != nil {
		errCh <- err
		return
	}
	defer stream.CloseSend()
	go func() {
		for {
			result, err := stream.Recv()
			if err != nil {
				errCh <- err
				cancel()
				return
			}
			if !result.Sent {
				errCh <- fmt.Errorf("%s", result.Error)
			}
		}
	}()

	for {
		select {
		case event := <-eventsCh:
			err := stream.Send(&pb.Event{
				EventID:  event.Id,
				ClientID: c.opts.clientId,
				Channel:  event.Channel,
				Metadata: event.Metadata,
				Body:     event.Body,
			})
			if err != nil {
				errCh <- err
				return
			}
		case <-ctx.Done():
			return
		}
	}

}

// StreamEventsStore - send stream of events store in a single call
func (c *Client) StreamEventsStore(ctx context.Context, eventsCh chan *EventStore, eventsResultCh chan *EventStoreResult, errCh chan error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.grpcClient.SendEventsStream(streamCtx)
	if err != nil {
		errCh <- err
		return
	}
	defer stream.CloseSend()
	go func() {
		for {
			result, err := stream.Recv()
			if err != nil {
				errCh <- err
				cancel()
				return
			}
			eventResult := &EventStoreResult{
				Id:   result.EventID,
				Sent: result.Sent,
				Err:  nil,
			}
			if !result.Sent {
				eventResult.Err = fmt.Errorf("%s", result.Error)
			}
			eventsResultCh <- eventResult
		}
	}()

	for {
		select {
		case event := <-eventsCh:
			err := stream.Send(&pb.Event{
				EventID:  event.Id,
				ClientID: c.opts.clientId,
				Channel:  event.Channel,
				Metadata: event.Metadata,
				Body:     event.Body,
				Store:    true,
			})
			if err != nil {
				errCh <- err
				return
			}
		case <-ctx.Done():
			return
		}
	}

}

// C - create an empty command object
func (c *Client) C() *Command {
	return &Command{
		Id:       generateUUID(),
		Channel:  c.opts.defaultChannel,
		Metadata: "",
		Body:     nil,
		Timeout:  defaultRequestTimeout,
		client:   c.grpcClient,
		ClientId: c.opts.clientId,
	}
}

// Q - create an empty query object
func (c *Client) Q() *Query {
	return &Query{
		Id:       generateUUID(),
		Channel:  c.opts.defaultChannel,
		Metadata: "",
		Body:     nil,
		Timeout:  defaultRequestTimeout,
		ClientId: c.opts.clientId,
		CacheKey: "",
		CacheTTL: c.opts.defaultCacheTTL,
		client:   c.grpcClient,
	}
}

// R - create an empty response object for command or query responses
func (c *Client) R() *Response {
	return &Response{
		RequestId:  "",
		ResponseTo: "",
		Metadata:   "",
		Body:       nil,
		ClientId:   c.opts.clientId,
		ExecutedAt: time.Time{},
		Err:        nil,
		client:     c.grpcClient,
	}
}

// SubscribeToEvents - subscribe to events by channel and group. return channel of events or en error
func (c *Client) SubscribeToEvents(ctx context.Context, channel, group string, errCh chan error) (<-chan *Event, error) {
	eventsCh := make(chan *Event, c.opts.receiveBufferSize)
	subRequest := &pb.Subscribe{
		SubscribeTypeData: pb.Events,
		ClientID:          c.opts.clientId,
		Channel:           channel,
		Group:             group,
	}
	stream, err := c.grpcClient.SubscribeToEvents(ctx, subRequest)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			event, err := stream.Recv()
			if err != nil {
				errCh <- err
				close(eventsCh)
				return
			}
			eventsCh <- &Event{
				Id:       event.EventID,
				Channel:  event.Channel,
				Metadata: event.Metadata,
				Body:     event.Body,
			}
		}
	}()
	return eventsCh, nil
}

// SubscribeToEventsStore - subscribe to events store by channel and group with subscription option. return channel of events or en error
func (c *Client) SubscribeToEventsStore(ctx context.Context, channel, group string, errCh chan error, opt SubscriptionOption) (<-chan *EventStoreReceive, error) {
	eventsReceiveCh := make(chan *EventStoreReceive, c.opts.receiveBufferSize)
	subOption := subscriptionOption{}
	opt.apply(&subOption)
	subRequest := &pb.Subscribe{
		SubscribeTypeData:    pb.EventsStore,
		ClientID:             c.opts.clientId,
		Channel:              channel,
		Group:                group,
		EventsStoreTypeData:  subOption.kind,
		EventsStoreTypeValue: subOption.value,
	}
	stream, err := c.grpcClient.SubscribeToEvents(ctx, subRequest)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			event, err := stream.Recv()
			if err != nil {
				errCh <- err
				close(eventsReceiveCh)
				return
			}
			eventsReceiveCh <- &EventStoreReceive{
				Id:        event.EventID,
				Sequence:  event.Sequence,
				Timestamp: time.Unix(0, event.Timestamp),
				Channel:   event.Channel,
				Metadata:  event.Metadata,
				Body:      event.Body,
				ClientId:  "",
			}
		}
	}()
	return eventsReceiveCh, nil
}

// SubscribeToCommands - subscribe to commands requests by channel and group. return channel of CommandReceived or en error
func (c *Client) SubscribeToCommands(ctx context.Context, channel, group string, errCh chan error) (<-chan *CommandReceive, error) {
	commandsCh := make(chan *CommandReceive, c.opts.receiveBufferSize)
	subRequest := &pb.Subscribe{
		SubscribeTypeData: pb.Commands,
		ClientID:          c.opts.clientId,
		Channel:           channel,
		Group:             group,
	}
	stream, err := c.grpcClient.SubscribeToRequests(ctx, subRequest)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			request, err := stream.Recv()
			if err != nil {
				errCh <- err
				close(commandsCh)
				return
			}
			commandsCh <- &CommandReceive{
				Id:         request.RequestID,
				Channel:    request.Channel,
				Metadata:   request.Metadata,
				Body:       request.Body,
				ResponseTo: request.ReplyChannel,
			}
		}
	}()
	return commandsCh, nil
}

// SubscribeToQueries - subscribe to queries requests by channel and group. return channel of QueryReceived or en error
func (c *Client) SubscribeToQueries(ctx context.Context, channel, group string, errCh chan error) (<-chan *QueryReceive, error) {
	queriesCH := make(chan *QueryReceive, c.opts.receiveBufferSize)
	subRequest := &pb.Subscribe{
		SubscribeTypeData: pb.Queries,
		ClientID:          c.opts.clientId,
		Channel:           channel,
		Group:             group,
	}
	stream, err := c.grpcClient.SubscribeToRequests(ctx, subRequest)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			request, err := stream.Recv()
			if err != nil {
				errCh <- err
				close(queriesCH)
				return
			}
			queriesCH <- &QueryReceive{
				Id:         request.RequestID,
				Channel:    request.Channel,
				Metadata:   request.Metadata,
				Body:       request.Body,
				ResponseTo: request.ReplyChannel,
			}
		}
	}()
	return queriesCH, nil
}
