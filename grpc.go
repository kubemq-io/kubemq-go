package kubemq

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"time"
)

type gRPCTransport struct {
	opts   *Options
	conn   *grpc.ClientConn
	client pb.KubemqClient
}

func newGRPCTransport(ctx context.Context, opts *Options) (Transport, error) {
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
	g := &gRPCTransport{
		opts:   opts,
		conn:   nil,
		client: nil,
	}
	connOptions = append(connOptions, grpc.WithUnaryInterceptor(g.SetUnaryInterceptor()), grpc.WithStreamInterceptor(g.SetStreamInterceptor()))

	var err error
	g.conn, err = grpc.DialContext(ctx, address, connOptions...)
	if err != nil {
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
			if g.conn != nil {
				_ = g.conn.Close()
			}
		}
	}()
	g.client = pb.NewKubemqClient(g.conn)
	return g, nil

}

func (g *gRPCTransport) SetUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if g.opts.token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, kubeMQTokenHeader, g.opts.token)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (g *gRPCTransport) SetStreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if g.opts.token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, kubeMQTokenHeader, g.opts.token)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}
func (g *gRPCTransport) SendEvent(ctx context.Context, event *Event) error {

	result, err := g.client.SendEvent(ctx, &pb.Event{
		EventID:  event.Id,
		ClientID: event.ClientId,
		Channel:  event.Channel,
		Metadata: event.Metadata,
		Body:     event.Body,
	})
	if err != nil {
		return err
	}
	if !result.Sent {
		return fmt.Errorf("%s", result.Error)
	}
	return nil
}

func (g *gRPCTransport) StreamEvents(ctx context.Context, eventsCh chan *Event, errCh chan error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := g.client.SendEventsStream(streamCtx)
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
				ClientID: g.opts.clientId,
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

func (g *gRPCTransport) SubscribeToEvents(ctx context.Context, channel, group string, errCh chan error) (<-chan *Event, error) {
	eventsCh := make(chan *Event, g.opts.receiveBufferSize)
	subRequest := &pb.Subscribe{
		SubscribeTypeData: pb.Events,
		ClientID:          g.opts.clientId,
		Channel:           channel,
		Group:             group,
	}
	stream, err := g.client.SubscribeToEvents(ctx, subRequest)
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

func (g *gRPCTransport) SendEventStore(ctx context.Context, eventStore *EventStore) (*EventStoreResult, error) {
	result, err := g.client.SendEvent(ctx, &pb.Event{
		EventID:  eventStore.Id,
		ClientID: eventStore.ClientId,
		Channel:  eventStore.Channel,
		Metadata: eventStore.Metadata,
		Body:     eventStore.Body,
		Store:    true,
	})
	if err != nil {
		return nil, err
	}
	eventResult := &EventStoreResult{
		Id:   result.EventID,
		Sent: result.Sent,
		Err:  nil,
	}
	if !result.Sent {
		eventResult.Err = fmt.Errorf("%s", result.Error)
	}
	return eventResult, nil
}

func (g *gRPCTransport) StreamEventsStore(ctx context.Context, eventsCh chan *EventStore, eventsResultCh chan *EventStoreResult, errCh chan error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := g.client.SendEventsStream(streamCtx)
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
				ClientID: g.opts.clientId,
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

func (g *gRPCTransport) SubscribeToEventsStore(ctx context.Context, channel, group string, errCh chan error, opt SubscriptionOption) (<-chan *EventStoreReceive, error) {
	eventsReceiveCh := make(chan *EventStoreReceive, g.opts.receiveBufferSize)
	subOption := subscriptionOption{}
	opt.apply(&subOption)
	subRequest := &pb.Subscribe{
		SubscribeTypeData:    pb.EventsStore,
		ClientID:             g.opts.clientId,
		Channel:              channel,
		Group:                group,
		EventsStoreTypeData:  subOption.kind,
		EventsStoreTypeValue: subOption.value,
	}
	stream, err := g.client.SubscribeToEvents(ctx, subRequest)
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

func (g *gRPCTransport) SendCommand(ctx context.Context, command *Command) (*CommandResponse, error) {

	grpcRequest := &pb.Request{
		RequestID:       command.Id,
		RequestTypeData: pb.Command,
		ClientID:        command.ClientId,
		Channel:         command.Channel,
		Metadata:        command.Metadata,
		Body:            command.Body,
		Timeout:         int32(command.Timeout.Nanoseconds() / 1e6),
	}
	grpcResponse, err := g.client.SendRequest(ctx, grpcRequest)
	if err != nil {
		return nil, err
	}
	commandResponse := &CommandResponse{
		CommandId:        grpcResponse.RequestID,
		ResponseClientId: grpcResponse.ClientID,
		Executed:         grpcResponse.Executed,
		ExecutedAt:       time.Unix(grpcResponse.Timestamp, 0),
		Error:            grpcResponse.Error,
	}
	return commandResponse, nil
}

func (g *gRPCTransport) SubscribeToCommands(ctx context.Context, channel, group string, errCh chan error) (<-chan *CommandReceive, error) {
	commandsCh := make(chan *CommandReceive, g.opts.receiveBufferSize)
	subRequest := &pb.Subscribe{
		SubscribeTypeData: pb.Commands,
		ClientID:          g.opts.clientId,
		Channel:           channel,
		Group:             group,
	}
	stream, err := g.client.SubscribeToRequests(ctx, subRequest)
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

func (g *gRPCTransport) SendQuery(ctx context.Context, query *Query) (*QueryResponse, error) {
	grpcRequest := &pb.Request{
		RequestID:       query.Id,
		RequestTypeData: pb.Query,
		ClientID:        query.ClientId,
		Channel:         query.Channel,
		Metadata:        query.Metadata,
		Body:            query.Body,
		Timeout:         int32(query.Timeout.Nanoseconds() / 1e6),
		CacheKey:        query.CacheKey,
		CacheTTL:        int32(query.CacheTTL.Nanoseconds() / 1e6),
	}
	grpcResponse, err := g.client.SendRequest(ctx, grpcRequest)
	if err != nil {
		return nil, err
	}
	queryResponse := &QueryResponse{
		QueryId:          grpcResponse.RequestID,
		Executed:         grpcResponse.Executed,
		ExecutedAt:       time.Unix(grpcResponse.Timestamp, 0),
		Metadata:         grpcResponse.Metadata,
		ResponseClientId: grpcResponse.ClientID,
		Body:             grpcResponse.Body,
		CacheHit:         grpcResponse.CacheHit,
		Error:            grpcResponse.Error,
	}
	return queryResponse, nil
}

func (g *gRPCTransport) SubscribeToQueries(ctx context.Context, channel, group string, errCh chan error) (<-chan *QueryReceive, error) {
	queriesCH := make(chan *QueryReceive, g.opts.receiveBufferSize)
	subRequest := &pb.Subscribe{
		SubscribeTypeData: pb.Queries,
		ClientID:          g.opts.clientId,
		Channel:           channel,
		Group:             group,
	}
	stream, err := g.client.SubscribeToRequests(ctx, subRequest)
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

func (g *gRPCTransport) SendResponse(ctx context.Context, response *Response) error {

	grpcResponse := &pb.Response{
		ClientID:     response.ClientId,
		RequestID:    response.RequestId,
		ReplyChannel: response.ResponseTo,
		Metadata:     response.Metadata,
		Body:         response.Body,
		Timestamp:    response.ExecutedAt.Unix(),
		Executed:     true,
		Error:        "",
	}
	if response.Err != nil {
		grpcResponse.Executed = false
		grpcResponse.Error = response.Err.Error()
	}
	_, err := g.client.SendResponse(ctx, grpcResponse)
	if err != nil {
		return err
	}
	return nil
}

func (g *gRPCTransport) Close() error {
	return g.conn.Close()
}
