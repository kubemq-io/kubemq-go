package grpc

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/v2/config"
	pb "github.com/kubemq-io/protobuf/go"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Transport struct {
	opts     *config.Connection
	conn     *grpc.ClientConn
	client   pb.KubemqClient
	isClosed *atomic.Bool
}

func NewTransport(ctx context.Context, cfg *config.Connection) (*Transport, error) {
	t := &Transport{
		opts:     cfg,
		conn:     nil,
		client:   nil,
		isClosed: atomic.NewBool(false),
	}

	connOptions := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cfg.MaxReceiveSize), grpc.MaxCallSendMsgSize(cfg.MaxSendSize)),
		grpc.WithUnaryInterceptor(t.setUnaryInterceptor()),
		grpc.WithStreamInterceptor(t.setStreamInterceptor()),
	}
	tlsOptions, err := getTLSConnectionOptions(cfg.Tls)
	if err != nil {
		return nil, err
	}
	if tlsOptions != nil {
		connOptions = append(connOptions, tlsOptions...)
	}
	if keepAliveOptions := getKeepAliveConnectionOptions(cfg.KeepAlive); keepAliveOptions != nil {
		connOptions = append(connOptions, keepAliveOptions...)
	}

	t.conn, err = grpc.DialContext(ctx, cfg.Address, connOptions...)
	if err != nil {
		return nil, fmt.Errorf("error connecting to kubemq server, %w", err)
	}
	go func() {
		<-ctx.Done()
		if t.conn != nil {
			_ = t.conn.Close()
		}
	}()
	t.client = pb.NewKubemqClient(t.conn)
	_, err = t.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Transport) setUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if t.opts.AuthToken != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", t.opts.AuthToken)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (t *Transport) setStreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if t.opts.AuthToken != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", t.opts.AuthToken)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func (t *Transport) Ping(ctx context.Context) (*ServerInfo, error) {
	res, err := t.client.Ping(ctx, &pb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("error connecting to kubemq server, %w", err)
	}
	si := &ServerInfo{
		Host:                res.Host,
		Version:             res.Version,
		ServerStartTime:     res.ServerStartTime,
		ServerUpTimeSeconds: res.ServerUpTimeSeconds,
	}
	return si, nil
}

func (t *Transport) KubeMQClient() pb.KubemqClient {
	return t.client
}
func (t *Transport) Close() error {
	err := t.conn.Close()
	if err != nil {
		return err
	}
	t.isClosed.Store(true)
	return nil
}

//
//func (t *Transport) SendEvent(ctx context.Context, event *Event) error {
//	if t.isClosed.Load() {
//		return errConnectionClosed
//	}
//	if event.ClientId == "" && t.opts.clientId != "" {
//		event.ClientId = t.opts.clientId
//	}
//	result, err := t.client.SendEvent(ctx, &pb.Event{
//		EventID:  event.Id,
//		ClientID: event.ClientId,
//		Channel:  event.Channel,
//		Metadata: event.Metadata,
//		Body:     event.Body,
//		Store:    false,
//		Tags:     event.Tags,
//	})
//	if err != nil {
//		return err
//	}
//	if !result.Sent {
//		return fmt.Errorf("%s", result.Error)
//	}
//	return nil
//}
//
//func (t *Transport) StreamEvents(ctx context.Context, eventsCh chan *Event, errCh chan error) {
//	retries := atomic.NewUint32(0)
//	for {
//	start:
//		localErrCh := make(chan error, 2)
//		retries.Inc()
//		go t.streamEvents(ctx, eventsCh, localErrCh)
//		for {
//			select {
//			case err := <-localErrCh:
//				if !t.opts.autoReconnect {
//					errCh <- err
//					return
//				} else {
//					if t.opts.maxReconnect == 0 || int(retries.Load()) <= t.opts.maxReconnect {
//						time.Sleep(t.opts.reconnectInterval)
//						goto start
//					}
//					errCh <- fmt.Errorf("max reconnects reached, aborting")
//					return
//				}
//			case <-ctx.Done():
//				return
//			}
//		}
//	}
//}
//
//func (t *Transport) streamEvents(ctx context.Context, eventsCh chan *Event, errCh chan error) {
//	streamCtx, cancel := context.WithCancel(ctx)
//	defer cancel()
//	quit := make(chan struct{}, 1)
//	stream, err := t.client.SendEventsStream(streamCtx)
//	if err != nil {
//		errCh <- err
//		return
//	}
//	defer func() {
//		_ = stream.CloseSend()
//	}()
//	go func() {
//		for {
//			result, err := stream.Recv()
//			if err != nil {
//				errCh <- err
//				quit <- struct{}{}
//				cancel()
//				return
//			}
//			if !result.Sent {
//				errCh <- fmt.Errorf("%s", result.Error)
//			}
//		}
//	}()
//
//	for {
//		select {
//		case event := <-eventsCh:
//			if event.ClientId == "" && t.opts.clientId != "" {
//				event.ClientId = t.opts.clientId
//			}
//			err := stream.Send(&pb.Event{
//				EventID:  event.Id,
//				ClientID: event.ClientId,
//				Channel:  event.Channel,
//				Metadata: event.Metadata,
//				Body:     event.Body,
//				Store:    false,
//				Tags:     event.Tags,
//			})
//			if err != nil {
//				errCh <- err
//				return
//			}
//		case <-quit:
//			return
//		case <-ctx.Done():
//			return
//		}
//	}
//}
//
//func (t *Transport) SubscribeToEvents(ctx context.Context, request *EventsSubscription, errCh chan error) (<-chan *Event, error) {
//	eventsCh := make(chan *Event, t.opts.receiveBufferSize)
//	subRequest := &pb.Subscribe{
//		SubscribeTypeData: pb.Subscribe_Events,
//		ClientID:          request.ClientId,
//		Channel:           request.Channel,
//		Group:             request.Group,
//	}
//
//	go func() {
//		retries := atomic.NewUint32(0)
//		for {
//			internalErrCh := make(chan error, 1)
//			quit := make(chan struct{}, 1)
//			retries.Inc()
//			go func() {
//				readyCh := make(chan bool, 1)
//				t.subscribeToEvents(ctx, subRequest, eventsCh, internalErrCh, readyCh)
//				for {
//					select {
//					case <-readyCh:
//						retries.Store(0)
//					case <-quit:
//						return
//					case <-ctx.Done():
//						return
//					}
//				}
//			}()
//
//			select {
//			case err := <-internalErrCh:
//				quit <- struct{}{}
//				if t.isClosed.Load() {
//					errCh <- errConnectionClosed
//					return
//				}
//				if !t.opts.autoReconnect {
//					errCh <- err
//					return
//				} else {
//					if t.opts.maxReconnect == 0 || int(retries.Load()) <= t.opts.maxReconnect {
//						time.Sleep(t.opts.reconnectInterval)
//						continue
//					}
//					errCh <- fmt.Errorf("max reconnects reached, aborting")
//					close(eventsCh)
//					return
//				}
//			case <-ctx.Done():
//				return
//			}
//
//		}
//	}()
//	return eventsCh, nil
//}
//
//func (t *Transport) subscribeToEvents(ctx context.Context, subRequest *pb.Subscribe, eventsCh chan *Event, errCh chan error, readyCh chan bool) {
//	stream, err := t.client.SubscribeToEvents(ctx, subRequest)
//	if err != nil {
//		errCh <- err
//		return
//	}
//	readyCh <- true
//	for {
//		event, err := stream.Recv()
//		if err != nil {
//			errCh <- err
//			return
//		}
//		select {
//		case eventsCh <- &Event{
//			Id:        event.EventID,
//			Channel:   event.Channel,
//			Metadata:  event.Metadata,
//			Body:      event.Body,
//			ClientId:  t.opts.clientId,
//			Tags:      event.Tags,
//			transport: nil,
//		}:
//		case <-ctx.Done():
//			return
//		}
//
//	}
//}
//
//func (t *Transport) SendEventStore(ctx context.Context, eventStore *EventStore) (*EventStoreResult, error) {
//	if eventStore.ClientId == "" && t.opts.clientId != "" {
//		eventStore.ClientId = t.opts.clientId
//	}
//	result, err := t.client.SendEvent(ctx, &pb.Event{
//		EventID:  eventStore.Id,
//		ClientID: eventStore.ClientId,
//		Channel:  eventStore.Channel,
//		Metadata: eventStore.Metadata,
//		Body:     eventStore.Body,
//		Store:    true,
//		Tags:     eventStore.Tags,
//	})
//	if err != nil {
//		return nil, err
//	}
//	eventResult := &EventStoreResult{
//		Id:   result.EventID,
//		Sent: result.Sent,
//		Err:  nil,
//	}
//	if !result.Sent {
//		eventResult.Err = fmt.Errorf("%s", result.Error)
//	}
//	return eventResult, nil
//}
//
//func (t *Transport) StreamEventsStore(ctx context.Context, eventsCh chan *EventStore, eventsResultCh chan *EventStoreResult, errCh chan error) {
//	retries := atomic.NewUint32(0)
//	for {
//	start:
//		localEventsResultCh := make(chan *EventStoreResult, 2)
//		localErrCh := make(chan error, 2)
//		retries.Inc()
//		go t.streamEventsStore(ctx, eventsCh, localEventsResultCh, localErrCh)
//		for {
//			select {
//			case eventResult, ok := <-localEventsResultCh:
//				if ok {
//					eventsResultCh <- eventResult
//				} else {
//					if !t.opts.autoReconnect {
//						close(eventsResultCh)
//						return
//					} else {
//						if t.opts.maxReconnect == 0 || int(retries.Load()) <= t.opts.maxReconnect {
//							time.Sleep(t.opts.reconnectInterval)
//							goto start
//						}
//						errCh <- fmt.Errorf("max reconnects reached, aborting")
//						close(eventsResultCh)
//						return
//					}
//				}
//			case err := <-localErrCh:
//				if !t.opts.autoReconnect {
//					errCh <- err
//					return
//				} else {
//					if t.opts.maxReconnect == 0 || int(retries.Load()) <= t.opts.maxReconnect {
//						time.Sleep(t.opts.reconnectInterval)
//						goto start
//					}
//					errCh <- fmt.Errorf("max reconnects reached, aborting")
//					close(eventsResultCh)
//					return
//				}
//			case <-ctx.Done():
//				return
//			}
//		}
//	}
//}
//
//func (t *Transport) streamEventsStore(ctx context.Context, eventsCh chan *EventStore, eventsResultCh chan *EventStoreResult, errCh chan error) {
//	streamCtx, cancel := context.WithCancel(ctx)
//	defer cancel()
//	quit := make(chan struct{}, 1)
//	stream, err := t.client.SendEventsStream(streamCtx)
//	if err != nil {
//		errCh <- err
//		return
//	}
//	defer func() {
//		_ = stream.CloseSend()
//		close(eventsResultCh)
//	}()
//	go func() {
//		for {
//			result, err := stream.Recv()
//			if err != nil {
//				select {
//				case errCh <- err:
//				default:
//				}
//				quit <- struct{}{}
//				cancel()
//				return
//			}
//			eventResult := &EventStoreResult{
//				Id:   result.EventID,
//				Sent: result.Sent,
//				Err:  nil,
//			}
//			if !result.Sent {
//				eventResult.Err = fmt.Errorf("%s", result.Error)
//			}
//			select {
//			case eventsResultCh <- eventResult:
//			case <-quit:
//				return
//			case <-ctx.Done():
//				return
//			}
//
//		}
//	}()
//
//	for {
//		select {
//		case eventStore := <-eventsCh:
//			if eventStore.ClientId == "" && t.opts.clientId != "" {
//				eventStore.ClientId = t.opts.clientId
//			}
//			err := stream.Send(&pb.Event{
//				EventID:  eventStore.Id,
//				ClientID: t.opts.clientId,
//				Channel:  eventStore.Channel,
//				Metadata: eventStore.Metadata,
//				Body:     eventStore.Body,
//				Store:    true,
//				Tags:     eventStore.Tags,
//			})
//			if err != nil {
//				select {
//				case errCh <- err:
//				default:
//				}
//				return
//			}
//		case <-ctx.Done():
//			return
//		}
//	}
//}
//
//func (t *Transport) SubscribeToEventsStore(ctx context.Context, request *EventsStoreSubscription, errCh chan error) (<-chan *EventStoreReceive, error) {
//	eventsReceiveCh := make(chan *EventStoreReceive, t.opts.receiveBufferSize)
//	subOption := subscriptionOption{}
//	request.SubscriptionType.apply(&subOption)
//	subRequest := &pb.Subscribe{
//		SubscribeTypeData:    pb.Subscribe_EventsStore,
//		ClientID:             request.ClientId,
//		Channel:              request.Channel,
//		Group:                request.Group,
//		EventsStoreTypeData:  subOption.kind,
//		EventsStoreTypeValue: subOption.value,
//	}
//	go func() {
//		retries := atomic.NewUint32(0)
//		for {
//			internalErrCh := make(chan error, 1)
//			quit := make(chan struct{}, 1)
//			retries.Inc()
//			go func() {
//				readyCh := make(chan bool, 1)
//				t.subscribeToEventsStore(ctx, subRequest, eventsReceiveCh, internalErrCh, readyCh)
//				for {
//					select {
//					case <-readyCh:
//						retries.Store(0)
//					case <-quit:
//						return
//					case <-ctx.Done():
//						return
//					}
//				}
//			}()
//
//			select {
//			case err := <-internalErrCh:
//				quit <- struct{}{}
//				if t.isClosed.Load() {
//					errCh <- errConnectionClosed
//					return
//				}
//				if !t.opts.autoReconnect {
//					errCh <- err
//					return
//				} else {
//					if t.opts.maxReconnect == 0 || int(retries.Load()) <= t.opts.maxReconnect {
//						time.Sleep(t.opts.reconnectInterval)
//						continue
//					}
//					errCh <- fmt.Errorf("max reconnects reached, aborting")
//					close(eventsReceiveCh)
//					return
//				}
//			case <-ctx.Done():
//				return
//			}
//
//		}
//	}()
//	return eventsReceiveCh, nil
//}
//
//func (t *Transport) subscribeToEventsStore(ctx context.Context, subRequest *pb.Subscribe, eventsCh chan *EventStoreReceive, errCh chan error, readyCh chan bool) {
//	stream, err := t.client.SubscribeToEvents(ctx, subRequest)
//	if err != nil {
//		errCh <- err
//		return
//	}
//	readyCh <- true
//	for {
//		event, err := stream.Recv()
//		if err != nil {
//			errCh <- err
//			return
//		}
//		select {
//		case eventsCh <- &EventStoreReceive{
//			Id:        event.EventID,
//			Sequence:  event.Sequence,
//			Timestamp: time.Unix(0, event.Timestamp),
//			Channel:   event.Channel,
//			Metadata:  event.Metadata,
//			Body:      event.Body,
//			ClientId:  t.opts.clientId,
//			Tags:      event.Tags,
//		}:
//		case <-ctx.Done():
//			return
//		}
//
//	}
//}
//
//func (t *Transport) SendCommand(ctx context.Context, command *Command) (*CommandResponse, error) {
//	if t.isClosed.Load() {
//		return nil, errConnectionClosed
//	}
//	if command.ClientId == "" && t.opts.clientId != "" {
//		command.ClientId = t.opts.clientId
//	}
//	grpcRequest := &pb.Request{
//		RequestID:       command.Id,
//		RequestTypeData: pb.Request_Command,
//		ClientID:        command.ClientId,
//		Channel:         command.Channel,
//		Metadata:        command.Metadata,
//		Body:            command.Body,
//		ReplyChannel:    "",
//		Timeout:         int32(command.Timeout.Nanoseconds() / 1e6),
//		CacheKey:        "",
//		CacheTTL:        0,
//		Span:            nil,
//		Tags:            command.Tags,
//	}
//	var span *trace.Span
//	if command.trace != nil {
//		ctx, span = trace.StartSpan(ctx, command.trace.Name, trace.WithSpanKind(trace.SpanKindClient))
//		defer span.End()
//		span.AddAttributes(command.trace.attributes...)
//	}
//
//	grpcResponse, err := t.client.SendRequest(ctx, grpcRequest)
//	if err != nil {
//		return nil, err
//	}
//	commandResponse := &CommandResponse{
//		CommandId:        grpcResponse.RequestID,
//		ResponseClientId: grpcResponse.ClientID,
//		Executed:         grpcResponse.Executed,
//		ExecutedAt:       time.Unix(grpcResponse.Timestamp, 0),
//		Error:            grpcResponse.Error,
//		Tags:             grpcResponse.Tags,
//	}
//	return commandResponse, nil
//}
//
//func (t *Transport) SubscribeToCommands(ctx context.Context, request *CommandsSubscription, errCh chan error) (<-chan *CommandReceive, error) {
//	commandsCh := make(chan *CommandReceive, t.opts.receiveBufferSize)
//	subRequest := &pb.Subscribe{
//		SubscribeTypeData: pb.Subscribe_Commands,
//		ClientID:          request.ClientId,
//		Channel:           request.Channel,
//		Group:             request.Group,
//	}
//	go func() {
//		retries := atomic.NewUint32(0)
//		for {
//			internalErrCh := make(chan error, 1)
//			quit := make(chan struct{}, 1)
//			retries.Inc()
//			go func() {
//				readyCh := make(chan bool, 1)
//				t.subscribeToCommands(ctx, subRequest, commandsCh, internalErrCh, readyCh)
//				for {
//					select {
//					case <-readyCh:
//						retries.Store(0)
//					case <-quit:
//						return
//					case <-ctx.Done():
//						return
//					}
//				}
//			}()
//
//			select {
//			case err := <-internalErrCh:
//				quit <- struct{}{}
//				if t.isClosed.Load() {
//					errCh <- errConnectionClosed
//					return
//				}
//				if !t.opts.autoReconnect {
//					errCh <- err
//					return
//				} else {
//					if t.opts.maxReconnect == 0 || int(retries.Load()) <= t.opts.maxReconnect {
//						time.Sleep(t.opts.reconnectInterval)
//						continue
//					}
//					errCh <- fmt.Errorf("max reconnects reached, aborting")
//					close(commandsCh)
//					return
//				}
//			case <-ctx.Done():
//				return
//			}
//
//		}
//	}()
//	return commandsCh, nil
//}
//
//func (t *Transport) subscribeToCommands(ctx context.Context, subRequest *pb.Subscribe, commandsCh chan *CommandReceive, errCh chan error, readyCh chan bool) {
//	stream, err := t.client.SubscribeToRequests(ctx, subRequest)
//	if err != nil {
//		errCh <- err
//		return
//	}
//	readyCh <- true
//	for {
//		command, err := stream.Recv()
//		if err != nil {
//			errCh <- err
//			return
//		}
//		select {
//		case commandsCh <- &CommandReceive{
//			ClientId:   command.ClientID,
//			Id:         command.RequestID,
//			Channel:    command.Channel,
//			Metadata:   command.Metadata,
//			Body:       command.Body,
//			ResponseTo: command.ReplyChannel,
//			Tags:       command.Tags,
//		}:
//		case <-ctx.Done():
//			return
//		}
//	}
//}
//
//func (t *Transport) SendQuery(ctx context.Context, query *Query) (*QueryResponse, error) {
//	if t.isClosed.Load() {
//		return nil, errConnectionClosed
//	}
//	if query.ClientId == "" && t.opts.clientId != "" {
//		query.ClientId = t.opts.clientId
//	}
//	grpcRequest := &pb.Request{
//		RequestID:       query.Id,
//		RequestTypeData: pb.Request_Query,
//		ClientID:        query.ClientId,
//		Channel:         query.Channel,
//		Metadata:        query.Metadata,
//		Body:            query.Body,
//		ReplyChannel:    "",
//		Timeout:         int32(query.Timeout.Nanoseconds() / 1e6),
//		CacheKey:        query.CacheKey,
//		CacheTTL:        int32(query.CacheTTL.Nanoseconds() / 1e6),
//		Span:            nil,
//		Tags:            query.Tags,
//	}
//	var span *trace.Span
//	if query.trace != nil {
//		ctx, span = trace.StartSpan(ctx, query.trace.Name, trace.WithSpanKind(trace.SpanKindClient))
//		defer span.End()
//		span.AddAttributes(query.trace.attributes...)
//	}
//
//	grpcResponse, err := t.client.SendRequest(ctx, grpcRequest)
//	if err != nil {
//		return nil, err
//	}
//	queryResponse := &QueryResponse{
//		QueryId:          grpcResponse.RequestID,
//		Executed:         grpcResponse.Executed,
//		ExecutedAt:       time.Unix(grpcResponse.Timestamp, 0),
//		Metadata:         grpcResponse.Metadata,
//		ResponseClientId: grpcResponse.ClientID,
//		Body:             grpcResponse.Body,
//		Tags:             grpcResponse.Tags,
//		CacheHit:         grpcResponse.CacheHit,
//		Error:            grpcResponse.Error,
//	}
//	return queryResponse, nil
//}
//
//func (t *Transport) SubscribeToQueries(ctx context.Context, request *QueriesSubscription, errCh chan error) (<-chan *QueryReceive, error) {
//	queriesCh := make(chan *QueryReceive, t.opts.receiveBufferSize)
//	subRequest := &pb.Subscribe{
//		SubscribeTypeData: pb.Subscribe_Queries,
//		ClientID:          request.ClientId,
//		Channel:           request.Channel,
//		Group:             request.Group,
//	}
//	go func() {
//		retries := atomic.NewUint32(0)
//		for {
//			internalErrCh := make(chan error, 1)
//			quit := make(chan struct{}, 1)
//			retries.Inc()
//			go func() {
//				readyCh := make(chan bool, 1)
//				t.subscribeToQueries(ctx, subRequest, queriesCh, internalErrCh, readyCh)
//				for {
//					select {
//					case <-readyCh:
//						retries.Store(0)
//					case <-quit:
//						return
//					case <-ctx.Done():
//						return
//					}
//				}
//			}()
//
//			select {
//			case err := <-internalErrCh:
//				quit <- struct{}{}
//				if t.isClosed.Load() {
//					errCh <- errConnectionClosed
//					return
//				}
//				if !t.opts.autoReconnect {
//					errCh <- err
//					return
//				} else {
//					if t.opts.maxReconnect == 0 || int(retries.Load()) <= t.opts.maxReconnect {
//						time.Sleep(t.opts.reconnectInterval)
//						continue
//					}
//					errCh <- fmt.Errorf("max reconnects reached, aborting")
//					close(queriesCh)
//					return
//				}
//			case <-ctx.Done():
//				return
//			}
//
//		}
//	}()
//	return queriesCh, nil
//}
//
//func (t *Transport) subscribeToQueries(ctx context.Context, subRequest *pb.Subscribe, queriesCH chan *QueryReceive, errCh chan error, readyCh chan bool) {
//	stream, err := t.client.SubscribeToRequests(ctx, subRequest)
//	if err != nil {
//		errCh <- err
//		return
//	}
//	readyCh <- true
//	for {
//		query, err := stream.Recv()
//		if err != nil {
//			errCh <- err
//			return
//		}
//		queriesCH <- &QueryReceive{
//			Id:         query.RequestID,
//			ClientId:   query.ClientID,
//			Channel:    query.Channel,
//			Metadata:   query.Metadata,
//			Body:       query.Body,
//			ResponseTo: query.ReplyChannel,
//			Tags:       query.Tags,
//		}
//	}
//}
//
//func (t *Transport) SendResponse(ctx context.Context, response *Response) error {
//	if t.isClosed.Load() {
//		return errConnectionClosed
//	}
//	if response.ClientId == "" && t.opts.clientId != "" {
//		response.ClientId = t.opts.clientId
//	}
//	grpcResponse := &pb.Response{
//		ClientID:             response.ClientId,
//		RequestID:            response.RequestId,
//		ReplyChannel:         response.ResponseTo,
//		Metadata:             response.Metadata,
//		Body:                 response.Body,
//		CacheHit:             false,
//		Timestamp:            response.ExecutedAt.Unix(),
//		Executed:             true,
//		Error:                "",
//		Span:                 nil,
//		Tags:                 response.Tags,
//		XXX_NoUnkeyedLiteral: struct{}{},
//		XXX_sizecache:        0,
//	}
//	if response.Err != nil {
//		grpcResponse.Executed = false
//		grpcResponse.Error = response.Err.Error()
//	}
//	var span *trace.Span
//	if response.trace != nil {
//		ctx, span = trace.StartSpan(ctx, response.trace.Name, trace.WithSpanKind(trace.SpanKindClient))
//		defer span.End()
//		span.AddAttributes(response.trace.attributes...)
//	}
//
//	_, err := t.client.SendResponse(ctx, grpcResponse)
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
//func (t *Transport) SendQueueMessage(ctx context.Context, msg *QueueMessage) (*SendQueueMessageResult, error) {
//	if t.isClosed.Load() {
//		return nil, errConnectionClosed
//	}
//	if msg.ClientID == "" && t.opts.clientId != "" {
//		msg.ClientID = t.opts.clientId
//	}
//	result, err := t.client.SendQueueMessage(ctx, msg.QueueMessage)
//	if err != nil {
//		return nil, err
//	}
//	if result != nil {
//		return &SendQueueMessageResult{
//			MessageID:    result.MessageID,
//			SentAt:       result.SentAt,
//			ExpirationAt: result.ExpirationAt,
//			DelayedTo:    result.DelayedTo,
//			IsError:      result.IsError,
//			Error:        result.Error,
//		}, nil
//	}
//	return nil, nil
//}
//
//func (t *Transport) SendQueueMessages(ctx context.Context, msgs []*QueueMessage) ([]*SendQueueMessageResult, error) {
//	if t.isClosed.Load() {
//		return nil, errConnectionClosed
//	}
//	br := &pb.QueueMessagesBatchRequest{
//		BatchID:  nuid.New().Next(),
//		Messages: []*pb.QueueMessage{},
//	}
//
//	for _, msg := range msgs {
//		if msg.ClientID == "" && t.opts.clientId != "" {
//			msg.ClientID = t.opts.clientId
//		}
//		br.Messages = append(br.Messages, msg.QueueMessage)
//	}
//	batchResults, err := t.client.SendQueueMessagesBatch(ctx, br)
//	if err != nil {
//		return nil, err
//	}
//	if batchResults != nil {
//		var results []*SendQueueMessageResult
//		for _, result := range batchResults.Results {
//			results = append(results, &SendQueueMessageResult{
//				MessageID:    result.MessageID,
//				SentAt:       result.SentAt,
//				ExpirationAt: result.ExpirationAt,
//				DelayedTo:    result.DelayedTo,
//				IsError:      result.IsError,
//				Error:        result.Error,
//			})
//		}
//		return results, nil
//	}
//	return nil, nil
//}
//
//func (t *Transport) ReceiveQueueMessages(ctx context.Context, req *ReceiveQueueMessagesRequest) (*ReceiveQueueMessagesResponse, error) {
//	if t.isClosed.Load() {
//		return nil, errConnectionClosed
//	}
//	response, err := t.client.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
//		RequestID:           req.RequestID,
//		ClientID:            req.ClientID,
//		Channel:             req.Channel,
//		MaxNumberOfMessages: req.MaxNumberOfMessages,
//		WaitTimeSeconds:     req.WaitTimeSeconds,
//		IsPeak:              req.IsPeak,
//	})
//	if err != nil {
//		return nil, err
//	}
//	if response != nil {
//		res := &ReceiveQueueMessagesResponse{
//			RequestID:        response.RequestID,
//			Messages:         []*QueueMessage{},
//			MessagesReceived: response.MessagesReceived,
//			MessagesExpired:  response.MessagesExpired,
//			IsPeak:           response.IsPeak,
//			IsError:          response.IsError,
//			Error:            response.Error,
//		}
//		if response.Messages != nil {
//			for _, msg := range response.Messages {
//				res.Messages = append(res.Messages, &QueueMessage{
//					QueueMessage: msg,
//					transport:    t,
//					trace:        nil,
//					stream:       nil,
//				})
//			}
//		}
//		return res, nil
//	}
//	return nil, nil
//}
//
//func (t *Transport) AckAllQueueMessages(ctx context.Context, req *AckAllQueueMessagesRequest) (*AckAllQueueMessagesResponse, error) {
//	if t.isClosed.Load() {
//		return nil, errConnectionClosed
//	}
//	result, err := t.client.AckAllQueueMessages(ctx, &pb.AckAllQueueMessagesRequest{
//		RequestID:       req.RequestID,
//		ClientID:        req.ClientID,
//		Channel:         req.Channel,
//		WaitTimeSeconds: req.WaitTimeSeconds,
//	})
//	if err != nil {
//		return nil, err
//	}
//	if result != nil {
//		return &AckAllQueueMessagesResponse{
//			RequestID:        result.RequestID,
//			AffectedMessages: result.AffectedMessages,
//			IsError:          result.IsError,
//			Error:            result.Error,
//		}, nil
//	}
//
//	return nil, nil
//}
//
//func (t *Transport) StreamQueueMessage(ctx context.Context, reqCh chan *pb.StreamQueueMessagesRequest, resCh chan *pb.StreamQueueMessagesResponse, errCh chan error, doneCh chan bool) {
//	stream, err := t.client.StreamQueueMessage(ctx)
//	if err != nil {
//		errCh <- err
//		return
//	}
//	defer func() {
//		doneCh <- true
//	}()
//	go func() {
//		for {
//			res, err := stream.Recv()
//			if err != nil {
//				if err == io.EOF {
//					return
//				}
//				errCh <- err
//				return
//			}
//			select {
//			case resCh <- res:
//			case <-stream.Context().Done():
//				return
//			case <-ctx.Done():
//				return
//			}
//		}
//	}()
//
//	for {
//		select {
//		case req := <-reqCh:
//			err := stream.Send(req)
//			if err != nil {
//				if err == io.EOF {
//					return
//				}
//				errCh <- err
//				return
//			}
//		case <-stream.Context().Done():
//			return
//		case <-ctx.Done():
//			return
//		}
//	}
//}
//
//func (t *Transport) QueuesInfo(ctx context.Context, filter string) (*QueuesInfo, error) {
//	if t.isClosed.Load() {
//		return nil, errConnectionClosed
//	}
//	resp, err := t.client.QueuesInfo(ctx, &pb.QueuesInfoRequest{
//		RequestID: uuid.New(),
//		QueueName: filter,
//	})
//	if err != nil {
//		return nil, err
//	}
//	return fromQueuesInfoPb(resp.Info), nil
//}

//func (t *Transport) Close() error {
//	err := t.conn.Close()
//	if err != nil {
//		return err
//	}
//	t.isClosed.Store(true)
//	return nil
//}
