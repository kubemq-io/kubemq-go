package kubemq

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	"github.com/nats-io/nuid"
	"go.uber.org/atomic"
	"io"
	"time"

	pb "github.com/kubemq-io/protobuf/go"
	"go.opencensus.io/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	defaultMaxSendSize = 1024 * 1024 * 100 //100MB
	defaultMaxRcvSize  = 1024 * 1024 * 100 //100MB

)

var (
	errConnectionClosed = errors.New("connection closed locally")
)

type gRPCTransport struct {
	opts     *Options
	conn     *grpc.ClientConn
	client   pb.KubemqClient
	isClosed *atomic.Bool
}

func newGRPCTransport(ctx context.Context, opts *Options) (Transport, *ServerInfo, error) {
	connOptions := []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(defaultMaxRcvSize), grpc.MaxCallSendMsgSize(defaultMaxSendSize))}
	if opts.isSecured {
		if opts.certFile != "" {
			creds, err := credentials.NewClientTLSFromFile(opts.certFile, opts.serverOverrideDomain)
			if err != nil {
				return nil, nil, fmt.Errorf("could not load tls cert: %s", err)
			}
			connOptions = append(connOptions, grpc.WithTransportCredentials(creds))
		} else if opts.certData != "" {
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM([]byte(opts.certData)) {
				return nil, nil, fmt.Errorf("credentials: failed to append certificates to pool")
			}
			creds := credentials.NewClientTLSFromCert(certPool, opts.serverOverrideDomain)

			connOptions = append(connOptions, grpc.WithTransportCredentials(creds))

		} else {
			return nil, nil, fmt.Errorf("no valid tls security provided")
		}

	} else {
		connOptions = append(connOptions, grpc.WithInsecure())
	}
	address := fmt.Sprintf("%s:%d", opts.host, opts.port)
	g := &gRPCTransport{
		opts:     opts,
		conn:     nil,
		client:   nil,
		isClosed: atomic.NewBool(false),
	}
	connOptions = append(connOptions, grpc.WithUnaryInterceptor(g.SetUnaryInterceptor()), grpc.WithStreamInterceptor(g.SetStreamInterceptor()))

	var err error
	g.conn, err = grpc.DialContext(ctx, address, connOptions...)
	if err != nil {
		return nil, nil, err
	}
	go func() {

		<-ctx.Done()
		if g.conn != nil {
			_ = g.conn.Close()
		}

	}()
	g.client = pb.NewKubemqClient(g.conn)

	if g.opts.checkConnection {
		si, err := g.Ping(ctx)
		if err != nil {
			_ = g.Close()
			return nil, &ServerInfo{}, err
		}
		return g, si, nil
	} else {
		return g, &ServerInfo{}, nil
	}

}

func (g *gRPCTransport) SetUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if g.opts.authToken != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, kubeMQAuthTokenHeader, g.opts.authToken)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (g *gRPCTransport) SetStreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if g.opts.authToken != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, kubeMQAuthTokenHeader, g.opts.authToken)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}
func (g *gRPCTransport) Ping(ctx context.Context) (*ServerInfo, error) {
	res, err := g.client.Ping(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	si := &ServerInfo{
		Host:                res.Host,
		Version:             res.Version,
		ServerStartTime:     res.ServerStartTime,
		ServerUpTimeSeconds: res.ServerUpTimeSeconds,
	}
	return si, nil
}

func (g *gRPCTransport) SendEvent(ctx context.Context, event *Event) error {
	if g.isClosed.Load() {
		return errConnectionClosed
	}
	if event.ClientId == "" && g.opts.clientId != "" {
		event.ClientId = g.opts.clientId
	}
	result, err := g.client.SendEvent(ctx, &pb.Event{
		EventID:  event.Id,
		ClientID: event.ClientId,
		Channel:  event.Channel,
		Metadata: event.Metadata,
		Body:     event.Body,
		Store:    false,
		Tags:     event.Tags,
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
	quit := make(chan struct{}, 1)
	stream, err := g.client.SendEventsStream(streamCtx)
	if err != nil {
		errCh <- err
		return
	}
	defer func() {
		_ = stream.CloseSend()
	}()
	go func() {
		for {
			result, err := stream.Recv()
			if err != nil {
				errCh <- err
				quit <- struct{}{}
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
			if event.ClientId == "" && g.opts.clientId != "" {
				event.ClientId = g.opts.clientId
			}
			err := stream.Send(&pb.Event{
				EventID:  event.Id,
				ClientID: event.ClientId,
				Channel:  event.Channel,
				Metadata: event.Metadata,
				Body:     event.Body,
				Store:    false,
				Tags:     event.Tags,
			})
			if err != nil {
				errCh <- err
				return
			}
		case <-quit:
			return
		case <-ctx.Done():
			return
		}
	}

}
func (g *gRPCTransport) SubscribeToEvents(ctx context.Context, request *EventsSubscription, errCh chan error) (<-chan *Event, error) {
	eventsCh := make(chan *Event, g.opts.receiveBufferSize)
	subRequest := &pb.Subscribe{
		SubscribeTypeData: pb.Subscribe_Events,
		ClientID:          request.ClientId,
		Channel:           request.Channel,
		Group:             request.Group,
	}

	go func() {
		retries := atomic.NewUint32(0)
		for {
			internalErrCh := make(chan error, 1)
			quit := make(chan struct{}, 1)
			retries.Inc()
			go func() {
				readyCh := make(chan bool, 1)
				g.subscribeToEvents(ctx, subRequest, eventsCh, internalErrCh, readyCh)
				for {
					select {
					case <-readyCh:
						retries.Store(0)
					case <-quit:
						return
					case <-ctx.Done():
						return
					}
				}
			}()

			select {
			case err := <-internalErrCh:
				quit <- struct{}{}
				if g.isClosed.Load() {
					errCh <- errConnectionClosed
					return
				}
				if !g.opts.autoReconnect {
					errCh <- err
					return
				} else {
					if g.opts.maxReconnect == 0 || int(retries.Load()) <= g.opts.maxReconnect {
						time.Sleep(g.opts.reconnectInterval)
						continue
					}
					errCh <- fmt.Errorf("max reconnects reached, aborting")
					close(eventsCh)
					return
				}
			case <-ctx.Done():
				return
			}

		}
	}()
	return eventsCh, nil
}
func (g *gRPCTransport) subscribeToEvents(ctx context.Context, subRequest *pb.Subscribe, eventsCh chan *Event, errCh chan error, readyCh chan bool) {
	stream, err := g.client.SubscribeToEvents(ctx, subRequest)
	if err != nil {
		errCh <- err
		return
	}
	readyCh <- true
	for {
		event, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		select {
		case eventsCh <- &Event{
			Id:        event.EventID,
			Channel:   event.Channel,
			Metadata:  event.Metadata,
			Body:      event.Body,
			ClientId:  g.opts.clientId,
			Tags:      event.Tags,
			transport: nil,
		}:
		case <-ctx.Done():
			return
		}

	}
}

func (g *gRPCTransport) SendEventStore(ctx context.Context, eventStore *EventStore) (*EventStoreResult, error) {
	if eventStore.ClientId == "" && g.opts.clientId != "" {
		eventStore.ClientId = g.opts.clientId
	}
	result, err := g.client.SendEvent(ctx, &pb.Event{
		EventID:  eventStore.Id,
		ClientID: eventStore.ClientId,
		Channel:  eventStore.Channel,
		Metadata: eventStore.Metadata,
		Body:     eventStore.Body,
		Store:    true,
		Tags:     eventStore.Tags,
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
	quit := make(chan struct{}, 1)
	stream, err := g.client.SendEventsStream(streamCtx)
	if err != nil {
		errCh <- err
		return
	}
	defer func() {
		_ = stream.CloseSend()
	}()
	go func() {
		for {
			result, err := stream.Recv()
			if err != nil {
				errCh <- err
				quit <- struct{}{}
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
			select {
			case eventsResultCh <- eventResult:
			case <-quit:
				return
			case <-ctx.Done():
				return
			}

		}
	}()

	for {
		select {
		case eventStore := <-eventsCh:
			if eventStore.ClientId == "" && g.opts.clientId != "" {
				eventStore.ClientId = g.opts.clientId
			}
			err := stream.Send(&pb.Event{
				EventID:  eventStore.Id,
				ClientID: g.opts.clientId,
				Channel:  eventStore.Channel,
				Metadata: eventStore.Metadata,
				Body:     eventStore.Body,
				Store:    true,
				Tags:     eventStore.Tags,
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

func (g *gRPCTransport) SubscribeToEventsStore(ctx context.Context, request *EventsStoreSubscription, errCh chan error) (<-chan *EventStoreReceive, error) {
	eventsReceiveCh := make(chan *EventStoreReceive, g.opts.receiveBufferSize)
	subOption := subscriptionOption{}
	request.SubscriptionType.apply(&subOption)
	subRequest := &pb.Subscribe{
		SubscribeTypeData:    pb.Subscribe_EventsStore,
		ClientID:             request.ClientId,
		Channel:              request.Channel,
		Group:                request.Group,
		EventsStoreTypeData:  subOption.kind,
		EventsStoreTypeValue: subOption.value,
	}
	go func() {
		retries := atomic.NewUint32(0)
		for {
			internalErrCh := make(chan error, 1)
			quit := make(chan struct{}, 1)
			retries.Inc()
			go func() {
				readyCh := make(chan bool, 1)
				g.subscribeToEventsStore(ctx, subRequest, eventsReceiveCh, internalErrCh, readyCh)
				for {
					select {
					case <-readyCh:
						retries.Store(0)
					case <-quit:
						return
					case <-ctx.Done():
						return
					}
				}
			}()

			select {
			case err := <-internalErrCh:
				quit <- struct{}{}
				if g.isClosed.Load() {
					errCh <- errConnectionClosed
					return
				}
				if !g.opts.autoReconnect {
					errCh <- err
					return
				} else {
					if g.opts.maxReconnect == 0 || int(retries.Load()) <= g.opts.maxReconnect {
						time.Sleep(g.opts.reconnectInterval)
						continue
					}
					errCh <- fmt.Errorf("max reconnects reached, aborting")
					close(eventsReceiveCh)
					return
				}
			case <-ctx.Done():
				return
			}

		}
	}()
	return eventsReceiveCh, nil
}

func (g *gRPCTransport) subscribeToEventsStore(ctx context.Context, subRequest *pb.Subscribe, eventsCh chan *EventStoreReceive, errCh chan error, readyCh chan bool) {

	stream, err := g.client.SubscribeToEvents(ctx, subRequest)
	if err != nil {
		errCh <- err
		return
	}
	readyCh <- true
	for {
		event, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		select {
		case eventsCh <- &EventStoreReceive{
			Id:        event.EventID,
			Sequence:  event.Sequence,
			Timestamp: time.Unix(0, event.Timestamp),
			Channel:   event.Channel,
			Metadata:  event.Metadata,
			Body:      event.Body,
			ClientId:  g.opts.clientId,
			Tags:      event.Tags,
		}:
		case <-ctx.Done():
			return
		}

	}
}
func (g *gRPCTransport) SendCommand(ctx context.Context, command *Command) (*CommandResponse, error) {
	if g.isClosed.Load() {
		return nil, errConnectionClosed
	}
	if command.ClientId == "" && g.opts.clientId != "" {
		command.ClientId = g.opts.clientId
	}
	grpcRequest := &pb.Request{
		RequestID:       command.Id,
		RequestTypeData: pb.Request_Command,
		ClientID:        command.ClientId,
		Channel:         command.Channel,
		Metadata:        command.Metadata,
		Body:            command.Body,
		ReplyChannel:    "",
		Timeout:         int32(command.Timeout.Nanoseconds() / 1e6),
		CacheKey:        "",
		CacheTTL:        0,
		Span:            nil,
		Tags:            command.Tags,
	}
	var span *trace.Span
	if command.trace != nil {
		ctx, span = trace.StartSpan(ctx, command.trace.Name, trace.WithSpanKind(trace.SpanKindClient))
		defer span.End()
		span.AddAttributes(command.trace.attributes...)
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
		Tags:             grpcResponse.Tags,
	}
	return commandResponse, nil
}

func (g *gRPCTransport) SubscribeToCommands(ctx context.Context, request *CommandsSubscription, errCh chan error) (<-chan *CommandReceive, error) {
	commandsCh := make(chan *CommandReceive, g.opts.receiveBufferSize)
	subRequest := &pb.Subscribe{
		SubscribeTypeData: pb.Subscribe_Commands,
		ClientID:          request.ClientId,
		Channel:           request.Channel,
		Group:             request.Group,
	}
	go func() {
		retries := atomic.NewUint32(0)
		for {
			internalErrCh := make(chan error, 1)
			quit := make(chan struct{}, 1)
			retries.Inc()
			go func() {
				readyCh := make(chan bool, 1)
				g.subscribeToCommands(ctx, subRequest, commandsCh, internalErrCh, readyCh)
				for {
					select {
					case <-readyCh:
						retries.Store(0)
					case <-quit:
						return
					case <-ctx.Done():
						return
					}
				}
			}()

			select {
			case err := <-internalErrCh:
				quit <- struct{}{}
				if g.isClosed.Load() {
					errCh <- errConnectionClosed
					return
				}
				if !g.opts.autoReconnect {
					errCh <- err
					return
				} else {
					if g.opts.maxReconnect == 0 || int(retries.Load()) <= g.opts.maxReconnect {
						time.Sleep(g.opts.reconnectInterval)
						continue
					}
					errCh <- fmt.Errorf("max reconnects reached, aborting")
					close(commandsCh)
					return
				}
			case <-ctx.Done():
				return
			}

		}
	}()
	return commandsCh, nil
}
func (g *gRPCTransport) subscribeToCommands(ctx context.Context, subRequest *pb.Subscribe, commandsCh chan *CommandReceive, errCh chan error, readyCh chan bool) {

	stream, err := g.client.SubscribeToRequests(ctx, subRequest)
	if err != nil {
		errCh <- err
		return
	}
	readyCh <- true
	for {
		command, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		select {
		case commandsCh <- &CommandReceive{
			ClientId:   command.ClientID,
			Id:         command.RequestID,
			Channel:    command.Channel,
			Metadata:   command.Metadata,
			Body:       command.Body,
			ResponseTo: command.ReplyChannel,
			Tags:       command.Tags,
		}:
		case <-ctx.Done():
			return
		}
	}
}
func (g *gRPCTransport) SendQuery(ctx context.Context, query *Query) (*QueryResponse, error) {
	if g.isClosed.Load() {
		return nil, errConnectionClosed
	}
	if query.ClientId == "" && g.opts.clientId != "" {
		query.ClientId = g.opts.clientId
	}
	grpcRequest := &pb.Request{
		RequestID:       query.Id,
		RequestTypeData: pb.Request_Query,
		ClientID:        query.ClientId,
		Channel:         query.Channel,
		Metadata:        query.Metadata,
		Body:            query.Body,
		ReplyChannel:    "",
		Timeout:         int32(query.Timeout.Nanoseconds() / 1e6),
		CacheKey:        query.CacheKey,
		CacheTTL:        int32(query.CacheTTL.Nanoseconds() / 1e6),
		Span:            nil,
		Tags:            query.Tags,
	}
	var span *trace.Span
	if query.trace != nil {
		ctx, span = trace.StartSpan(ctx, query.trace.Name, trace.WithSpanKind(trace.SpanKindClient))
		defer span.End()
		span.AddAttributes(query.trace.attributes...)
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
		Tags:             grpcResponse.Tags,
		CacheHit:         grpcResponse.CacheHit,
		Error:            grpcResponse.Error,
	}
	return queryResponse, nil
}

func (g *gRPCTransport) SubscribeToQueries(ctx context.Context, request *QueriesSubscription, errCh chan error) (<-chan *QueryReceive, error) {
	queriesCh := make(chan *QueryReceive, g.opts.receiveBufferSize)
	subRequest := &pb.Subscribe{
		SubscribeTypeData: pb.Subscribe_Queries,
		ClientID:          request.ClientId,
		Channel:           request.Channel,
		Group:             request.Group,
	}
	go func() {

		retries := atomic.NewUint32(0)
		for {
			internalErrCh := make(chan error, 1)
			quit := make(chan struct{}, 1)
			retries.Inc()
			go func() {
				readyCh := make(chan bool, 1)
				g.subscribeToQueries(ctx, subRequest, queriesCh, internalErrCh, readyCh)
				for {
					select {
					case <-readyCh:
						retries.Store(0)
					case <-quit:
						return
					case <-ctx.Done():
						return
					}
				}
			}()

			select {
			case err := <-internalErrCh:
				quit <- struct{}{}
				if g.isClosed.Load() {
					errCh <- errConnectionClosed
					return
				}
				if !g.opts.autoReconnect {
					errCh <- err
					return
				} else {
					if g.opts.maxReconnect == 0 || int(retries.Load()) <= g.opts.maxReconnect {
						time.Sleep(g.opts.reconnectInterval)
						continue
					}
					errCh <- fmt.Errorf("max reconnects reached, aborting")
					close(queriesCh)
					return
				}
			case <-ctx.Done():
				return
			}

		}
	}()
	return queriesCh, nil
}
func (g *gRPCTransport) subscribeToQueries(ctx context.Context, subRequest *pb.Subscribe, queriesCH chan *QueryReceive, errCh chan error, readyCh chan bool) {
	stream, err := g.client.SubscribeToRequests(ctx, subRequest)
	if err != nil {
		errCh <- err
		return
	}
	readyCh <- true
	for {
		query, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		queriesCH <- &QueryReceive{
			Id:         query.RequestID,
			ClientId:   query.ClientID,
			Channel:    query.Channel,
			Metadata:   query.Metadata,
			Body:       query.Body,
			ResponseTo: query.ReplyChannel,
			Tags:       query.Tags,
		}
	}
}
func (g *gRPCTransport) SendResponse(ctx context.Context, response *Response) error {
	if g.isClosed.Load() {
		return errConnectionClosed
	}
	if response.ClientId == "" && g.opts.clientId != "" {
		response.ClientId = g.opts.clientId
	}
	grpcResponse := &pb.Response{
		ClientID:             response.ClientId,
		RequestID:            response.RequestId,
		ReplyChannel:         response.ResponseTo,
		Metadata:             response.Metadata,
		Body:                 response.Body,
		CacheHit:             false,
		Timestamp:            response.ExecutedAt.Unix(),
		Executed:             true,
		Error:                "",
		Span:                 nil,
		Tags:                 response.Tags,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	if response.Err != nil {
		grpcResponse.Executed = false
		grpcResponse.Error = response.Err.Error()
	}
	var span *trace.Span
	if response.trace != nil {
		ctx, span = trace.StartSpan(ctx, response.trace.Name, trace.WithSpanKind(trace.SpanKindClient))
		defer span.End()
		span.AddAttributes(response.trace.attributes...)
	}

	_, err := g.client.SendResponse(ctx, grpcResponse)
	if err != nil {
		return err
	}
	return nil
}

func (g *gRPCTransport) SendQueueMessage(ctx context.Context, msg *QueueMessage) (*SendQueueMessageResult, error) {
	if g.isClosed.Load() {
		return nil, errConnectionClosed
	}
	if msg.ClientID == "" && g.opts.clientId != "" {
		msg.ClientID = g.opts.clientId
	}
	result, err := g.client.SendQueueMessage(ctx, msg.QueueMessage)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return &SendQueueMessageResult{
			MessageID:    result.MessageID,
			SentAt:       result.SentAt,
			ExpirationAt: result.ExpirationAt,
			DelayedTo:    result.DelayedTo,
			IsError:      result.IsError,
			Error:        result.Error,
		}, nil
	}
	return nil, nil
}

func (g *gRPCTransport) SendQueueMessages(ctx context.Context, msgs []*QueueMessage) ([]*SendQueueMessageResult, error) {
	if g.isClosed.Load() {
		return nil, errConnectionClosed
	}
	br := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.New().Next(),
		Messages: []*pb.QueueMessage{},
	}

	for _, msg := range msgs {
		if msg.ClientID == "" && g.opts.clientId != "" {
			msg.ClientID = g.opts.clientId
		}
		br.Messages = append(br.Messages, msg.QueueMessage)
	}
	batchResults, err := g.client.SendQueueMessagesBatch(ctx, br)
	if err != nil {
		return nil, err
	}
	if batchResults != nil {
		var results []*SendQueueMessageResult
		for _, result := range batchResults.Results {
			results = append(results, &SendQueueMessageResult{
				MessageID:    result.MessageID,
				SentAt:       result.SentAt,
				ExpirationAt: result.ExpirationAt,
				DelayedTo:    result.DelayedTo,
				IsError:      result.IsError,
				Error:        result.Error,
			})
		}
		return results, nil
	}
	return nil, nil
}

func (g *gRPCTransport) ReceiveQueueMessages(ctx context.Context, req *ReceiveQueueMessagesRequest) (*ReceiveQueueMessagesResponse, error) {
	if g.isClosed.Load() {
		return nil, errConnectionClosed
	}
	response, err := g.client.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:           req.RequestID,
		ClientID:            req.ClientID,
		Channel:             req.Channel,
		MaxNumberOfMessages: req.MaxNumberOfMessages,
		WaitTimeSeconds:     req.WaitTimeSeconds,
		IsPeak:              req.IsPeak,
	})
	if err != nil {
		return nil, err
	}
	if response != nil {
		res := &ReceiveQueueMessagesResponse{
			RequestID:        response.RequestID,
			Messages:         []*QueueMessage{},
			MessagesReceived: response.MessagesReceived,
			MessagesExpired:  response.MessagesExpired,
			IsPeak:           response.IsPeak,
			IsError:          response.IsError,
			Error:            response.Error,
		}
		if response.Messages != nil {
			for _, msg := range response.Messages {
				res.Messages = append(res.Messages, &QueueMessage{
					QueueMessage: msg,
					transport:    g,
					trace:        nil,
					stream:       nil,
				})
			}

		}
		return res, nil
	}
	return nil, nil
}

func (g *gRPCTransport) AckAllQueueMessages(ctx context.Context, req *AckAllQueueMessagesRequest) (*AckAllQueueMessagesResponse, error) {
	if g.isClosed.Load() {
		return nil, errConnectionClosed
	}
	result, err := g.client.AckAllQueueMessages(ctx, &pb.AckAllQueueMessagesRequest{
		RequestID:       req.RequestID,
		ClientID:        req.ClientID,
		Channel:         req.Channel,
		WaitTimeSeconds: req.WaitTimeSeconds,
	})
	if err != nil {
		return nil, err
	}
	if result != nil {
		return &AckAllQueueMessagesResponse{
			RequestID:        result.RequestID,
			AffectedMessages: result.AffectedMessages,
			IsError:          result.IsError,
			Error:            result.Error,
		}, nil
	}

	return nil, nil
}

func (g *gRPCTransport) StreamQueueMessage(ctx context.Context, reqCh chan *pb.StreamQueueMessagesRequest, resCh chan *pb.StreamQueueMessagesResponse, errCh chan error, doneCh chan bool) {
	stream, err := g.client.StreamQueueMessage(ctx)
	if err != nil {
		errCh <- err
		return
	}
	defer func() {
		doneCh <- true
	}()
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				errCh <- err
				return
			}
			select {
			case resCh <- res:
			case <-stream.Context().Done():
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case req := <-reqCh:
			err := stream.Send(req)
			if err != nil {
				if err == io.EOF {
					return
				}
				errCh <- err
				return
			}
		case <-stream.Context().Done():
			return
		case <-ctx.Done():
			return
		}
	}

}
func (g *gRPCTransport) QueuesInfo(ctx context.Context, filter string) (*QueuesInfo, error) {
	if g.isClosed.Load() {
		return nil, errConnectionClosed
	}
	resp, err := g.client.QueuesInfo(ctx, &pb.QueuesInfoRequest{
		RequestID: uuid.New(),
		QueueName: filter,
	})
	if err != nil {
		return nil, err
	}
	return fromQueuesInfoPb(resp.Info), nil
}
func (g *gRPCTransport) Close() error {
	err := g.conn.Close()
	if err != nil {
		return err
	}
	g.isClosed.Store(true)
	return nil
}
func (g *gRPCTransport) GetGRPCRawClient() (pb.KubemqClient, error) {
	return g.client, nil
}
