package kubemq

import (
	"context"
	"crypto/x509"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
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

type gRPCTransport struct {
	opts   *Options
	conn   *grpc.ClientConn
	client pb.KubemqClient
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
		opts:   opts,
		conn:   nil,
		client: nil,
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

	si, err := g.Ping(ctx)
	if err != nil {
		return nil, &ServerInfo{}, err
	}

	return g, si, nil
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
			select {
			case eventsCh <- &Event{
				Id:        event.EventID,
				Channel:   event.Channel,
				Metadata:  event.Metadata,
				Body:      event.Body,
				ClientId:  "",
				Tags:      event.Tags,
				transport: nil,
			}:
			case <-ctx.Done():
				return
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
			select {
			case eventsResultCh <- eventResult:
			case <-ctx.Done():
				return
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
				Store:    true,
				Tags:     event.Tags,
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
			select {
			case eventsReceiveCh <- &EventStoreReceive{
				Id:        event.EventID,
				Sequence:  event.Sequence,
				Timestamp: time.Unix(0, event.Timestamp),
				Channel:   event.Channel,
				Metadata:  event.Metadata,
				Body:      event.Body,
				ClientId:  "",
				Tags:      event.Tags,
			}:
			case <-ctx.Done():
				return
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
			command, err := stream.Recv()
			if err != nil {
				errCh <- err
				close(commandsCh)
				return
			}
			select {
			case commandsCh <- &CommandReceive{
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
			query, err := stream.Recv()
			if err != nil {
				errCh <- err
				close(queriesCH)
				return
			}
			queriesCH <- &QueryReceive{
				Id:         query.RequestID,
				Channel:    query.Channel,
				Metadata:   query.Metadata,
				Body:       query.Body,
				ResponseTo: query.ReplyChannel,
				Tags:       query.Tags,
			}
		}
	}()
	return queriesCH, nil
}

func (g *gRPCTransport) SendResponse(ctx context.Context, response *Response) error {

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
	result, err := g.client.SendQueueMessage(ctx, &pb.QueueMessage{
		MessageID:  msg.Id,
		ClientID:   msg.ClientId,
		Channel:    msg.Channel,
		Metadata:   msg.Metadata,
		Body:       msg.Body,
		Tags:       msg.Tags,
		Attributes: &pb.QueueMessageAttributes{},
		Policy: &pb.QueueMessagePolicy{
			ExpirationSeconds: msg.Policy.ExpirationSeconds,
			DelaySeconds:      msg.Policy.DelaySeconds,
			MaxReceiveCount:   msg.Policy.MaxReceiveCount,
			MaxReceiveQueue:   msg.Policy.MaxReceiveQueue,
		},
	})
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
	br := &pb.QueueMessagesBatchRequest{
		BatchID:  uuid.New().String(),
		Messages: []*pb.QueueMessage{},
	}

	for _, msg := range msgs {
		br.Messages = append(br.Messages, &pb.QueueMessage{
			MessageID:  msg.Id,
			ClientID:   msg.ClientId,
			Channel:    msg.Channel,
			Metadata:   msg.Metadata,
			Body:       msg.Body,
			Tags:       msg.Tags,
			Attributes: &pb.QueueMessageAttributes{},
			Policy: &pb.QueueMessagePolicy{
				ExpirationSeconds: msg.Policy.ExpirationSeconds,
				DelaySeconds:      msg.Policy.DelaySeconds,
				MaxReceiveCount:   msg.Policy.MaxReceiveCount,
				MaxReceiveQueue:   msg.Policy.MaxReceiveQueue,
			},
		})
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
					Id:       msg.MessageID,
					ClientId: msg.ClientID,
					Channel:  msg.Channel,
					Metadata: msg.Metadata,
					Body:     msg.Body,
					Tags:     msg.Tags,
					Attributes: &QueueMessageAttributes{
						Timestamp:         msg.Attributes.Timestamp,
						Sequence:          msg.Attributes.Sequence,
						MD5OfBody:         msg.Attributes.MD5OfBody,
						ReceiveCount:      msg.Attributes.ReceiveCount,
						ReRouted:          msg.Attributes.ReRouted,
						ReRoutedFromQueue: msg.Attributes.ReRoutedFromQueue,
						ExpirationAt:      msg.Attributes.ExpirationAt,
						DelayedTo:         msg.Attributes.DelayedTo,
					},
					Policy: &QueueMessagePolicy{
						ExpirationSeconds: msg.Policy.ExpirationSeconds,
						DelaySeconds:      msg.Policy.DelaySeconds,
						MaxReceiveCount:   msg.Policy.MaxReceiveCount,
						MaxReceiveQueue:   msg.Policy.MaxReceiveQueue,
					},
				})
			}

		}
		return res, nil
	}
	return nil, nil
}

func (g *gRPCTransport) AckAllQueueMessages(ctx context.Context, req *AckAllQueueMessagesRequest) (*AckAllQueueMessagesResponse, error) {
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

func (g *gRPCTransport) Close() error {
	return g.conn.Close()
}
