package testutil

import (
	"context"
	"net"
	"sync"
	"testing"

	pb "github.com/kubemq-io/protobuf/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// TestServer is an in-memory gRPC server for unit testing.
// It records all received requests and allows configuring per-method responses.
type TestServer struct {
	listener *bufconn.Listener
	server   *grpc.Server
	impl     *mockKubemqService

	mu     sync.Mutex
	closed bool
}

// NewTestServer creates and starts a bufconn-based gRPC test server.
// The server is automatically stopped when the test completes.
func NewTestServer(t *testing.T) *TestServer {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	impl := &mockKubemqService{
		sendEventFn:              defaultSendEvent,
		sendRequestFn:            defaultSendRequest,
		sendResponseFn:           defaultSendResponse,
		sendQueueMessageFn:       defaultSendQueueMessage,
		sendQueueMessagesBatchFn: defaultSendQueueMessagesBatch,
		receiveQueueMessagesFn:   defaultReceiveQueueMessages,
		ackAllQueueMessagesFn:    defaultAckAllQueueMessages,
		queuesInfoFn:             defaultQueuesInfo,
		pingFn:                   defaultPing,
	}
	pb.RegisterKubemqServer(srv, impl)

	ts := &TestServer{
		listener: lis,
		server:   srv,
		impl:     impl,
	}

	go func() {
		if err := srv.Serve(lis); err != nil && !ts.isClosed() {
			t.Logf("bufconn server error: %v", err)
		}
	}()

	t.Cleanup(func() {
		ts.mu.Lock()
		ts.closed = true
		ts.mu.Unlock()
		srv.GracefulStop()
	})

	return ts
}

func (ts *TestServer) isClosed() bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.closed
}

// Dial returns a gRPC client connection to the in-memory server.
func (ts *TestServer) Dial() (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return ts.listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

// SetSendEventHandler overrides the server's SendEvent behavior.
func (ts *TestServer) SetSendEventHandler(fn func(ctx context.Context, event *pb.Event) (*pb.Result, error)) {
	ts.impl.mu.Lock()
	defer ts.impl.mu.Unlock()
	ts.impl.sendEventFn = fn
}

// SetSendRequestHandler overrides the server's SendRequest behavior.
func (ts *TestServer) SetSendRequestHandler(fn func(ctx context.Context, req *pb.Request) (*pb.Response, error)) {
	ts.impl.mu.Lock()
	defer ts.impl.mu.Unlock()
	ts.impl.sendRequestFn = fn
}

// SetPingHandler overrides the server's Ping behavior.
func (ts *TestServer) SetPingHandler(fn func(ctx context.Context, empty *pb.Empty) (*pb.PingResult, error)) {
	ts.impl.mu.Lock()
	defer ts.impl.mu.Unlock()
	ts.impl.pingFn = fn
}

// SetSendQueueMessageHandler overrides the server's queue send behavior.
func (ts *TestServer) SetSendQueueMessageHandler(fn func(ctx context.Context, msg *pb.QueueMessage) (*pb.SendQueueMessageResult, error)) {
	ts.impl.mu.Lock()
	defer ts.impl.mu.Unlock()
	ts.impl.sendQueueMessageFn = fn
}

// SetSendResponseHandler overrides the server's SendResponse behavior.
func (ts *TestServer) SetSendResponseHandler(fn func(ctx context.Context, resp *pb.Response) (*pb.Empty, error)) {
	ts.impl.mu.Lock()
	defer ts.impl.mu.Unlock()
	ts.impl.sendResponseFn = fn
}

// SetSendQueueMessagesBatchHandler overrides the server's SendQueueMessagesBatch behavior.
func (ts *TestServer) SetSendQueueMessagesBatchHandler(fn func(ctx context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error)) {
	ts.impl.mu.Lock()
	defer ts.impl.mu.Unlock()
	ts.impl.sendQueueMessagesBatchFn = fn
}

// SetReceiveQueueMessagesHandler overrides the server's ReceiveQueueMessages behavior.
func (ts *TestServer) SetReceiveQueueMessagesHandler(fn func(ctx context.Context, req *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error)) {
	ts.impl.mu.Lock()
	defer ts.impl.mu.Unlock()
	ts.impl.receiveQueueMessagesFn = fn
}

// SetAckAllQueueMessagesHandler overrides the server's AckAllQueueMessages behavior.
func (ts *TestServer) SetAckAllQueueMessagesHandler(fn func(ctx context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error)) {
	ts.impl.mu.Lock()
	defer ts.impl.mu.Unlock()
	ts.impl.ackAllQueueMessagesFn = fn
}

// SetQueuesInfoHandler overrides the server's QueuesInfo behavior.
func (ts *TestServer) SetQueuesInfoHandler(fn func(ctx context.Context, req *pb.QueuesInfoRequest) (*pb.QueuesInfoResponse, error)) {
	ts.impl.mu.Lock()
	defer ts.impl.mu.Unlock()
	ts.impl.queuesInfoFn = fn
}

// FailWith configures the server to return a gRPC error for all operations.
func (ts *TestServer) FailWith(code codes.Code, msg string) {
	err := status.Error(code, msg)
	ts.SetSendEventHandler(func(_ context.Context, _ *pb.Event) (*pb.Result, error) {
		return nil, err
	})
	ts.SetSendRequestHandler(func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return nil, err
	})
	ts.SetSendResponseHandler(func(_ context.Context, _ *pb.Response) (*pb.Empty, error) {
		return nil, err
	})
	ts.SetSendQueueMessageHandler(func(_ context.Context, _ *pb.QueueMessage) (*pb.SendQueueMessageResult, error) {
		return nil, err
	})
	ts.SetSendQueueMessagesBatchHandler(func(_ context.Context, _ *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
		return nil, err
	})
	ts.SetReceiveQueueMessagesHandler(func(_ context.Context, _ *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
		return nil, err
	})
	ts.SetAckAllQueueMessagesHandler(func(_ context.Context, _ *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
		return nil, err
	})
	ts.SetQueuesInfoHandler(func(_ context.Context, _ *pb.QueuesInfoRequest) (*pb.QueuesInfoResponse, error) {
		return nil, err
	})
	ts.SetPingHandler(func(_ context.Context, _ *pb.Empty) (*pb.PingResult, error) {
		return nil, err
	})
}

// RecordedEvents returns all events received by the test server.
func (ts *TestServer) RecordedEvents() []*pb.Event {
	ts.impl.mu.Lock()
	defer ts.impl.mu.Unlock()
	events := make([]*pb.Event, len(ts.impl.recordedEvents))
	copy(events, ts.impl.recordedEvents)
	return events
}

// RecordedRequests returns all requests received by the test server.
func (ts *TestServer) RecordedRequests() []*pb.Request {
	ts.impl.mu.Lock()
	defer ts.impl.mu.Unlock()
	reqs := make([]*pb.Request, len(ts.impl.recordedRequests))
	copy(reqs, ts.impl.recordedRequests)
	return reqs
}

// mockKubemqService implements pb.KubemqServer with configurable behavior.
// The protobuf package uses gogo/protobuf (no UnimplementedKubemqServer),
// so all interface methods must be explicitly implemented.
type mockKubemqService struct {
	mu sync.Mutex

	sendEventFn              func(context.Context, *pb.Event) (*pb.Result, error)
	sendRequestFn            func(context.Context, *pb.Request) (*pb.Response, error)
	sendResponseFn           func(context.Context, *pb.Response) (*pb.Empty, error)
	sendQueueMessageFn       func(context.Context, *pb.QueueMessage) (*pb.SendQueueMessageResult, error)
	sendQueueMessagesBatchFn func(context.Context, *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error)
	receiveQueueMessagesFn   func(context.Context, *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error)
	ackAllQueueMessagesFn    func(context.Context, *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error)
	queuesInfoFn             func(context.Context, *pb.QueuesInfoRequest) (*pb.QueuesInfoResponse, error)
	pingFn                   func(context.Context, *pb.Empty) (*pb.PingResult, error)

	recordedEvents   []*pb.Event
	recordedRequests []*pb.Request
}

func (s *mockKubemqService) SendEvent(ctx context.Context, event *pb.Event) (*pb.Result, error) {
	s.mu.Lock()
	s.recordedEvents = append(s.recordedEvents, event)
	fn := s.sendEventFn
	s.mu.Unlock()
	return fn(ctx, event)
}

func (s *mockKubemqService) SendEventsStream(_ pb.Kubemq_SendEventsStreamServer) error {
	return status.Error(codes.Unimplemented, "not implemented in test server")
}

func (s *mockKubemqService) SubscribeToEvents(_ *pb.Subscribe, _ pb.Kubemq_SubscribeToEventsServer) error {
	return status.Error(codes.Unimplemented, "not implemented in test server")
}

func (s *mockKubemqService) SubscribeToRequests(_ *pb.Subscribe, _ pb.Kubemq_SubscribeToRequestsServer) error {
	return status.Error(codes.Unimplemented, "not implemented in test server")
}

func (s *mockKubemqService) SendRequest(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	s.mu.Lock()
	s.recordedRequests = append(s.recordedRequests, req)
	fn := s.sendRequestFn
	s.mu.Unlock()
	return fn(ctx, req)
}

func (s *mockKubemqService) SendResponse(ctx context.Context, resp *pb.Response) (*pb.Empty, error) {
	s.mu.Lock()
	fn := s.sendResponseFn
	s.mu.Unlock()
	return fn(ctx, resp)
}

func (s *mockKubemqService) Ping(ctx context.Context, empty *pb.Empty) (*pb.PingResult, error) {
	s.mu.Lock()
	fn := s.pingFn
	s.mu.Unlock()
	return fn(ctx, empty)
}

func (s *mockKubemqService) SendQueueMessage(ctx context.Context, msg *pb.QueueMessage) (*pb.SendQueueMessageResult, error) {
	s.mu.Lock()
	fn := s.sendQueueMessageFn
	s.mu.Unlock()
	return fn(ctx, msg)
}

func (s *mockKubemqService) SendQueueMessagesBatch(ctx context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
	s.mu.Lock()
	fn := s.sendQueueMessagesBatchFn
	s.mu.Unlock()
	return fn(ctx, req)
}

func (s *mockKubemqService) ReceiveQueueMessages(ctx context.Context, req *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
	s.mu.Lock()
	fn := s.receiveQueueMessagesFn
	s.mu.Unlock()
	return fn(ctx, req)
}

func (s *mockKubemqService) StreamQueueMessage(_ pb.Kubemq_StreamQueueMessageServer) error {
	return status.Error(codes.Unimplemented, "not implemented in test server")
}

func (s *mockKubemqService) AckAllQueueMessages(ctx context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
	s.mu.Lock()
	fn := s.ackAllQueueMessagesFn
	s.mu.Unlock()
	return fn(ctx, req)
}

func (s *mockKubemqService) QueuesDownstream(_ pb.Kubemq_QueuesDownstreamServer) error {
	return status.Error(codes.Unimplemented, "not implemented in test server")
}

func (s *mockKubemqService) QueuesUpstream(_ pb.Kubemq_QueuesUpstreamServer) error {
	return status.Error(codes.Unimplemented, "not implemented in test server")
}

func (s *mockKubemqService) QueuesInfo(ctx context.Context, req *pb.QueuesInfoRequest) (*pb.QueuesInfoResponse, error) {
	s.mu.Lock()
	fn := s.queuesInfoFn
	s.mu.Unlock()
	return fn(ctx, req)
}

func defaultSendEvent(_ context.Context, event *pb.Event) (*pb.Result, error) {
	return &pb.Result{EventID: event.EventID, Sent: true}, nil
}

func defaultSendRequest(_ context.Context, req *pb.Request) (*pb.Response, error) {
	return &pb.Response{
		ClientID:     req.ClientID,
		RequestID:    req.RequestID,
		ReplyChannel: req.ReplyChannel,
		Executed:     true,
	}, nil
}

func defaultSendResponse(_ context.Context, _ *pb.Response) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func defaultSendQueueMessage(_ context.Context, msg *pb.QueueMessage) (*pb.SendQueueMessageResult, error) {
	return &pb.SendQueueMessageResult{
		MessageID: msg.MessageID,
		IsError:   false,
	}, nil
}

func defaultPing(_ context.Context, _ *pb.Empty) (*pb.PingResult, error) {
	return &pb.PingResult{
		Host:                "test-server",
		Version:             "test-1.0.0",
		ServerStartTime:     0,
		ServerUpTimeSeconds: 100,
	}, nil
}

func defaultSendQueueMessagesBatch(_ context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
	results := make([]*pb.SendQueueMessageResult, 0, len(req.Messages))
	for _, m := range req.Messages {
		results = append(results, &pb.SendQueueMessageResult{
			MessageID: m.MessageID,
			IsError:   false,
		})
	}
	return &pb.QueueMessagesBatchResponse{
		BatchID:    req.BatchID,
		Results:    results,
		HaveErrors: false,
	}, nil
}

func defaultReceiveQueueMessages(_ context.Context, req *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
	return &pb.ReceiveQueueMessagesResponse{
		RequestID:        req.RequestID,
		Messages:         nil,
		MessagesReceived: 0,
		MessagesExpired:  0,
		IsPeak:           req.IsPeak,
		IsError:          false,
	}, nil
}

func defaultAckAllQueueMessages(_ context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
	return &pb.AckAllQueueMessagesResponse{
		RequestID:        req.RequestID,
		AffectedMessages: 0,
		IsError:          false,
	}, nil
}

func defaultQueuesInfo(_ context.Context, _ *pb.QueuesInfoRequest) (*pb.QueuesInfoResponse, error) {
	return &pb.QueuesInfoResponse{
		Info: &pb.QueuesInfo{
			TotalQueue: 0,
			Sent:       0,
			Delivered:  0,
			Waiting:    0,
			Queues:     nil,
		},
	}, nil
}
