package transport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testBufSize = 1024 * 1024

type inlineServer struct {
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
}

func newInlineServer() *inlineServer {
	return &inlineServer{
		sendEventFn: func(_ context.Context, e *pb.Event) (*pb.Result, error) {
			return &pb.Result{EventID: e.EventID, Sent: true}, nil
		},
		sendRequestFn: func(_ context.Context, r *pb.Request) (*pb.Response, error) {
			return &pb.Response{ClientID: r.ClientID, RequestID: r.RequestID, Executed: true}, nil
		},
		sendResponseFn: func(_ context.Context, _ *pb.Response) (*pb.Empty, error) {
			return &pb.Empty{}, nil
		},
		sendQueueMessageFn: func(_ context.Context, m *pb.QueueMessage) (*pb.SendQueueMessageResult, error) {
			return &pb.SendQueueMessageResult{MessageID: m.MessageID}, nil
		},
		sendQueueMessagesBatchFn: func(_ context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
			results := make([]*pb.SendQueueMessageResult, 0, len(req.Messages))
			for _, m := range req.Messages {
				results = append(results, &pb.SendQueueMessageResult{MessageID: m.MessageID})
			}
			return &pb.QueueMessagesBatchResponse{BatchID: req.BatchID, Results: results}, nil
		},
		receiveQueueMessagesFn: func(_ context.Context, req *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
			return &pb.ReceiveQueueMessagesResponse{RequestID: req.RequestID}, nil
		},
		ackAllQueueMessagesFn: func(_ context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
			return &pb.AckAllQueueMessagesResponse{RequestID: req.RequestID}, nil
		},
		queuesInfoFn: func(_ context.Context, _ *pb.QueuesInfoRequest) (*pb.QueuesInfoResponse, error) {
			return &pb.QueuesInfoResponse{Info: &pb.QueuesInfo{}}, nil
		},
		pingFn: func(_ context.Context, _ *pb.Empty) (*pb.PingResult, error) {
			return &pb.PingResult{Host: "test-server", Version: "test-1.0.0", ServerUpTimeSeconds: 100}, nil
		},
	}
}

func (s *inlineServer) SendEvent(ctx context.Context, e *pb.Event) (*pb.Result, error) {
	s.mu.Lock()
	fn := s.sendEventFn
	s.mu.Unlock()
	return fn(ctx, e)
}
func (s *inlineServer) SendEventsStream(_ pb.Kubemq_SendEventsStreamServer) error {
	return status.Error(codes.Unimplemented, "not implemented")
}
func (s *inlineServer) SubscribeToEvents(_ *pb.Subscribe, _ pb.Kubemq_SubscribeToEventsServer) error {
	return status.Error(codes.Unimplemented, "not implemented")
}
func (s *inlineServer) SubscribeToRequests(_ *pb.Subscribe, _ pb.Kubemq_SubscribeToRequestsServer) error {
	return status.Error(codes.Unimplemented, "not implemented")
}
func (s *inlineServer) SendRequest(ctx context.Context, r *pb.Request) (*pb.Response, error) {
	s.mu.Lock()
	fn := s.sendRequestFn
	s.mu.Unlock()
	return fn(ctx, r)
}
func (s *inlineServer) SendResponse(ctx context.Context, r *pb.Response) (*pb.Empty, error) {
	s.mu.Lock()
	fn := s.sendResponseFn
	s.mu.Unlock()
	return fn(ctx, r)
}
func (s *inlineServer) Ping(ctx context.Context, e *pb.Empty) (*pb.PingResult, error) {
	s.mu.Lock()
	fn := s.pingFn
	s.mu.Unlock()
	return fn(ctx, e)
}
func (s *inlineServer) SendQueueMessage(ctx context.Context, m *pb.QueueMessage) (*pb.SendQueueMessageResult, error) {
	s.mu.Lock()
	fn := s.sendQueueMessageFn
	s.mu.Unlock()
	return fn(ctx, m)
}
func (s *inlineServer) SendQueueMessagesBatch(ctx context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
	s.mu.Lock()
	fn := s.sendQueueMessagesBatchFn
	s.mu.Unlock()
	return fn(ctx, req)
}
func (s *inlineServer) ReceiveQueueMessages(ctx context.Context, req *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
	s.mu.Lock()
	fn := s.receiveQueueMessagesFn
	s.mu.Unlock()
	return fn(ctx, req)
}
func (s *inlineServer) StreamQueueMessage(_ pb.Kubemq_StreamQueueMessageServer) error {
	return status.Error(codes.Unimplemented, "not implemented")
}
func (s *inlineServer) AckAllQueueMessages(ctx context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
	s.mu.Lock()
	fn := s.ackAllQueueMessagesFn
	s.mu.Unlock()
	return fn(ctx, req)
}
func (s *inlineServer) QueuesDownstream(_ pb.Kubemq_QueuesDownstreamServer) error {
	return status.Error(codes.Unimplemented, "not implemented")
}
func (s *inlineServer) QueuesUpstream(_ pb.Kubemq_QueuesUpstreamServer) error {
	return status.Error(codes.Unimplemented, "not implemented")
}
func (s *inlineServer) QueuesInfo(ctx context.Context, req *pb.QueuesInfoRequest) (*pb.QueuesInfoResponse, error) {
	s.mu.Lock()
	fn := s.queuesInfoFn
	s.mu.Unlock()
	return fn(ctx, req)
}

// newTestTransport creates a grpcTransport wired to a bufconn gRPC server.
func newTestTransport(t *testing.T) (*grpcTransport, *inlineServer) {
	t.Helper()

	impl := newInlineServer()
	lis := bufconn.Listen(testBufSize)
	srv := grpc.NewServer()
	pb.RegisterKubemqServer(srv, impl)

	go func() {
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, nil)
	sm.transition(types.StateConnecting)
	sm.transition(types.StateReady)

	gt := &grpcTransport{
		cfg:          Config{ClientID: "test-client"},
		stateMachine: sm,
		logger:       log,
		drainTimeout: 5 * time.Second,
		dispatcher:   newCallbackDispatcher(1, log),
	}

	gt.conn.Store(conn)
	gt.client.Store(pb.NewKubemqClient(conn))

	t.Cleanup(func() {
		_ = conn.Close()
		srv.GracefulStop()
	})

	return gt, impl
}

func TestGRPCTransport_SendEventStore(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendEventFn = func(_ context.Context, event *pb.Event) (*pb.Result, error) {
		return &pb.Result{EventID: event.EventID, Sent: true}, nil
	}
	impl.mu.Unlock()

	result, err := gt.SendEventStore(context.Background(), &SendEventStoreRequest{
		ID:      "es-1",
		Channel: "events-store.test",
		Body:    []byte("hello"),
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Sent)
	assert.Equal(t, "es-1", result.ID)
}

func TestGRPCTransport_SendEventStore_Error(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendEventFn = func(_ context.Context, _ *pb.Event) (*pb.Result, error) {
		return nil, fmt.Errorf("store error")
	}
	impl.mu.Unlock()

	result, err := gt.SendEventStore(context.Background(), &SendEventStoreRequest{
		ID:      "es-2",
		Channel: "events-store.test",
	})
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "send event store failed")
}

func TestGRPCTransport_SendCommand(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, req *pb.Request) (*pb.Response, error) {
		return &pb.Response{
			ClientID:  req.ClientID,
			RequestID: req.RequestID,
			Executed:  true,
		}, nil
	}
	impl.mu.Unlock()

	result, err := gt.SendCommand(context.Background(), &SendCommandRequest{
		ID:      "cmd-1",
		Channel: "commands.test",
		Timeout: 5 * time.Second,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Executed)
	assert.Equal(t, "cmd-1", result.CommandID)
}

func TestGRPCTransport_SendCommand_Error(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return nil, fmt.Errorf("command error")
	}
	impl.mu.Unlock()

	result, err := gt.SendCommand(context.Background(), &SendCommandRequest{
		ID:      "cmd-2",
		Channel: "commands.test",
		Timeout: 5 * time.Second,
	})
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "send command failed")
}

func TestGRPCTransport_SendQuery(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, req *pb.Request) (*pb.Response, error) {
		return &pb.Response{
			ClientID:  req.ClientID,
			RequestID: req.RequestID,
			Executed:  true,
			Body:      []byte("query result"),
			Metadata:  "meta",
		}, nil
	}
	impl.mu.Unlock()

	result, err := gt.SendQuery(context.Background(), &SendQueryRequest{
		ID:      "q-1",
		Channel: "queries.test",
		Timeout: 5 * time.Second,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Executed)
	assert.Equal(t, "q-1", result.QueryID)
	assert.Equal(t, []byte("query result"), result.Body)
	assert.Equal(t, "meta", result.Metadata)
}

func TestGRPCTransport_SendQuery_Error(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return nil, fmt.Errorf("query error")
	}
	impl.mu.Unlock()

	result, err := gt.SendQuery(context.Background(), &SendQueryRequest{
		ID:      "q-2",
		Channel: "queries.test",
		Timeout: 5 * time.Second,
	})
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "send query failed")
}

func TestGRPCTransport_SendResponse(t *testing.T) {
	gt, _ := newTestTransport(t)

	err := gt.SendResponse(context.Background(), &SendResponseRequest{
		RequestID:  "req-1",
		ResponseTo: "reply-channel",
		Metadata:   "ok",
		Body:       []byte("response body"),
		ClientID:   "responder",
		ExecutedAt: time.Now(),
	})
	require.NoError(t, err)
}

func TestGRPCTransport_SendResponse_Error(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendResponseFn = func(_ context.Context, _ *pb.Response) (*pb.Empty, error) {
		return nil, fmt.Errorf("response error")
	}
	impl.mu.Unlock()

	err := gt.SendResponse(context.Background(), &SendResponseRequest{
		RequestID:  "req-2",
		ResponseTo: "reply-channel",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "send response failed")
}

func TestGRPCTransport_ReceiveQueueMessages(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.receiveQueueMessagesFn = func(_ context.Context, req *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
		return &pb.ReceiveQueueMessagesResponse{
			RequestID: req.RequestID,
			Messages: []*pb.QueueMessage{
				{MessageID: "m1", Channel: req.Channel, Body: []byte("msg1")},
			},
			MessagesReceived: 1,
			IsPeak:           req.IsPeak,
		}, nil
	}
	impl.mu.Unlock()

	result, err := gt.ReceiveQueueMessages(context.Background(), &ReceiveQueueMessagesReq{
		RequestID:           "recv-1",
		Channel:             "queues.test",
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     1,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int32(1), result.MessagesReceived)
	require.Len(t, result.Messages, 1)
	assert.Equal(t, "m1", result.Messages[0].ID)
}

func TestGRPCTransport_ReceiveQueueMessages_Error(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.receiveQueueMessagesFn = func(_ context.Context, _ *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
		return nil, fmt.Errorf("receive error")
	}
	impl.mu.Unlock()

	result, err := gt.ReceiveQueueMessages(context.Background(), &ReceiveQueueMessagesReq{
		RequestID: "recv-2",
		Channel:   "queues.test",
	})
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "receive queue messages failed")
}

func TestGRPCTransport_AckAllQueueMessages(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.ackAllQueueMessagesFn = func(_ context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
		return &pb.AckAllQueueMessagesResponse{
			RequestID:        req.RequestID,
			AffectedMessages: 5,
			IsError:          false,
		}, nil
	}
	impl.mu.Unlock()

	result, err := gt.AckAllQueueMessages(context.Background(), &AckAllQueueMessagesReq{
		RequestID:       "ack-1",
		Channel:         "queues.test",
		WaitTimeSeconds: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, uint64(5), result.AffectedMessages)
	assert.False(t, result.IsError)
}

func TestGRPCTransport_AckAllQueueMessages_Error(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.ackAllQueueMessagesFn = func(_ context.Context, _ *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
		return nil, fmt.Errorf("ack error")
	}
	impl.mu.Unlock()

	result, err := gt.AckAllQueueMessages(context.Background(), &AckAllQueueMessagesReq{
		RequestID: "ack-2",
		Channel:   "queues.test",
	})
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "ack all queue messages failed")
}

func TestGRPCTransport_QueuesInfo(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.queuesInfoFn = func(_ context.Context, _ *pb.QueuesInfoRequest) (*pb.QueuesInfoResponse, error) {
		return &pb.QueuesInfoResponse{
			Info: &pb.QueuesInfo{
				TotalQueue: 2,
				Sent:       100,
				Delivered:  80,
				Waiting:    20,
				Queues: []*pb.QueueInfo{
					{
						Name:          "q1",
						Messages:      50,
						Bytes:         1024,
						FirstSequence: 1,
						LastSequence:  50,
						Sent:          50,
						Delivered:     40,
						Waiting:       10,
						Subscribers:   2,
					},
					{
						Name:          "q2",
						Messages:      50,
						Bytes:         2048,
						FirstSequence: 1,
						LastSequence:  50,
						Sent:          50,
						Delivered:     40,
						Waiting:       10,
						Subscribers:   1,
					},
				},
			},
		}, nil
	}
	impl.mu.Unlock()

	result, err := gt.QueuesInfo(context.Background(), "q*")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int32(2), result.TotalQueue)
	assert.Equal(t, int64(100), result.Sent)
	assert.Equal(t, int64(80), result.Delivered)
	assert.Equal(t, int64(20), result.Waiting)
	require.Len(t, result.Queues, 2)
	assert.Equal(t, "q1", result.Queues[0].Name)
	assert.Equal(t, int64(1), result.Queues[0].FirstSeq)
	assert.Equal(t, int64(50), result.Queues[0].LastSeq)
	assert.Equal(t, "q2", result.Queues[1].Name)
}

func TestGRPCTransport_QueuesInfo_Error(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.queuesInfoFn = func(_ context.Context, _ *pb.QueuesInfoRequest) (*pb.QueuesInfoResponse, error) {
		return nil, fmt.Errorf("info error")
	}
	impl.mu.Unlock()

	result, err := gt.QueuesInfo(context.Background(), "")
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "queues info failed")
}

func TestGRPCTransport_QueuesInfo_NilInfo(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.queuesInfoFn = func(_ context.Context, _ *pb.QueuesInfoRequest) (*pb.QueuesInfoResponse, error) {
		return &pb.QueuesInfoResponse{Info: nil}, nil
	}
	impl.mu.Unlock()

	result, err := gt.QueuesInfo(context.Background(), "")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int32(0), result.TotalQueue)
}

func TestGRPCTransport_SendQueueMessages(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendQueueMessagesBatchFn = func(_ context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
		results := make([]*pb.SendQueueMessageResult, 0, len(req.Messages))
		for _, m := range req.Messages {
			results = append(results, &pb.SendQueueMessageResult{
				MessageID: m.MessageID,
				IsError:   false,
				SentAt:    1000,
			})
		}
		return &pb.QueueMessagesBatchResponse{BatchID: req.BatchID, Results: results}, nil
	}
	impl.mu.Unlock()

	result, err := gt.SendQueueMessages(context.Background(), &SendQueueMessagesRequest{
		Messages: []*QueueMessageItem{
			{ID: "qm-1", Channel: "queues.test", Body: []byte("msg1")},
			{ID: "qm-2", Channel: "queues.test", Body: []byte("msg2")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Results, 2)
	assert.Equal(t, "qm-1", result.Results[0].MessageID)
	assert.Equal(t, "qm-2", result.Results[1].MessageID)
	assert.False(t, result.Results[0].IsError)
}

func TestGRPCTransport_SendQueueMessages_Error(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendQueueMessagesBatchFn = func(_ context.Context, _ *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
		return nil, fmt.Errorf("batch error")
	}
	impl.mu.Unlock()

	result, err := gt.SendQueueMessages(context.Background(), &SendQueueMessagesRequest{
		Messages: []*QueueMessageItem{
			{ID: "qm-3", Channel: "queues.test"},
		},
	})
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "send queue messages failed")
}

func TestGRPCTransport_Closed_RejectsOperations(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)

	_, err := gt.SendEventStore(context.Background(), &SendEventStoreRequest{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client closed")

	_, err = gt.SendCommand(context.Background(), &SendCommandRequest{})
	assert.Error(t, err)

	_, err = gt.SendQuery(context.Background(), &SendQueryRequest{})
	assert.Error(t, err)

	err = gt.SendResponse(context.Background(), &SendResponseRequest{})
	assert.Error(t, err)

	_, err = gt.AckAllQueueMessages(context.Background(), &AckAllQueueMessagesReq{})
	assert.Error(t, err)

	_, err = gt.QueuesInfo(context.Background(), "")
	assert.Error(t, err)

	_, err = gt.ReceiveQueueMessages(context.Background(), &ReceiveQueueMessagesReq{})
	assert.Error(t, err)

	_, err = gt.SendQueueMessages(context.Background(), &SendQueueMessagesRequest{})
	assert.Error(t, err)
}

func TestGRPCTransport_Ping(t *testing.T) {
	gt, _ := newTestTransport(t)

	info, err := gt.Ping(context.Background())
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "test-server", info.Host)
	assert.Equal(t, "test-1.0.0", info.Version)
}

func TestGRPCTransport_SendEvent(t *testing.T) {
	gt, _ := newTestTransport(t)

	err := gt.SendEvent(context.Background(), &SendEventRequest{
		ID:      "ev-1",
		Channel: "events.test",
		Body:    []byte("payload"),
	})
	require.NoError(t, err)
}
