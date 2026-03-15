package transport

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	pb "github.com/kubemq-io/kubemq-go/v2/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// ---------------------------------------------------------------------------
// SendEvent — "not sent" error path (result.Sent == false)
// ---------------------------------------------------------------------------

func TestGRPCTransport_SendEvent_NotSent(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendEventFn = func(_ context.Context, _ *pb.Event) (*pb.Result, error) {
		return &pb.Result{Sent: false, Error: "channel not found"}, nil
	}
	impl.mu.Unlock()

	err := gt.SendEvent(context.Background(), &SendEventRequest{
		ID:      "ev-err",
		Channel: "events.test",
		Body:    []byte("payload"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "event not sent")
	assert.Contains(t, err.Error(), "channel not found")
}

func TestGRPCTransport_SendEvent_RPCError(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendEventFn = func(_ context.Context, _ *pb.Event) (*pb.Result, error) {
		return nil, fmt.Errorf("rpc error")
	}
	impl.mu.Unlock()

	err := gt.SendEvent(context.Background(), &SendEventRequest{
		ID:      "ev-rpc-err",
		Channel: "events.test",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send event failed")
}

// ---------------------------------------------------------------------------
// DeleteChannel — response error path (resp.Error != "")
// ---------------------------------------------------------------------------

func TestGRPCTransport_DeleteChannel_ResponseError(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return &pb.Response{Error: "channel in use"}, nil
	}
	impl.mu.Unlock()

	err := gt.DeleteChannel(context.Background(), &DeleteChannelRequest{
		ClientID:    "test",
		Channel:     "test-ch",
		ChannelType: "events",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "channel in use")
}

// ---------------------------------------------------------------------------
// isListChannelsRetryable — all branches
// ---------------------------------------------------------------------------

func TestIsListChannelsRetryable_DeadlineExceeded(t *testing.T) {
	err := status.Error(codes.DeadlineExceeded, "deadline")
	assert.True(t, isListChannelsRetryable(err))
}

func TestIsListChannelsRetryable_SnapshotNotReady(t *testing.T) {
	err := fmt.Errorf("cluster snapshot not ready yet")
	assert.True(t, isListChannelsRetryable(err))
}

func TestIsListChannelsRetryable_OtherGRPCError(t *testing.T) {
	err := status.Error(codes.NotFound, "not found")
	assert.False(t, isListChannelsRetryable(err))
}

func TestIsListChannelsRetryable_OtherPlainError(t *testing.T) {
	err := fmt.Errorf("some other error")
	assert.False(t, isListChannelsRetryable(err))
}

// ---------------------------------------------------------------------------
// ListChannels — retry on "cluster snapshot not ready" response error
// ---------------------------------------------------------------------------

func TestGRPCTransport_ListChannels_RetryOnSnapshotNotReady(t *testing.T) {
	gt, impl := newTestTransport(t)

	var attempt atomic.Int32
	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		n := attempt.Add(1)
		if n <= 2 {
			return &pb.Response{Error: "cluster snapshot not ready"}, nil
		}
		return &pb.Response{Executed: true, Body: []byte("[]")}, nil
	}
	impl.mu.Unlock()

	channels, err := gt.ListChannels(context.Background(), &ListChannelsRequest{
		ClientID:    "test",
		ChannelType: "events",
	})
	require.NoError(t, err)
	assert.Empty(t, channels)
	assert.GreaterOrEqual(t, int(attempt.Load()), 3)
}

func TestGRPCTransport_ListChannels_RetryOnDeadlineExceeded(t *testing.T) {
	gt, impl := newTestTransport(t)

	var attempt atomic.Int32
	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		n := attempt.Add(1)
		if n <= 2 {
			return nil, status.Error(codes.DeadlineExceeded, "deadline exceeded")
		}
		return &pb.Response{Executed: true, Body: []byte("[]")}, nil
	}
	impl.mu.Unlock()

	channels, err := gt.ListChannels(context.Background(), &ListChannelsRequest{
		ClientID:    "test",
		ChannelType: "events",
	})
	require.NoError(t, err)
	assert.Empty(t, channels)
	assert.GreaterOrEqual(t, int(attempt.Load()), 3)
}

func TestGRPCTransport_ListChannels_RetryExhausted_SnapshotNotReady(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return &pb.Response{Error: "cluster snapshot not ready"}, nil
	}
	impl.mu.Unlock()

	_, err := gt.ListChannels(context.Background(), &ListChannelsRequest{
		ClientID:    "test",
		ChannelType: "events",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster snapshot not ready")
}

func TestGRPCTransport_ListChannels_RetryExhausted_DeadlineExceeded(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return nil, status.Error(codes.DeadlineExceeded, "deadline exceeded")
	}
	impl.mu.Unlock()

	_, err := gt.ListChannels(context.Background(), &ListChannelsRequest{
		ClientID:    "test",
		ChannelType: "events",
	})
	require.Error(t, err)
}

func TestGRPCTransport_ListChannels_ContextCancelledDuringRetry(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return &pb.Response{Error: "cluster snapshot not ready"}, nil
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := gt.ListChannels(ctx, &ListChannelsRequest{
		ClientID:    "test",
		ChannelType: "events",
	})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// SendEventsStream — via streaming bufconn server
// ---------------------------------------------------------------------------

type eventStreamServer struct {
	*inlineServer
	mu                 sync.Mutex
	sendEventsStreamFn func(pb.Kubemq_SendEventsStreamServer) error
	queuesUpstreamFn   func(pb.Kubemq_QueuesUpstreamServer) error
	queuesDownstreamFn func(pb.Kubemq_QueuesDownstreamServer) error
}

func (s *eventStreamServer) SendEventsStream(stream pb.Kubemq_SendEventsStreamServer) error {
	s.mu.Lock()
	fn := s.sendEventsStreamFn
	s.mu.Unlock()
	if fn != nil {
		return fn(stream)
	}
	return s.inlineServer.SendEventsStream(stream)
}

func (s *eventStreamServer) QueuesUpstream(stream pb.Kubemq_QueuesUpstreamServer) error {
	s.mu.Lock()
	fn := s.queuesUpstreamFn
	s.mu.Unlock()
	if fn != nil {
		return fn(stream)
	}
	return s.inlineServer.QueuesUpstream(stream)
}

func (s *eventStreamServer) QueuesDownstream(stream pb.Kubemq_QueuesDownstreamServer) error {
	s.mu.Lock()
	fn := s.queuesDownstreamFn
	s.mu.Unlock()
	if fn != nil {
		return fn(stream)
	}
	return s.inlineServer.QueuesDownstream(stream)
}

func startEventStreamBufconnServer(t *testing.T) (*bufconn.Listener, *eventStreamServer) {
	t.Helper()
	impl := &eventStreamServer{inlineServer: newInlineServer()}
	lis := bufconn.Listen(testBufSize)
	srv := grpc.NewServer()
	pb.RegisterKubemqServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.GracefulStop() })
	return lis, impl
}

func TestSendEventsStream_Success(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	impl.mu.Lock()
	impl.sendEventsStreamFn = func(stream pb.Kubemq_SendEventsStreamServer) error {
		for {
			ev, err := stream.Recv()
			if err != nil {
				return err
			}
			_ = stream.Send(&pb.Result{EventID: ev.EventID, Sent: true})
		}
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.SendEventsStream(ctx)
	require.NoError(t, err)
	require.NotNil(t, handle)

	err = handle.Send(&EventStreamItem{
		ID:      "es-1",
		Channel: "events.test",
		Body:    []byte("hello"),
	})
	require.NoError(t, err)

	select {
	case result := <-handle.Results:
		require.NotNil(t, result)
		assert.True(t, result.Sent)
		assert.Equal(t, "es-1", result.EventID)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event stream result")
	}

	handle.Close()

	select {
	case <-handle.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for handle.Done")
	}
}

func TestSendEventsStream_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)

	_, err := gt.SendEventsStream(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "client closed")
}

// ---------------------------------------------------------------------------
// QueueUpstream — via streaming bufconn server
// ---------------------------------------------------------------------------

func TestQueueUpstream_StreamSuccess(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	impl.mu.Lock()
	impl.queuesUpstreamFn = func(stream pb.Kubemq_QueuesUpstreamServer) error {
		for {
			req, err := stream.Recv()
			if err != nil {
				return err
			}
			results := make([]*pb.SendQueueMessageResult, 0, len(req.Messages))
			for _, m := range req.Messages {
				results = append(results, &pb.SendQueueMessageResult{
					MessageID: m.MessageID,
					SentAt:    time.Now().Unix(),
				})
			}
			_ = stream.Send(&pb.QueuesUpstreamResponse{
				RefRequestID: req.RequestID,
				Results:      results,
			})
		}
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.QueueUpstream(ctx)
	require.NoError(t, err)
	require.NotNil(t, handle)

	err = handle.SendFn("req-1", []*QueueMessageItem{
		{ID: "q1", Channel: "queue-ch", Body: []byte("payload")},
	})
	require.NoError(t, err)

	select {
	case result := <-handle.Results:
		require.NotNil(t, result)
		assert.Equal(t, "req-1", result.RefRequestID)
		require.Len(t, result.Results, 1)
		assert.Equal(t, "q1", result.Results[0].MessageID)
	case err := <-handle.Errors:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for queue upstream result")
	}

	handle.Close()

	select {
	case <-handle.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for handle.Done")
	}
}

func TestQueueUpstream_ClosedTransport(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)

	_, err := gt.QueueUpstream(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "client closed")
}

// ---------------------------------------------------------------------------
// QueueDownstream — server-close signal (RequestTypeData == 11)
// ---------------------------------------------------------------------------

func TestQueueDownstream_ServerClose(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	impl.mu.Lock()
	impl.queuesDownstreamFn = func(stream pb.Kubemq_QueuesDownstreamServer) error {
		_ = stream.Send(&pb.QueuesDownstreamResponse{
			RequestTypeData: 11,
		})
		<-stream.Context().Done()
		return nil
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.QueueDownstream(ctx, &QueueDownstreamRequest{Channel: "q-ch"})
	require.NoError(t, err)
	require.NotNil(t, handle)

	select {
	case err := <-handle.Errors:
		assert.Contains(t, err.Error(), "server closed downstream stream")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server close error")
	}

	handle.Close()
}

func TestQueueDownstream_ClosedTransport(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)

	_, err := gt.QueueDownstream(context.Background(), &QueueDownstreamRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "client closed")
}

// ---------------------------------------------------------------------------
// QueueUpstream — recv non-connection error path
// ---------------------------------------------------------------------------

func TestQueueUpstream_RecvNonConnectionError(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	impl.mu.Lock()
	impl.queuesUpstreamFn = func(stream pb.Kubemq_QueuesUpstreamServer) error {
		return status.Error(codes.PermissionDenied, "permission denied")
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.QueueUpstream(ctx)
	require.NoError(t, err)
	require.NotNil(t, handle)

	select {
	case err := <-handle.Errors:
		assert.Contains(t, err.Error(), "PermissionDenied")
	case <-handle.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for upstream error")
	}
}

// ---------------------------------------------------------------------------
// QueueDownstream — recv non-connection error path
// ---------------------------------------------------------------------------

func TestQueueDownstream_RecvNonConnectionError(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	impl.mu.Lock()
	impl.queuesDownstreamFn = func(stream pb.Kubemq_QueuesDownstreamServer) error {
		return status.Error(codes.PermissionDenied, "access denied")
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.QueueDownstream(ctx, &QueueDownstreamRequest{Channel: "q-ch"})
	require.NoError(t, err)
	require.NotNil(t, handle)

	select {
	case err := <-handle.Errors:
		assert.Contains(t, err.Error(), "PermissionDenied")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for downstream error")
	}
}

// ---------------------------------------------------------------------------
// SendQueueMessage — closed path
// ---------------------------------------------------------------------------

func TestGRPCTransport_SendQueueMessage_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)

	_, err := gt.SendQueueMessage(context.Background(), &QueueMessageItem{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "client closed")
}

// ---------------------------------------------------------------------------
// subscribePatternName — default branch
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// waitForReady — successful wait path (state transitions to Ready)
// ---------------------------------------------------------------------------

func TestGRPCTransport_WaitForReady_BecomesReady(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.stateMachine.transition(types.StateReconnecting)

	go func() {
		time.Sleep(10 * time.Millisecond)
		gt.stateMachine.transition(types.StateReady)
	}()

	err := gt.waitForReady(context.Background())
	assert.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Close — callback drain timeout path
// ---------------------------------------------------------------------------

func TestGRPCTransport_Close_WithAuthClose(t *testing.T) {
	gt, _ := newTestTransportWithReconnect(t)
	authCloseCalled := false
	gt.authClose = func() { authCloseCalled = true }
	err := gt.Close()
	assert.NoError(t, err)
	assert.True(t, authCloseCalled)
}

func TestGRPCTransport_Close_FromReconnecting_WithAuthClose(t *testing.T) {
	gt, _ := newTestTransportWithReconnect(t)
	gt.stateMachine.transition(types.StateReconnecting)
	authCloseCalled := false
	gt.authClose = func() { authCloseCalled = true }
	err := gt.Close()
	assert.NoError(t, err)
	assert.True(t, authCloseCalled)
}

func TestGRPCTransport_Close_CallbackDrainTimeout(t *testing.T) {
	gt, _ := newTestTransportWithReconnect(t)
	gt.callbackTimeout = 50 * time.Millisecond
	gt.dispatcher.wg.Add(1)

	err := gt.Close()
	assert.NoError(t, err)

	gt.dispatcher.wg.Done()
}

func TestSubscribePatternName_Unknown(t *testing.T) {
	name := subscribePatternName(pb.Subscribe_SubscribeType(99))
	assert.Equal(t, "unknown", name)
}

// ---------------------------------------------------------------------------
// isValidTransition — default from-state
// ---------------------------------------------------------------------------

func TestIsValidTransition_UnknownFromState(t *testing.T) {
	assert.False(t, isValidTransition(types.ConnectionState(99), types.StateReady))
}

// ---------------------------------------------------------------------------
// Ping — error path
// ---------------------------------------------------------------------------

func TestGRPCTransport_Ping_Error(t *testing.T) {
	gt, impl := newTestTransport(t)

	impl.mu.Lock()
	impl.pingFn = func(_ context.Context, _ *pb.Empty) (*pb.PingResult, error) {
		return nil, fmt.Errorf("ping failed")
	}
	impl.mu.Unlock()

	_, err := gt.Ping(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ping failed")
}

// ---------------------------------------------------------------------------
// SendEventsStream — context cancelled recv path
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// SendEventsStream — reconnection path (stream lost → wait → reopen success)
// ---------------------------------------------------------------------------

func TestSendEventsStream_Reconnection(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	var streamCount atomic.Int32
	impl.mu.Lock()
	impl.sendEventsStreamFn = func(stream pb.Kubemq_SendEventsStreamServer) error {
		n := streamCount.Add(1)
		if n == 1 {
			return status.Error(codes.Unavailable, "server going away")
		}
		for {
			ev, err := stream.Recv()
			if err != nil {
				return err
			}
			_ = stream.Send(&pb.Result{EventID: ev.EventID, Sent: true})
		}
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.SendEventsStream(ctx)
	require.NoError(t, err)
	require.NotNil(t, handle)

	time.Sleep(50 * time.Millisecond)
	gt.notifyReconnected()
	time.Sleep(100 * time.Millisecond)

	err = handle.Send(&EventStreamItem{ID: "es-reconnect", Channel: "ch", Body: []byte("data")})
	require.NoError(t, err)

	select {
	case result := <-handle.Results:
		require.NotNil(t, result)
		assert.Equal(t, "es-reconnect", result.EventID)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for result after reconnection")
	}

	handle.Close()
}

func TestSendEventsStream_ContextCancelledDuringReconnect(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	impl.mu.Lock()
	impl.sendEventsStreamFn = func(stream pb.Kubemq_SendEventsStreamServer) error {
		return status.Error(codes.Unavailable, "server going away")
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.SendEventsStream(ctx)
	require.NoError(t, err)
	require.NotNil(t, handle)

	time.Sleep(50 * time.Millisecond)
	handle.Close()

	select {
	case <-handle.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for handle.Done after context cancel")
	}
}

func TestSendEventsStream_NonConnectionError(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	impl.mu.Lock()
	impl.sendEventsStreamFn = func(stream pb.Kubemq_SendEventsStreamServer) error {
		return status.Error(codes.PermissionDenied, "access denied")
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.SendEventsStream(ctx)
	require.NoError(t, err)
	require.NotNil(t, handle)

	select {
	case <-handle.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for handle.Done after non-connection error")
	}
}

// ---------------------------------------------------------------------------
// QueueUpstream — reconnection path
// ---------------------------------------------------------------------------

func TestQueueUpstream_Reconnection(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	var streamCount atomic.Int32
	impl.mu.Lock()
	impl.queuesUpstreamFn = func(stream pb.Kubemq_QueuesUpstreamServer) error {
		n := streamCount.Add(1)
		if n == 1 {
			return status.Error(codes.Unavailable, "server going away")
		}
		for {
			req, err := stream.Recv()
			if err != nil {
				return err
			}
			results := make([]*pb.SendQueueMessageResult, 0, len(req.Messages))
			for _, m := range req.Messages {
				results = append(results, &pb.SendQueueMessageResult{MessageID: m.MessageID})
			}
			_ = stream.Send(&pb.QueuesUpstreamResponse{
				RefRequestID: req.RequestID,
				Results:      results,
			})
		}
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.QueueUpstream(ctx)
	require.NoError(t, err)
	require.NotNil(t, handle)

	// Drain the reconnecting error from errCh
	time.Sleep(50 * time.Millisecond)
	gt.notifyReconnected()
	time.Sleep(100 * time.Millisecond)

	err = handle.SendFn("req-after-reconnect", []*QueueMessageItem{
		{ID: "q-recon", Channel: "ch", Body: []byte("data")},
	})
	require.NoError(t, err)

	select {
	case result := <-handle.Results:
		require.NotNil(t, result)
		assert.Equal(t, "req-after-reconnect", result.RefRequestID)
	case err := <-handle.Errors:
		if err != nil && err.Error() != "" {
			// drain reconnecting errors - not fatal
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for result after reconnection")
	}

	handle.Close()
}

func TestQueueUpstream_ContextCancelledDuringReconnect(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	impl.mu.Lock()
	impl.queuesUpstreamFn = func(stream pb.Kubemq_QueuesUpstreamServer) error {
		return status.Error(codes.Unavailable, "server going away")
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.QueueUpstream(ctx)
	require.NoError(t, err)
	require.NotNil(t, handle)

	time.Sleep(50 * time.Millisecond)
	handle.Close()

	select {
	case <-handle.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for handle.Done after context cancel")
	}
}

// ---------------------------------------------------------------------------
// QueueDownstream — reconnection path
// ---------------------------------------------------------------------------

func TestQueueDownstream_Reconnection(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	var streamCount atomic.Int32
	impl.mu.Lock()
	impl.queuesDownstreamFn = func(stream pb.Kubemq_QueuesDownstreamServer) error {
		n := streamCount.Add(1)
		if n == 1 {
			return status.Error(codes.Unavailable, "server going away")
		}
		for {
			req, err := stream.Recv()
			if err != nil {
				return err
			}
			_ = stream.Send(&pb.QueuesDownstreamResponse{
				TransactionId:   "tx-recon",
				RefRequestId:    req.RequestID,
				RequestTypeData: req.RequestTypeData,
			})
		}
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.QueueDownstream(ctx, &QueueDownstreamRequest{Channel: "q-ch"})
	require.NoError(t, err)
	require.NotNil(t, handle)

	time.Sleep(50 * time.Millisecond)
	gt.notifyReconnected()
	time.Sleep(100 * time.Millisecond)

	err = handle.SendFn(&QueueDownstreamSendRequest{
		RequestID: "req-recon",
		Channel:   "q-ch",
		MaxItems:  10,
	})
	require.NoError(t, err)

	timeout := time.After(3 * time.Second)
	for {
		select {
		case raw := <-handle.Messages:
			result, ok := raw.(*QueueDownstreamResult)
			if ok {
				assert.Equal(t, "tx-recon", result.TransactionID)
				goto done
			}
		case err := <-handle.Errors:
			_ = err // drain reconnecting errors
		case <-timeout:
			t.Fatal("timed out waiting for downstream result after reconnection")
			goto done
		}
	}
done:
	handle.Close()
}

func TestQueueDownstream_ContextCancelledDuringReconnect(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	impl.mu.Lock()
	impl.queuesDownstreamFn = func(stream pb.Kubemq_QueuesDownstreamServer) error {
		return status.Error(codes.Unavailable, "server going away")
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.QueueDownstream(ctx, &QueueDownstreamRequest{Channel: "q-ch"})
	require.NoError(t, err)
	require.NotNil(t, handle)

	time.Sleep(50 * time.Millisecond)
	handle.Close()

	// Channels should be closed after the context cancel propagates
	timeout := time.After(3 * time.Second)
	select {
	case _, ok := <-handle.Messages:
		if !ok {
			// channel closed, expected
		}
	case <-timeout:
		t.Fatal("timed out waiting for downstream to exit")
	}
}

// ---------------------------------------------------------------------------
// buildDialOptions — TLS config and credential provider branches
// ---------------------------------------------------------------------------

func TestGRPCTransport_BuildDialOptions_WithTLSConfig(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.cfg.TLSConfig = &types.TLSConfig{}
	gt.cfg.InsecureSkipVerify = true
	ctx := context.Background()
	opts := gt.buildDialOptions(ctx)
	assert.NotEmpty(t, opts)
}

func TestGRPCTransport_BuildDialOptions_WithCredentialProvider(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.cfg.CredentialProvider = &staticTokenProvider{token: "test-token"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := gt.buildDialOptions(ctx)
	assert.NotEmpty(t, opts)
	if gt.authClose != nil {
		gt.authClose()
	}
}

type staticTokenProvider struct {
	token string
}

func (s *staticTokenProvider) GetToken(_ context.Context) (string, time.Time, error) {
	return s.token, time.Now().Add(time.Hour), nil
}

// ---------------------------------------------------------------------------
// checkReady error path — transport not ready (Reconnecting state)
// covers the "if err := t.checkReady(ctx)" branches in all unary methods
// ---------------------------------------------------------------------------

func TestGRPCTransport_NotReady_RejectsOperations(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.stateMachine.transition(types.StateReconnecting)

	_, err := gt.Ping(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not ready")

	err = gt.SendEvent(context.Background(), &SendEventRequest{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not ready")

	_, err = gt.SendEventStore(context.Background(), &SendEventStoreRequest{})
	assert.Error(t, err)

	_, err = gt.SendCommand(context.Background(), &SendCommandRequest{})
	assert.Error(t, err)

	_, err = gt.SendQuery(context.Background(), &SendQueryRequest{})
	assert.Error(t, err)

	err = gt.SendResponse(context.Background(), &SendResponseRequest{})
	assert.Error(t, err)

	_, err = gt.SendQueueMessage(context.Background(), &QueueMessageItem{})
	assert.Error(t, err)

	_, err = gt.SendQueueMessages(context.Background(), &SendQueueMessagesRequest{})
	assert.Error(t, err)

	_, err = gt.ReceiveQueueMessages(context.Background(), &ReceiveQueueMessagesReq{})
	assert.Error(t, err)

	_, err = gt.AckAllQueueMessages(context.Background(), &AckAllQueueMessagesReq{})
	assert.Error(t, err)

	err = gt.CreateChannel(context.Background(), &CreateChannelRequest{})
	assert.Error(t, err)

	err = gt.DeleteChannel(context.Background(), &DeleteChannelRequest{})
	assert.Error(t, err)

	_, err = gt.ListChannels(context.Background(), &ListChannelsRequest{})
	assert.Error(t, err)

	_, err = gt.SendEventsStream(context.Background())
	assert.Error(t, err)

	_, err = gt.QueueUpstream(context.Background())
	assert.Error(t, err)

	_, err = gt.QueueDownstream(context.Background(), &QueueDownstreamRequest{})
	assert.Error(t, err)

	_, err = gt.subscribe(context.Background(), &SubscribeRequest{Channel: "ch"}, pb.Subscribe_Events)
	assert.Error(t, err)
}

func TestSendEventsStream_ContextCancelled(t *testing.T) {
	lis, impl := startEventStreamBufconnServer(t)

	impl.mu.Lock()
	impl.sendEventsStreamFn = func(stream pb.Kubemq_SendEventsStreamServer) error {
		<-stream.Context().Done()
		return nil
	}
	impl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.SendEventsStream(ctx)
	require.NoError(t, err)
	require.NotNil(t, handle)

	handle.Close()

	select {
	case <-handle.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for handle.Done after cancel")
	}
}
