package transport

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	pb "github.com/kubemq-io/kubemq-go/v2/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// seqRecvStream delivers a fixed sequence of (msg, err) pairs, then blocks until
// the context is cancelled. This gives tests deterministic control without races.
type seqRecvStream struct {
	items []seqItem
	idx   int
	block chan struct{}
}

type seqItem struct {
	msg interface{}
	err error
}

func newSeqRecvStream(items ...seqItem) *seqRecvStream {
	return &seqRecvStream{items: items, block: make(chan struct{})}
}

func (s *seqRecvStream) Recv() (interface{}, error) {
	if s.idx < len(s.items) {
		it := s.items[s.idx]
		s.idx++
		return it.msg, it.err
	}
	<-s.block
	return nil, io.EOF
}

func (s *seqRecvStream) unblock() {
	select {
	case <-s.block:
	default:
		close(s.block)
	}
}

// ---------------------------------------------------------------------------
// recvLoop tests — one per subType branch + error / cancel paths
// ---------------------------------------------------------------------------

func TestRecvLoop_Events(t *testing.T) {
	tr := &grpcTransport{
		cfg:    Config{ReceiveBufferSize: 10},
		subs:   &subscriptionTracker{},
		logger: noopLogger{},
	}
	subID := "ch::1"
	tr.subs.register(&subscriptionRecord{id: subID, pattern: "events", channel: "ch"})

	ev := &pb.EventReceive{EventID: "e1", Channel: "ch", Metadata: "meta", Body: []byte("body")}
	stream := newSeqRecvStream(seqItem{msg: ev})

	msgCh := make(chan any, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- tr.recvLoop(ctx, stream, pb.Subscribe_Events, subID, msgCh) }()

	select {
	case raw := <-msgCh:
		item, ok := raw.(*EventReceiveItem)
		require.True(t, ok, "expected *EventReceiveItem")
		assert.Equal(t, "e1", item.ID)
		assert.Equal(t, "ch", item.Channel)
		assert.Equal(t, "meta", item.Metadata)
		assert.Equal(t, []byte("body"), item.Body)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event message")
	}

	cancel()
	stream.unblock()
	assert.NoError(t, <-done)
}

func TestRecvLoop_EventsStore(t *testing.T) {
	tr := &grpcTransport{
		cfg:    Config{ReceiveBufferSize: 10},
		subs:   &subscriptionTracker{},
		logger: noopLogger{},
	}
	subID := "ch::3"
	tr.subs.register(&subscriptionRecord{id: subID, pattern: "events_store", channel: "ch"})

	ev := &pb.EventReceive{EventID: "es1", Channel: "ch", Sequence: 42}
	stream := newSeqRecvStream(seqItem{msg: ev})

	msgCh := make(chan any, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- tr.recvLoop(ctx, stream, pb.Subscribe_EventsStore, subID, msgCh) }()

	select {
	case raw := <-msgCh:
		item, ok := raw.(*EventStoreReceiveItem)
		require.True(t, ok, "expected *EventStoreReceiveItem")
		assert.Equal(t, "es1", item.ID)
		assert.Equal(t, uint64(42), item.Sequence)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for events_store message")
	}

	assert.Equal(t, int64(42), tr.subs.getLastSeq(subID))

	cancel()
	stream.unblock()
	assert.NoError(t, <-done)
}

func TestRecvLoop_Commands(t *testing.T) {
	tr := &grpcTransport{
		cfg:    Config{ReceiveBufferSize: 10},
		subs:   &subscriptionTracker{},
		logger: noopLogger{},
	}
	subID := "ch::4"
	tr.subs.register(&subscriptionRecord{id: subID, pattern: "commands", channel: "ch"})

	req := &pb.Request{RequestID: "cmd-1", Channel: "ch", ReplyChannel: "reply-ch", Metadata: "m", Body: []byte("b")}
	stream := newSeqRecvStream(seqItem{msg: req})

	msgCh := make(chan any, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- tr.recvLoop(ctx, stream, pb.Subscribe_Commands, subID, msgCh) }()

	select {
	case raw := <-msgCh:
		item, ok := raw.(*CommandReceiveItem)
		require.True(t, ok, "expected *CommandReceiveItem")
		assert.Equal(t, "cmd-1", item.ID)
		assert.Equal(t, "ch", item.Channel)
		assert.Equal(t, "reply-ch", item.ResponseTo)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for command message")
	}

	cancel()
	stream.unblock()
	assert.NoError(t, <-done)
}

func TestRecvLoop_Queries(t *testing.T) {
	tr := &grpcTransport{
		cfg:    Config{ReceiveBufferSize: 10},
		subs:   &subscriptionTracker{},
		logger: noopLogger{},
	}
	subID := "ch::5"
	tr.subs.register(&subscriptionRecord{id: subID, pattern: "queries", channel: "ch"})

	req := &pb.Request{RequestID: "qry-1", Channel: "ch", ReplyChannel: "reply-q", Body: []byte("q-body")}
	stream := newSeqRecvStream(seqItem{msg: req})

	msgCh := make(chan any, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- tr.recvLoop(ctx, stream, pb.Subscribe_Queries, subID, msgCh) }()

	select {
	case raw := <-msgCh:
		item, ok := raw.(*QueryReceiveItem)
		require.True(t, ok, "expected *QueryReceiveItem")
		assert.Equal(t, "qry-1", item.ID)
		assert.Equal(t, "reply-q", item.ResponseTo)
		assert.Equal(t, []byte("q-body"), item.Body)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for query message")
	}

	cancel()
	stream.unblock()
	assert.NoError(t, <-done)
}

func TestRecvLoop_ContextCancelled(t *testing.T) {
	tr := &grpcTransport{
		cfg:    Config{ReceiveBufferSize: 10},
		subs:   &subscriptionTracker{},
		logger: noopLogger{},
	}

	stream := newSeqRecvStream() // no messages — blocks immediately
	msgCh := make(chan any, 10)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- tr.recvLoop(ctx, stream, pb.Subscribe_Events, "x", msgCh) }()

	cancel()
	stream.unblock()

	select {
	case err := <-done:
		assert.NoError(t, err, "recvLoop should return nil on context cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("recvLoop did not exit after context cancel")
	}
}

func TestRecvLoop_StreamError(t *testing.T) {
	tr := &grpcTransport{
		cfg:    Config{ReceiveBufferSize: 10},
		subs:   &subscriptionTracker{},
		logger: noopLogger{},
	}

	streamErr := status.Error(codes.Unavailable, "connection lost")
	stream := newSeqRecvStream(seqItem{err: streamErr})
	msgCh := make(chan any, 10)

	err := tr.recvLoop(context.Background(), stream, pb.Subscribe_Events, "x", msgCh)
	assert.Error(t, err)
	assert.Equal(t, streamErr, err)
}

func TestRecvLoop_WrongTypeIgnored(t *testing.T) {
	tr := &grpcTransport{
		cfg:    Config{ReceiveBufferSize: 10},
		subs:   &subscriptionTracker{},
		logger: noopLogger{},
	}

	// Send a *pb.Request on an Events stream — type assertion fails, msg stays nil, skipped
	req := &pb.Request{RequestID: "wrong-type"}
	stream := newSeqRecvStream(seqItem{msg: req})

	msgCh := make(chan any, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- tr.recvLoop(ctx, stream, pb.Subscribe_Events, "x", msgCh) }()

	// Should not deliver anything; wait briefly then cancel
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, msgCh, "wrong-type message should be silently dropped")

	cancel()
	stream.unblock()
	assert.NoError(t, <-done)
}

// ---------------------------------------------------------------------------
// subscribeLoop tests
// ---------------------------------------------------------------------------

func TestSubscribeLoop_CleanExit(t *testing.T) {
	tr := &grpcTransport{
		cfg:               Config{ReceiveBufferSize: 10},
		subs:              &subscriptionTracker{},
		logger:            noopLogger{},
		reconnectNotifyCh: make(chan struct{}),
	}
	subID := "ch::1"
	tr.subs.register(&subscriptionRecord{id: subID, pattern: "events", channel: "ch"})

	// Stream returns one message then context is cancelled → recvLoop returns nil
	ev := &pb.EventReceive{EventID: "e1", Channel: "ch"}
	stream := newSeqRecvStream(seqItem{msg: ev})

	msgCh := make(chan any, 10)
	errCh := make(chan error, 8)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		tr.subscribeLoop(ctx, subID, pb.Subscribe_Events, stream, nil, msgCh, errCh)
		close(done)
	}()

	// Consume the message
	<-msgCh

	// Cancel → recvLoop returns nil → subscribeLoop exits
	cancel()
	stream.unblock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscribeLoop did not exit")
	}

	// Channels should be closed
	_, ok := <-msgCh
	assert.False(t, ok, "msgCh should be closed")
	_, ok = <-errCh
	assert.False(t, ok, "errCh should be closed")
}

func TestSubscribeLoop_NonConnectionError(t *testing.T) {
	tr := &grpcTransport{
		cfg:               Config{ReceiveBufferSize: 10},
		subs:              &subscriptionTracker{},
		logger:            noopLogger{},
		reconnectNotifyCh: make(chan struct{}),
	}
	subID := "ch::1"
	tr.subs.register(&subscriptionRecord{id: subID, pattern: "events", channel: "ch"})

	// A NotFound gRPC error is NOT a connection error
	notFoundErr := status.Error(codes.NotFound, "channel not found")
	stream := newSeqRecvStream(seqItem{err: notFoundErr})

	msgCh := make(chan any, 10)
	errCh := make(chan error, 8)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		tr.subscribeLoop(ctx, subID, pb.Subscribe_Events, stream, nil, msgCh, errCh)
		close(done)
	}()

	select {
	case err := <-errCh:
		assert.Equal(t, notFoundErr, err)
	case <-time.After(2 * time.Second):
		t.Fatal("expected error on errCh")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscribeLoop did not exit after non-connection error")
	}
}

func TestSubscribeLoop_ContextCancel(t *testing.T) {
	tr := &grpcTransport{
		cfg:               Config{ReceiveBufferSize: 10},
		subs:              &subscriptionTracker{},
		logger:            noopLogger{},
		reconnectNotifyCh: make(chan struct{}),
	}
	subID := "ch::1"
	tr.subs.register(&subscriptionRecord{id: subID, pattern: "events", channel: "ch"})

	stream := newSeqRecvStream() // blocks forever
	msgCh := make(chan any, 10)
	errCh := make(chan error, 8)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		tr.subscribeLoop(ctx, subID, pb.Subscribe_Events, stream, nil, msgCh, errCh)
		close(done)
	}()

	cancel()
	stream.unblock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscribeLoop did not exit after context cancel")
	}
}

func TestSubscribeLoop_ReconnectOpenStreamFails(t *testing.T) {
	tr := &grpcTransport{
		cfg:               Config{ReceiveBufferSize: 10},
		subs:              &subscriptionTracker{},
		logger:            noopLogger{},
		reconnectNotifyCh: make(chan struct{}),
	}
	subID := "ch::1"
	tr.subs.register(&subscriptionRecord{id: subID, pattern: "events", channel: "ch"})

	// First stream returns a connection error (Unavailable) to trigger reconnection
	connErr := status.Error(codes.Unavailable, "server down")
	stream := newSeqRecvStream(seqItem{err: connErr})

	openStream := func(_ context.Context) (recvStream, error) {
		return nil, fmt.Errorf("dial failed on reopen")
	}

	msgCh := make(chan any, 10)
	errCh := make(chan error, 8)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		tr.subscribeLoop(ctx, subID, pb.Subscribe_Events, stream, openStream, msgCh, errCh)
		close(done)
	}()

	// Fire reconnect notification so subscribeLoop tries to reopen
	time.Sleep(20 * time.Millisecond)
	tr.notifyReconnected()

	select {
	case err := <-errCh:
		assert.Contains(t, err.Error(), "subscription recovery failed")
		assert.Contains(t, err.Error(), "dial failed on reopen")
	case <-time.After(2 * time.Second):
		t.Fatal("expected error on errCh after reopen failure")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscribeLoop did not exit after reopen failure")
	}
}

// ---------------------------------------------------------------------------
// isConnectionError — comprehensive branch coverage
// ---------------------------------------------------------------------------

func TestIsConnectionError_Nil(t *testing.T) {
	assert.False(t, isConnectionError(nil))
}

func TestIsConnectionError_Unavailable(t *testing.T) {
	err := status.Error(codes.Unavailable, "server unavailable")
	assert.True(t, isConnectionError(err))
}

func TestIsConnectionError_InternalTransport(t *testing.T) {
	err := status.Error(codes.Internal, "transport is closing")
	assert.True(t, isConnectionError(err))
}

func TestIsConnectionError_InternalConnection(t *testing.T) {
	err := status.Error(codes.Internal, "connection reset by peer")
	assert.True(t, isConnectionError(err))
}

func TestIsConnectionError_InternalRSTStream(t *testing.T) {
	err := status.Error(codes.Internal, "received RST_STREAM with code 0")
	assert.True(t, isConnectionError(err))
}

func TestIsConnectionError_InternalOther(t *testing.T) {
	err := status.Error(codes.Internal, "something random happened")
	assert.False(t, isConnectionError(err))
}

func TestIsConnectionError_PlainError(t *testing.T) {
	err := fmt.Errorf("random non-gRPC error")
	assert.True(t, isConnectionError(err))
}

func TestIsConnectionError_NotFound(t *testing.T) {
	err := status.Error(codes.NotFound, "channel not found")
	assert.False(t, isConnectionError(err))
}

func TestIsConnectionError_PermissionDenied(t *testing.T) {
	err := status.Error(codes.PermissionDenied, "access denied")
	assert.False(t, isConnectionError(err))
}

func TestIsConnectionError_Canceled(t *testing.T) {
	err := status.Error(codes.Canceled, "context cancelled")
	assert.False(t, isConnectionError(err))
}

func TestIsConnectionError_DeadlineExceeded(t *testing.T) {
	err := status.Error(codes.DeadlineExceeded, "deadline exceeded")
	assert.False(t, isConnectionError(err))
}

// ---------------------------------------------------------------------------
// subscribe — branch coverage for each subType (Commands, Queries, default)
// ---------------------------------------------------------------------------

func TestSubscribeToCommands_Success(t *testing.T) {
	lis, _ := startBufconnServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.SubscribeToCommands(ctx, &SubscribeRequest{
		Channel:  "test-cmds",
		Group:    "g1",
		ClientID: "sub-client",
	})
	if err != nil {
		assert.Contains(t, err.Error(), "Unimplemented")
		return
	}
	assert.NotNil(t, handle)
	handle.Close()
}

func TestSubscribeToQueries_Success(t *testing.T) {
	lis, _ := startBufconnServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.SubscribeToQueries(ctx, &SubscribeRequest{
		Channel:  "test-queries",
		Group:    "g1",
		ClientID: "sub-client",
	})
	if err != nil {
		assert.Contains(t, err.Error(), "Unimplemented")
		return
	}
	assert.NotNil(t, handle)
	handle.Close()
}

func TestSubscribeToEventsStore_Success(t *testing.T) {
	lis, _ := startBufconnServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.SubscribeToEventsStore(ctx, &SubscribeRequest{
		Channel:          "test-store",
		ClientID:         "sub-client",
		SubscriptionType: 1,
	})
	if err != nil {
		assert.Contains(t, err.Error(), "Unimplemented")
		return
	}
	assert.NotNil(t, handle)
	handle.Close()
}

func TestSubscribe_UnknownSubType(t *testing.T) {
	lis, _ := startBufconnServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	_, err = gt.subscribe(ctx, &SubscribeRequest{Channel: "ch"}, pb.Subscribe_SubscribeType(99))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown subscribe type")
}

// ---------------------------------------------------------------------------
// QueueUpstream / QueueDownstream — exercise success paths through bufconn
// ---------------------------------------------------------------------------

func TestQueueUpstream_Success(t *testing.T) {
	lis, _ := startBufconnServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	handle, err := gt.QueueUpstream(ctx)
	if err != nil {
		assert.Contains(t, err.Error(), "Unimplemented")
		return
	}
	require.NotNil(t, handle)

	sendErr := handle.SendFn("req-1", []*QueueMessageItem{
		{ID: "q1", Channel: "queue-ch", Body: []byte("payload")},
	})
	_ = sendErr

	handle.Close()
	<-handle.Done
}

func TestQueueDownstream_Success(t *testing.T) {
	lis, _ := startBufconnServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	stream, err := gt.QueueDownstream(ctx)
	if err != nil {
		assert.Contains(t, err.Error(), "Unimplemented")
		return
	}
	require.NotNil(t, stream)
	_ = stream.CloseSend()
}

// ---------------------------------------------------------------------------
// streamingServer — extends inlineServer with working streaming methods
// ---------------------------------------------------------------------------

type streamingServer struct {
	*inlineServer

	mu                    sync.Mutex
	subscribeToRequestsFn func(*pb.Subscribe, pb.Kubemq_SubscribeToRequestsServer) error
	subscribeToEventsFn   func(*pb.Subscribe, pb.Kubemq_SubscribeToEventsServer) error
	queuesDownstreamFn    func(pb.Kubemq_QueuesDownstreamServer) error
}

func (s *streamingServer) SubscribeToRequests(sub *pb.Subscribe, stream pb.Kubemq_SubscribeToRequestsServer) error {
	s.mu.Lock()
	fn := s.subscribeToRequestsFn
	s.mu.Unlock()
	if fn != nil {
		return fn(sub, stream)
	}
	return s.inlineServer.SubscribeToRequests(sub, stream)
}

func (s *streamingServer) SubscribeToEvents(sub *pb.Subscribe, stream pb.Kubemq_SubscribeToEventsServer) error {
	s.mu.Lock()
	fn := s.subscribeToEventsFn
	s.mu.Unlock()
	if fn != nil {
		return fn(sub, stream)
	}
	return s.inlineServer.SubscribeToEvents(sub, stream)
}

func (s *streamingServer) QueuesDownstream(stream pb.Kubemq_QueuesDownstreamServer) error {
	s.mu.Lock()
	fn := s.queuesDownstreamFn
	s.mu.Unlock()
	if fn != nil {
		return fn(stream)
	}
	return s.inlineServer.QueuesDownstream(stream)
}

func startStreamingBufconnServer(t *testing.T) (*bufconn.Listener, *streamingServer) {
	t.Helper()
	impl := &streamingServer{inlineServer: newInlineServer()}
	lis := bufconn.Listen(testBufSize)
	srv := grpc.NewServer()
	pb.RegisterKubemqServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.GracefulStop() })
	return lis, impl
}

// ---------------------------------------------------------------------------
// requestStreamAdapter.Recv() — via a real streaming subscribe
// ---------------------------------------------------------------------------

func TestSubscribeToCommands_StreamRecv(t *testing.T) {
	lis, impl := startStreamingBufconnServer(t)

	impl.mu.Lock()
	impl.subscribeToRequestsFn = func(_ *pb.Subscribe, stream pb.Kubemq_SubscribeToRequestsServer) error {
		_ = stream.Send(&pb.Request{
			RequestID:    "cmd-from-server",
			Channel:      "cmd-ch",
			ReplyChannel: "reply-1",
			Body:         []byte("cmd-body"),
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

	handle, err := gt.SubscribeToCommands(ctx, &SubscribeRequest{
		Channel: "cmd-ch", ClientID: "test",
	})
	require.NoError(t, err)
	require.NotNil(t, handle)

	select {
	case raw := <-handle.Messages:
		item, ok := raw.(*CommandReceiveItem)
		require.True(t, ok)
		assert.Equal(t, "cmd-from-server", item.ID)
		assert.Equal(t, "reply-1", item.ResponseTo)
	case err := <-handle.Errors:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for command from stream")
	}

	handle.Close()
}

func TestSubscribeToQueries_StreamRecv(t *testing.T) {
	lis, impl := startStreamingBufconnServer(t)

	impl.mu.Lock()
	impl.subscribeToRequestsFn = func(_ *pb.Subscribe, stream pb.Kubemq_SubscribeToRequestsServer) error {
		_ = stream.Send(&pb.Request{
			RequestID:    "qry-from-server",
			Channel:      "qry-ch",
			ReplyChannel: "reply-q",
			Body:         []byte("qry-body"),
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

	handle, err := gt.SubscribeToQueries(ctx, &SubscribeRequest{
		Channel: "qry-ch", ClientID: "test",
	})
	require.NoError(t, err)
	require.NotNil(t, handle)

	select {
	case raw := <-handle.Messages:
		item, ok := raw.(*QueryReceiveItem)
		require.True(t, ok)
		assert.Equal(t, "qry-from-server", item.ID)
		assert.Equal(t, "reply-q", item.ResponseTo)
	case err := <-handle.Errors:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for query from stream")
	}

	handle.Close()
}

func TestSubscribeToEvents_StreamRecv(t *testing.T) {
	lis, impl := startStreamingBufconnServer(t)

	impl.mu.Lock()
	impl.subscribeToEventsFn = func(_ *pb.Subscribe, stream pb.Kubemq_SubscribeToEventsServer) error {
		_ = stream.Send(&pb.EventReceive{
			EventID:  "ev-from-server",
			Channel:  "ev-ch",
			Metadata: "md",
			Body:     []byte("ev-body"),
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

	handle, err := gt.SubscribeToEvents(ctx, &SubscribeRequest{
		Channel: "ev-ch", ClientID: "test",
	})
	require.NoError(t, err)
	require.NotNil(t, handle)

	select {
	case raw := <-handle.Messages:
		item, ok := raw.(*EventReceiveItem)
		require.True(t, ok)
		assert.Equal(t, "ev-from-server", item.ID)
	case err := <-handle.Errors:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event from stream")
	}

	handle.Close()
}

// ---------------------------------------------------------------------------
// QueueDownstream — success path via streaming server
// ---------------------------------------------------------------------------

func TestQueueDownstream_StreamRecv(t *testing.T) {
	lis, impl := startStreamingBufconnServer(t)

	impl.mu.Lock()
	impl.queuesDownstreamFn = func(stream pb.Kubemq_QueuesDownstreamServer) error {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		_ = stream.Send(&pb.QueuesDownstreamResponse{
			TransactionId: "tx-1",
			RefRequestId:  req.RequestID,
			IsError:       false,
			Messages:      []*pb.QueueMessage{{MessageID: "m1", Channel: "q-ch", Body: []byte("payload")}},
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

	stream, err := gt.QueueDownstream(ctx)
	require.NoError(t, err)
	require.NotNil(t, stream)

	sendErr := stream.Send(&pb.QueuesDownstreamRequest{
		RequestID: "req-1",
		Channel:   "q-ch",
		MaxItems:  10,
	})
	require.NoError(t, sendErr)

	resp, err := stream.Recv()
	require.NoError(t, err)
	assert.Equal(t, "tx-1", resp.TransactionId)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "m1", resp.Messages[0].MessageID)

	_ = stream.CloseSend()
}

func TestQueueDownstream_StreamIsError(t *testing.T) {
	lis, impl := startStreamingBufconnServer(t)

	impl.mu.Lock()
	impl.queuesDownstreamFn = func(stream pb.Kubemq_QueuesDownstreamServer) error {
		_, err := stream.Recv()
		if err != nil {
			return err
		}
		_ = stream.Send(&pb.QueuesDownstreamResponse{
			IsError: true,
			Error:   "queue not found",
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

	stream, err := gt.QueueDownstream(ctx)
	require.NoError(t, err)
	require.NotNil(t, stream)

	sendErr := stream.Send(&pb.QueuesDownstreamRequest{
		RequestID: "req-1",
		Channel:   "q-ch",
	})
	require.NoError(t, sendErr)

	resp, err := stream.Recv()
	require.NoError(t, err)
	assert.True(t, resp.IsError)
	assert.Contains(t, resp.Error, "queue not found")

	_ = stream.CloseSend()
}

// ---------------------------------------------------------------------------
// noopLogger — verify all methods exist and don't panic
// ---------------------------------------------------------------------------

func TestNoopLogger_AllMethods(t *testing.T) {
	var l noopLogger
	assert.NotPanics(t, func() {
		l.Info("msg")
		l.Info("msg", "key", "val", "key2", 42)
		l.Warn("warning")
		l.Warn("warning", "k", "v")
		l.Error("error")
		l.Error("error", "k", 1, "k2", true)
	})
}
