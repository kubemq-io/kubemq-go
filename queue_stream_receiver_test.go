package kubemq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	pb "github.com/kubemq-io/kubemq-go/v2/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ---------------------------------------------------------------------------
// Mock transport for receiver tests
// ---------------------------------------------------------------------------

type mockReceiverTransport struct {
	mu              sync.Mutex
	isConnErr       bool
	waitReconnectCh chan struct{}
	clientID        string

	queueDownstreamFn func(ctx context.Context) (pb.Kubemq_QueuesDownstreamClient, error)
}

func (m *mockReceiverTransport) Close() error                 { return nil }
func (m *mockReceiverTransport) State() types.ConnectionState { return types.StateReady }
func (m *mockReceiverTransport) Ping(_ context.Context) (*transport.ServerInfoResult, error) {
	return nil, nil
}
func (m *mockReceiverTransport) SendEvent(_ context.Context, _ *transport.SendEventRequest) error {
	return nil
}
func (m *mockReceiverTransport) SendEventStore(_ context.Context, _ *transport.SendEventStoreRequest) (*transport.SendEventStoreResult, error) {
	return nil, nil
}
func (m *mockReceiverTransport) SendCommand(_ context.Context, _ *transport.SendCommandRequest) (*transport.SendCommandResult, error) {
	return nil, nil
}
func (m *mockReceiverTransport) SendQuery(_ context.Context, _ *transport.SendQueryRequest) (*transport.SendQueryResult, error) {
	return nil, nil
}
func (m *mockReceiverTransport) SendResponse(_ context.Context, _ *transport.SendResponseRequest) error { //nolint:revive // interface method
	return nil
}
func (m *mockReceiverTransport) SendQueueMessage(_ context.Context, _ *transport.QueueMessageItem) (*transport.QueueSendResultItem, error) {
	return nil, nil
}
func (m *mockReceiverTransport) SendQueueMessages(_ context.Context, _ *transport.SendQueueMessagesRequest) (*transport.SendQueueMessagesResult, error) {
	return nil, nil
}
func (m *mockReceiverTransport) AckAllQueueMessages(_ context.Context, _ *transport.AckAllQueueMessagesReq) (*transport.AckAllQueueMessagesResp, error) {
	return nil, nil
}
func (m *mockReceiverTransport) SubscribeToEvents(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
	return nil, nil
}
func (m *mockReceiverTransport) SubscribeToEventsStore(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
	return nil, nil
}
func (m *mockReceiverTransport) SubscribeToCommands(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
	return nil, nil
}
func (m *mockReceiverTransport) SubscribeToQueries(_ context.Context, _ *transport.SubscribeRequest) (*transport.StreamHandle, error) {
	return nil, nil
}
func (m *mockReceiverTransport) SendEventsStream(_ context.Context) (*transport.EventStreamHandle, error) {
	return nil, nil
}
func (m *mockReceiverTransport) QueueUpstream(_ context.Context) (*transport.QueueUpstreamHandle, error) {
	return nil, nil
}
func (m *mockReceiverTransport) QueueDownstream(ctx context.Context) (pb.Kubemq_QueuesDownstreamClient, error) {
	m.mu.Lock()
	fn := m.queueDownstreamFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return nil, nil
}
func (m *mockReceiverTransport) CreateChannel(_ context.Context, _ *transport.CreateChannelRequest) error {
	return nil
}
func (m *mockReceiverTransport) DeleteChannel(_ context.Context, _ *transport.DeleteChannelRequest) error {
	return nil
}
func (m *mockReceiverTransport) ListChannels(_ context.Context, _ *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
	return nil, nil
}

func (m *mockReceiverTransport) WaitReconnect() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.waitReconnectCh
}

func (m *mockReceiverTransport) IsConnectionError(_ error) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isConnErr
}

func (m *mockReceiverTransport) ClientID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clientID
}

// ---------------------------------------------------------------------------
// Fake downstream stream for receiver tests
// ---------------------------------------------------------------------------

type fakeRecvStream struct {
	grpc.ClientStream
	ctx    context.Context
	sendFn func(req *pb.QueuesDownstreamRequest) error
	recvFn func() (*pb.QueuesDownstreamResponse, error)
}

func (f *fakeRecvStream) Send(req *pb.QueuesDownstreamRequest) error {
	if f.sendFn != nil {
		return f.sendFn(req)
	}
	return nil
}

func (f *fakeRecvStream) Recv() (*pb.QueuesDownstreamResponse, error) {
	if f.recvFn != nil {
		return f.recvFn()
	}
	return nil, errors.New("not implemented")
}

func (f *fakeRecvStream) CloseSend() error             { return nil }
func (f *fakeRecvStream) Header() (metadata.MD, error) { return nil, nil }
func (f *fakeRecvStream) Trailer() metadata.MD         { return nil }
func (f *fakeRecvStream) SendMsg(_ interface{}) error  { return nil }
func (f *fakeRecvStream) RecvMsg(_ interface{}) error  { return nil }
func (f *fakeRecvStream) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}

// ---------------------------------------------------------------------------
// Spy logger for testing Warn calls
// ---------------------------------------------------------------------------

type spyLogger struct {
	noopLogger
	mu       sync.Mutex
	warnMsgs []string
}

func (s *spyLogger) Warn(msg string, _ ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.warnMsgs = append(s.warnMsgs, msg)
}

func (s *spyLogger) getWarnMsgs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]string, len(s.warnMsgs))
	copy(cp, s.warnMsgs)
	return cp
}

// ---------------------------------------------------------------------------
// Helper: build a test receiver with full control
// ---------------------------------------------------------------------------

func newTestReceiver(t *testing.T, stream pb.Kubemq_QueuesDownstreamClient, mt *mockReceiverTransport) *QueueDownstreamReceiver {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	return &QueueDownstreamReceiver{
		transport:   mt,
		clientID:    "test-client",
		logger:      noopLogger{},
		ctx:         ctx,
		cancel:      cancel,
		stream:      stream,
		sendCh:      make(chan *pb.QueuesDownstreamRequest, 512),
		responseCh:  make(chan *pb.QueuesDownstreamResponse, 1),
		errCh:       make(chan error, 8),
		stopCh:      make(chan struct{}),
		reconnectCh: make(chan struct{}),
		openTxns:    make(map[string]struct{}),
		openStream: func() (pb.Kubemq_QueuesDownstreamClient, error) {
			return stream, nil
		},
	}
}

// waitRecvLoopDone launches recvLoop in a goroutine and returns a channel
// that closes when recvLoop returns.
func waitRecvLoopDone(r *QueueDownstreamReceiver) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		r.recvLoop()
		close(done)
	}()
	return done
}

// drainErrors reads all available errors from errCh within timeout.
func drainErrors(ch <-chan error, timeout time.Duration) []error {
	var errs []error
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return errs
			}
			errs = append(errs, e)
		case <-timer.C:
			return errs
		}
	}
}

// waitChanClosed waits for a channel to be closed or times out.
func waitChanClosed[T any](t *testing.T, ch <-chan T, timeout time.Duration) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-timer.C:
			t.Fatal("timeout waiting for channel to close")
		}
	}
}

// =========================================================================
// recvLoop tests
// =========================================================================

func TestRecvLoop_CloseByServer(t *testing.T) {
	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			return &pb.QueuesDownstreamResponse{
				RequestTypeData: pb.QueuesDownstreamRequestType_CloseByServer,
			}, nil
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)
	done := waitRecvLoopDone(r)

	errs := drainErrors(r.errCh, 2*time.Second)
	require.GreaterOrEqual(t, len(errs), 1, "expected at least one error from CloseByServer")
	assert.Contains(t, errs[0].Error(), "server closed downstream stream")

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

func TestRecvLoop_NonConnectionError(t *testing.T) {
	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			return nil, errors.New("some-non-connection-error")
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
		isConnErr:       false,
	}
	r := newTestReceiver(t, stream, mt)
	done := waitRecvLoopDone(r)

	errs := drainErrors(r.errCh, 2*time.Second)
	require.GreaterOrEqual(t, len(errs), 1)
	assert.Contains(t, errs[0].Error(), "some-non-connection-error")

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

func TestRecvLoop_NonConnectionError_ErrChFull(t *testing.T) {
	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			return nil, errors.New("non-conn-error")
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
		isConnErr:       false,
	}
	r := newTestReceiver(t, stream, mt)

	for i := 0; i < cap(r.errCh); i++ {
		r.errCh <- fmt.Errorf("filler-%d", i)
	}

	done := waitRecvLoopDone(r)

	// recvLoop exits and closes errCh. Drain all.
	waitChanClosed(t, r.errCh, 2*time.Second)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

func TestRecvLoop_ConnectionError_Reconnect(t *testing.T) {
	reconnectDone := make(chan struct{})
	close(reconnectDone)

	mt := &mockReceiverTransport{
		waitReconnectCh: reconnectDone,
		clientID:        "test-client",
		isConnErr:       true,
	}

	firstCall := atomic.Bool{}
	firstCall.Store(true)
	newStreamCalled := atomic.Bool{}

	// stopRecv is used to unblock the new stream's Recv when the test is done
	stopRecv := make(chan struct{})

	newStream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			newStreamCalled.Store(true)
			<-stopRecv
			return nil, errors.New("stopped")
		},
	}

	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			if firstCall.CompareAndSwap(true, false) {
				return nil, errors.New("connection-lost")
			}
			<-stopRecv
			return nil, errors.New("stopped")
		},
	}

	r := newTestReceiver(t, stream, mt)

	r.sendCh <- &pb.QueuesDownstreamRequest{RequestID: "stale-1"}
	r.sendCh <- &pb.QueuesDownstreamRequest{RequestID: "stale-2"}

	r.openStream = func() (pb.Kubemq_QueuesDownstreamClient, error) {
		return newStream, nil
	}

	done := waitRecvLoopDone(r)

	time.Sleep(300 * time.Millisecond)

	assert.True(t, newStreamCalled.Load(), "expected new stream to be opened after reconnect")
	assert.Equal(t, 0, len(r.sendCh), "sendCh should be drained after reconnect")

	// Unblock Recv so recvLoop can process the error and exit
	close(stopRecv)
	// Cancel ctx to trigger recvLoop exit
	r.cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

func TestRecvLoop_ConnectionError_CtxCancel(t *testing.T) {
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}), // never closes
		clientID:        "test-client",
		isConnErr:       true,
	}

	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			return nil, errors.New("connection-lost")
		},
	}

	r := newTestReceiver(t, stream, mt)
	done := waitRecvLoopDone(r)

	time.Sleep(100 * time.Millisecond)
	r.cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recvLoop did not exit after ctx cancel")
	}
}

func TestRecvLoop_ConnectionError_OpenStreamFails(t *testing.T) {
	reconnectDone := make(chan struct{})
	close(reconnectDone)

	mt := &mockReceiverTransport{
		waitReconnectCh: reconnectDone,
		clientID:        "test-client",
		isConnErr:       true,
	}

	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			return nil, errors.New("connection-lost")
		},
	}

	r := newTestReceiver(t, stream, mt)
	r.openStream = func() (pb.Kubemq_QueuesDownstreamClient, error) {
		return nil, errors.New("open-stream-failed")
	}

	done := waitRecvLoopDone(r)

	errs := drainErrors(r.errCh, 2*time.Second)
	recoveryFailed := false
	for _, e := range errs {
		if e != nil && strings.Contains(e.Error(), "recovery failed") {
			recoveryFailed = true
		}
	}
	assert.True(t, recoveryFailed, "expected error about recovery failure, got: %v", errs)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

func TestRecvLoop_ConnectionError_SendChDrained(t *testing.T) {
	reconnectDone := make(chan struct{})
	close(reconnectDone)

	mt := &mockReceiverTransport{
		waitReconnectCh: reconnectDone,
		clientID:        "test-client",
		isConnErr:       true,
	}

	firstCall := atomic.Bool{}
	firstCall.Store(true)
	stopRecv := make(chan struct{})

	newStream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			<-stopRecv
			return nil, errors.New("stopped")
		},
	}

	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			if firstCall.CompareAndSwap(true, false) {
				return nil, errors.New("connection-lost")
			}
			<-stopRecv
			return nil, errors.New("stopped")
		},
	}

	r := newTestReceiver(t, stream, mt)
	r.openStream = func() (pb.Kubemq_QueuesDownstreamClient, error) {
		return newStream, nil
	}

	for i := 0; i < 10; i++ {
		r.sendCh <- &pb.QueuesDownstreamRequest{RequestID: fmt.Sprintf("stale-%d", i)}
	}

	done := waitRecvLoopDone(r)

	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, 0, len(r.sendCh), "sendCh should be drained after reconnect")

	close(stopRecv)
	r.cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

func TestRecvLoop_ErrorResponse_Get(t *testing.T) {
	callCount := atomic.Int32{}
	stopRecv := make(chan struct{})

	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			n := callCount.Add(1)
			if n == 1 {
				return &pb.QueuesDownstreamResponse{
					RequestTypeData: pb.QueuesDownstreamRequestType_Get,
					IsError:         true,
					Error:           "server-error-msg",
					RefRequestId:    "req-1",
				}, nil
			}
			<-stopRecv
			return nil, errors.New("stopped")
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)
	done := waitRecvLoopDone(r)

	// Check errCh
	select {
	case err := <-r.errCh:
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "server-error-msg")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for error on errCh")
	}

	// Check responseCh
	select {
	case resp := <-r.responseCh:
		require.NotNil(t, resp)
		assert.True(t, resp.IsError)
		assert.Equal(t, "req-1", resp.RefRequestId)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for response on responseCh")
	}

	close(stopRecv)
	r.cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

func TestRecvLoop_ErrorResponse_NonGet(t *testing.T) {
	callCount := atomic.Int32{}
	stopRecv := make(chan struct{})

	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			n := callCount.Add(1)
			if n == 1 {
				return &pb.QueuesDownstreamResponse{
					RequestTypeData: pb.QueuesDownstreamRequestType_AckRange,
					IsError:         true,
					Error:           "ack-error",
				}, nil
			}
			<-stopRecv
			return nil, errors.New("stopped")
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)
	done := waitRecvLoopDone(r)

	select {
	case err := <-r.errCh:
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "ack-error")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for error on errCh")
	}

	// responseCh should NOT receive anything
	select {
	case resp := <-r.responseCh:
		t.Fatalf("unexpected response on responseCh: %+v", resp)
	case <-time.After(200 * time.Millisecond):
		// expected
	}

	close(stopRecv)
	r.cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

func TestRecvLoop_ErrorResponse_ErrChFull(t *testing.T) {
	callCount := atomic.Int32{}
	stopRecv := make(chan struct{})

	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			n := callCount.Add(1)
			if n == 1 {
				return &pb.QueuesDownstreamResponse{
					RequestTypeData: pb.QueuesDownstreamRequestType_AckRange,
					IsError:         true,
					Error:           "err-full-test",
				}, nil
			}
			<-stopRecv
			return nil, errors.New("stopped")
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	for i := 0; i < cap(r.errCh); i++ {
		r.errCh <- fmt.Errorf("filler-%d", i)
	}

	spy := &spyLogger{}
	r.logger = spy

	done := waitRecvLoopDone(r)

	time.Sleep(300 * time.Millisecond)
	assert.GreaterOrEqual(t, callCount.Load(), int32(2), "recvLoop should continue after non-blocking send to full errCh")

	msgs := spy.getWarnMsgs()
	found := false
	for _, m := range msgs {
		if strings.Contains(m, "error channel full") {
			found = true
		}
	}
	assert.True(t, found, "expected warn about error channel full, got: %v", msgs)

	close(stopRecv)
	r.cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

func TestRecvLoop_UnexpectedResponseType(t *testing.T) {
	callCount := atomic.Int32{}
	stopRecv := make(chan struct{})

	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			n := callCount.Add(1)
			if n == 1 {
				return &pb.QueuesDownstreamResponse{
					RequestTypeData: pb.QueuesDownstreamRequestType(99),
					IsError:         false,
				}, nil
			}
			<-stopRecv
			return nil, errors.New("stopped")
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	spy := &spyLogger{}
	r.logger = spy

	done := waitRecvLoopDone(r)

	time.Sleep(300 * time.Millisecond)
	assert.GreaterOrEqual(t, callCount.Load(), int32(2), "recvLoop should continue after unexpected response type")

	msgs := spy.getWarnMsgs()
	found := false
	for _, m := range msgs {
		if strings.Contains(m, "unexpected downstream response type") {
			found = true
		}
	}
	assert.True(t, found, "expected warn about unexpected response type, got: %v", msgs)

	close(stopRecv)
	r.cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

// =========================================================================
// Poll tests
// =========================================================================

func TestPoll_ReconnectCh_EnqueueSelect(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	for i := 0; i < cap(r.sendCh); i++ {
		r.sendCh <- &pb.QueuesDownstreamRequest{RequestID: fmt.Sprintf("block-%d", i)}
	}

	close(r.reconnectCh)

	resp, err := r.Poll(context.Background(), &PollRequest{
		Channel:            "test-ch",
		MaxItems:           1,
		WaitTimeoutSeconds: 1,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "disconnected")
}

func TestPoll_StopCh_EnqueueSelect(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	for i := 0; i < cap(r.sendCh); i++ {
		r.sendCh <- &pb.QueuesDownstreamRequest{RequestID: fmt.Sprintf("block-%d", i)}
	}

	close(r.stopCh)

	resp, err := r.Poll(context.Background(), &PollRequest{
		Channel:            "test-ch",
		MaxItems:           1,
		WaitTimeoutSeconds: 1,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "closed")
}

func TestPoll_CtxDone_EnqueueSelect(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	for i := 0; i < cap(r.sendCh); i++ {
		r.sendCh <- &pb.QueuesDownstreamRequest{RequestID: fmt.Sprintf("block-%d", i)}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resp, err := r.Poll(ctx, &PollRequest{
		Channel:            "test-ch",
		MaxItems:           1,
		WaitTimeoutSeconds: 1,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestPoll_StaleResponse(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	go func() {
		pbReq := <-r.sendCh
		realID := pbReq.RequestID

		r.responseCh <- &pb.QueuesDownstreamResponse{
			RefRequestId:    "wrong-id",
			RequestTypeData: pb.QueuesDownstreamRequestType_Get,
			Messages:        []*pb.QueueMessage{{MessageID: "stale-msg"}},
		}

		time.Sleep(50 * time.Millisecond)
		r.responseCh <- &pb.QueuesDownstreamResponse{
			RefRequestId:    realID,
			TransactionId:   "tx-correct",
			RequestTypeData: pb.QueuesDownstreamRequestType_Get,
			Messages: []*pb.QueueMessage{
				{MessageID: "correct-msg", Channel: "test-ch", Body: []byte("data")},
			},
		}
	}()

	resp, err := r.Poll(context.Background(), &PollRequest{
		Channel:            "test-ch",
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
		AutoAck:            true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "tx-correct", resp.TransactionID)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "correct-msg", resp.Messages[0].Message.ID)
}

func TestPoll_ResponseCh_Closed(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	go func() {
		<-r.sendCh
		close(r.responseCh)
	}()

	resp, err := r.Poll(context.Background(), &PollRequest{
		Channel:            "test-ch",
		MaxItems:           1,
		WaitTimeoutSeconds: 1,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "closed")
}

func TestPoll_ReconnectCh_ResponseSelect(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	go func() {
		<-r.sendCh
		time.Sleep(50 * time.Millisecond)
		close(r.reconnectCh)
	}()

	resp, err := r.Poll(context.Background(), &PollRequest{
		Channel:            "test-ch",
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "disconnected")
}

func TestPoll_StopCh_ResponseSelect(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	go func() {
		<-r.sendCh
		time.Sleep(50 * time.Millisecond)
		close(r.stopCh)
	}()

	resp, err := r.Poll(context.Background(), &PollRequest{
		Channel:            "test-ch",
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "closed")
}

func TestPoll_CtxDone_ResponseSelect(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-r.sendCh
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	resp, err := r.Poll(ctx, &PollRequest{
		Channel:            "test-ch",
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestPoll_ClosedReceiver(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)
	r.closed.Store(true)

	resp, err := r.Poll(context.Background(), &PollRequest{
		Channel:            "test-ch",
		MaxItems:           1,
		WaitTimeoutSeconds: 1,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "closed")
}

// =========================================================================
// enqueue tests
// =========================================================================

func TestEnqueue_StopCh(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	for i := 0; i < cap(r.sendCh); i++ {
		r.sendCh <- &pb.QueuesDownstreamRequest{RequestID: fmt.Sprintf("block-%d", i)}
	}

	close(r.stopCh)

	err := r.enqueue(&pb.QueuesDownstreamRequest{RequestID: "test"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestEnqueue_CtxDone(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	for i := 0; i < cap(r.sendCh); i++ {
		r.sendCh <- &pb.QueuesDownstreamRequest{RequestID: fmt.Sprintf("block-%d", i)}
	}

	r.cancel()

	err := r.enqueue(&pb.QueuesDownstreamRequest{RequestID: "test"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestEnqueue_Closed(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)
	r.closed.Store(true)

	err := r.enqueue(&pb.QueuesDownstreamRequest{RequestID: "test"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// =========================================================================
// drainSendCh tests
// =========================================================================

func TestDrainSendCh_SendError(t *testing.T) {
	sendCount := atomic.Int32{}
	stream := &fakeRecvStream{
		sendFn: func(_ *pb.QueuesDownstreamRequest) error {
			n := sendCount.Add(1)
			if n >= 2 {
				return errors.New("send-failed")
			}
			return nil
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	for i := 0; i < 5; i++ {
		r.sendCh <- &pb.QueuesDownstreamRequest{RequestID: fmt.Sprintf("drain-%d", i)}
	}

	r.drainSendCh()

	assert.Equal(t, int32(2), sendCount.Load(), "drain should stop after first send error")
	assert.Equal(t, 3, len(r.sendCh), "remaining items should be in sendCh after drain error")
}

// =========================================================================
// startSendLoop tests
// =========================================================================

func TestStartSendLoop_SendError(t *testing.T) {
	stream := &fakeRecvStream{
		sendFn: func(_ *pb.QueuesDownstreamRequest) error {
			return errors.New("send-loop-error")
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	streamCtx, streamCancel := context.WithCancel(r.ctx)
	defer streamCancel()

	done := r.startSendLoop(streamCtx)

	r.sendCh <- &pb.QueuesDownstreamRequest{RequestID: "trigger-error"}

	select {
	case err, ok := <-done:
		if ok && err != nil {
			assert.Contains(t, err.Error(), "send-loop-error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for send loop error")
	}

	// done should close after
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for done to close")
	}
}

func TestStartSendLoop_CtxDone(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	streamCtx, streamCancel := context.WithCancel(r.ctx)
	done := r.startSendLoop(streamCtx)

	streamCancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for send loop to exit after stream ctx cancel")
	}
}

func TestStartSendLoop_ReceiverCtxDone(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	streamCtx, streamCancel := context.WithCancel(r.ctx)
	defer streamCancel()

	done := r.startSendLoop(streamCtx)

	r.cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for send loop to exit after receiver ctx cancel")
	}
}

// =========================================================================
// NackAll / ReQueueAll validation tests
// =========================================================================

func TestNackAll_EmptyMessages(t *testing.T) {
	pr := &PollResponse{
		TransactionID: "tx-1",
		Messages:      nil,
	}
	err := pr.NackAll()
	assert.NoError(t, err)
}

func TestNackAll_AutoAck(t *testing.T) {
	pr := &PollResponse{
		TransactionID: "tx-1",
		Messages:      []*QueueDownstreamMessage{{Message: &QueueMessage{ID: "m1"}}},
		autoAck:       true,
	}
	err := pr.NackAll()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AutoAck")
}

func TestNackAll_NilSendFn(t *testing.T) {
	pr := &PollResponse{
		TransactionID: "tx-1",
		Messages:      []*QueueDownstreamMessage{{Message: &QueueMessage{ID: "m1"}}},
		autoAck:       false,
		sendFn:        nil,
	}
	err := pr.NackAll()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no downstream send function")
}

func TestNackAll_EmptyTransactionID(t *testing.T) {
	pr := &PollResponse{
		TransactionID: "",
		Messages:      []*QueueDownstreamMessage{{Message: &QueueMessage{ID: "m1"}}},
		autoAck:       false,
		sendFn:        func(_ settleRequest) error { return nil },
	}
	err := pr.NackAll()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no transaction ID")
}

func TestNackAll_Success(t *testing.T) {
	sendCalled := false
	cleanupCalled := false
	pr := &PollResponse{
		TransactionID: "tx-1",
		Messages:      []*QueueDownstreamMessage{{Message: &QueueMessage{ID: "m1"}}},
		autoAck:       false,
		sendFn: func(req settleRequest) error {
			sendCalled = true
			assert.Equal(t, QueueDownstreamNAckAll, req.requestType)
			assert.Equal(t, "tx-1", req.transactionID)
			return nil
		},
		cleanupTxn: func() { cleanupCalled = true },
	}
	err := pr.NackAll()
	assert.NoError(t, err)
	assert.True(t, sendCalled)
	assert.True(t, cleanupCalled)
}

func TestReQueueAll_EmptyMessages(t *testing.T) {
	pr := &PollResponse{
		TransactionID: "tx-1",
		Messages:      nil,
	}
	err := pr.ReQueueAll("dest-ch")
	assert.NoError(t, err)
}

func TestReQueueAll_EmptyChannel(t *testing.T) {
	pr := &PollResponse{
		TransactionID: "tx-1",
		Messages:      []*QueueDownstreamMessage{{Message: &QueueMessage{ID: "m1"}}},
		autoAck:       false,
		sendFn:        func(_ settleRequest) error { return nil },
	}
	err := pr.ReQueueAll("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requeue channel is required")
}

func TestReQueueAll_AutoAck(t *testing.T) {
	pr := &PollResponse{
		TransactionID: "tx-1",
		Messages:      []*QueueDownstreamMessage{{Message: &QueueMessage{ID: "m1"}}},
		autoAck:       true,
	}
	err := pr.ReQueueAll("dest-ch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AutoAck")
}

func TestReQueueAll_NilSendFn(t *testing.T) {
	pr := &PollResponse{
		TransactionID: "tx-1",
		Messages:      []*QueueDownstreamMessage{{Message: &QueueMessage{ID: "m1"}}},
		autoAck:       false,
		sendFn:        nil,
	}
	err := pr.ReQueueAll("dest-ch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no downstream send function")
}

func TestReQueueAll_EmptyTransactionID(t *testing.T) {
	pr := &PollResponse{
		TransactionID: "",
		Messages:      []*QueueDownstreamMessage{{Message: &QueueMessage{ID: "m1"}}},
		autoAck:       false,
		sendFn:        func(_ settleRequest) error { return nil },
	}
	err := pr.ReQueueAll("dest-ch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no transaction ID")
}

func TestReQueueAll_Success(t *testing.T) {
	sendCalled := false
	cleanupCalled := false
	pr := &PollResponse{
		TransactionID: "tx-1",
		Messages:      []*QueueDownstreamMessage{{Message: &QueueMessage{ID: "m1"}}},
		autoAck:       false,
		sendFn: func(req settleRequest) error {
			sendCalled = true
			assert.Equal(t, QueueDownstreamReQueueAll, req.requestType)
			assert.Equal(t, "tx-1", req.transactionID)
			assert.Equal(t, "dest-ch", req.reQueueChannel)
			return nil
		},
		cleanupTxn: func() { cleanupCalled = true },
	}
	err := pr.ReQueueAll("dest-ch")
	assert.NoError(t, err)
	assert.True(t, sendCalled)
	assert.True(t, cleanupCalled)
}

// =========================================================================
// Errors() test
// =========================================================================

func TestErrors_ReturnsChan(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)
	ch := r.Errors()
	require.NotNil(t, ch)
	// Send an error and verify it arrives on the returned channel
	r.errCh <- errors.New("test-error")
	select {
	case err := <-ch:
		assert.Contains(t, err.Error(), "test-error")
	case <-time.After(time.Second):
		t.Fatal("timeout reading from Errors() channel")
	}
}

// =========================================================================
// settle() test
// =========================================================================

func TestSettle_EnqueuesRequest(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	err := r.settle(settleRequest{
		requestType:    QueueDownstreamAckAll,
		transactionID:  "tx-settle",
		sequences:      []int64{1, 2, 3},
		reQueueChannel: "requeue-ch",
	})
	require.NoError(t, err)

	// Check that sendCh received the request
	select {
	case req := <-r.sendCh:
		assert.Equal(t, pb.QueuesDownstreamRequestType_AckAll, req.RequestTypeData)
		assert.Equal(t, "tx-settle", req.RefTransactionId)
		assert.Equal(t, []int64{1, 2, 3}, req.SequenceRange)
		assert.Equal(t, "requeue-ch", req.ReQueueChannel)
		assert.Equal(t, "test-client", req.ClientID)
		assert.NotEmpty(t, req.RequestID)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for settle request on sendCh")
	}
}

// =========================================================================
// enqueue() happy path test
// =========================================================================

func TestEnqueue_Success(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	req := &pb.QueuesDownstreamRequest{RequestID: "happy-path"}
	err := r.enqueue(req)
	require.NoError(t, err)

	select {
	case got := <-r.sendCh:
		assert.Equal(t, "happy-path", got.RequestID)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for enqueue")
	}
}

// =========================================================================
// buildPollResponse() tests
// =========================================================================

func TestBuildPollResponse_ErrorResponse(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	resp := &pb.QueuesDownstreamResponse{
		TransactionId: "tx-err",
		IsError:       true,
		Error:         "some error",
	}

	result := r.buildPollResponse(resp, false)
	assert.Equal(t, "tx-err", result.TransactionID)
	assert.True(t, result.IsError)
	assert.Equal(t, "some error", result.Error)
	assert.Nil(t, result.Messages)
}

func TestBuildPollResponse_EmptyMessages(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	resp := &pb.QueuesDownstreamResponse{
		TransactionId: "tx-empty",
		IsError:       false,
		Messages:      nil,
	}

	result := r.buildPollResponse(resp, true)
	assert.Equal(t, "tx-empty", result.TransactionID)
	assert.Nil(t, result.Messages)
}

func TestBuildPollResponse_WithMessagesAutoAck(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	resp := &pb.QueuesDownstreamResponse{
		TransactionId: "tx-auto",
		IsError:       false,
		Messages: []*pb.QueueMessage{
			{
				MessageID: "msg-1",
				ClientID:  "client-1",
				Channel:   "ch-1",
				Metadata:  "meta",
				Body:      []byte("body-1"),
				Tags:      map[string]string{"k": "v"},
				Policy: &pb.QueueMessagePolicy{
					DelaySeconds:      10,
					ExpirationSeconds: 60,
					MaxReceiveCount:   3,
					MaxReceiveQueue:   "dlq",
				},
				Attributes: &pb.QueueMessageAttributes{
					Sequence:          42,
					Timestamp:         1000,
					MD5OfBody:         "abc",
					ReceiveCount:      2,
					ReRouted:          true,
					ReRoutedFromQueue: "orig",
					ExpirationAt:      2000,
					DelayedTo:         3000,
				},
			},
		},
	}

	result := r.buildPollResponse(resp, true)
	assert.Equal(t, "tx-auto", result.TransactionID)
	assert.True(t, result.autoAck)
	assert.Nil(t, result.sendFn) // autoAck => nil sendFn
	require.Len(t, result.Messages, 1)

	msg := result.Messages[0]
	assert.Equal(t, "msg-1", msg.Message.ID)
	assert.Equal(t, "client-1", msg.Message.ClientID)
	assert.Equal(t, "ch-1", msg.Message.Channel)
	assert.Equal(t, "meta", msg.Message.Metadata)
	assert.Equal(t, []byte("body-1"), msg.Message.Body)
	assert.Equal(t, map[string]string{"k": "v"}, msg.Message.Tags)
	assert.True(t, msg.autoAck)
	assert.Nil(t, msg.sendFn)
	assert.Equal(t, uint64(42), msg.Sequence)

	// Policy
	require.NotNil(t, msg.Message.Policy)
	assert.Equal(t, 10, msg.Message.Policy.DelaySeconds)
	assert.Equal(t, 60, msg.Message.Policy.ExpirationSeconds)
	assert.Equal(t, 3, msg.Message.Policy.MaxReceiveCount)
	assert.Equal(t, "dlq", msg.Message.Policy.MaxReceiveQueue)

	// Attributes
	require.NotNil(t, msg.Message.Attributes)
	assert.Equal(t, uint64(42), msg.Message.Attributes.Sequence)
	assert.Equal(t, int64(1000), msg.Message.Attributes.Timestamp)
	assert.Equal(t, "abc", msg.Message.Attributes.MD5OfBody)
	assert.Equal(t, 2, msg.Message.Attributes.ReceiveCount)
	assert.True(t, msg.Message.Attributes.ReRouted)
	assert.Equal(t, "orig", msg.Message.Attributes.ReRoutedFromQueue)
	assert.Equal(t, int64(2000), msg.Message.Attributes.ExpirationAt)
	assert.Equal(t, int64(3000), msg.Message.Attributes.DelayedTo)
}

func TestBuildPollResponse_WithMessagesNoAutoAck(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	resp := &pb.QueuesDownstreamResponse{
		TransactionId: "tx-manual",
		IsError:       false,
		Messages: []*pb.QueueMessage{
			{
				MessageID: "msg-2",
				Channel:   "ch-2",
				Body:      []byte("body-2"),
			},
		},
	}

	result := r.buildPollResponse(resp, false)
	assert.Equal(t, "tx-manual", result.TransactionID)
	assert.False(t, result.autoAck)
	assert.NotNil(t, result.sendFn) // not autoAck => sendFn present
	assert.NotNil(t, result.cleanupTxn)
	require.Len(t, result.Messages, 1)

	msg := result.Messages[0]
	assert.Equal(t, "msg-2", msg.Message.ID)
	assert.False(t, msg.autoAck)
	assert.NotNil(t, msg.sendFn)

	// Verify openTxns was updated
	r.txnMu.Lock()
	_, exists := r.openTxns["tx-manual"]
	r.txnMu.Unlock()
	assert.True(t, exists, "expected txn to be registered in openTxns")

	// Verify cleanupTxn removes it
	result.cleanupTxn()
	r.txnMu.Lock()
	_, exists = r.openTxns["tx-manual"]
	r.txnMu.Unlock()
	assert.False(t, exists, "expected txn to be removed from openTxns after cleanup")
}

func TestBuildPollResponse_MessageNoPolicyNoAttributes(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	resp := &pb.QueuesDownstreamResponse{
		TransactionId: "tx-bare",
		IsError:       false,
		Messages: []*pb.QueueMessage{
			{
				MessageID: "msg-bare",
				Channel:   "ch",
				Body:      []byte("data"),
				// No Policy, no Attributes
			},
		},
	}

	result := r.buildPollResponse(resp, true)
	require.Len(t, result.Messages, 1)
	msg := result.Messages[0]
	assert.Nil(t, msg.Message.Policy)
	assert.Nil(t, msg.Message.Attributes)
	assert.Equal(t, uint64(0), msg.Sequence) // no attributes => seq 0
}

// =========================================================================
// Full round-trip Poll with recvLoop test
// =========================================================================

func TestPoll_FullRoundTrip_WithRecvLoop(t *testing.T) {
	// This test runs recvLoop + Poll together to exercise the complete path
	// including buildPollResponse through the live receiver.
	reqIDCh := make(chan string, 1)
	stopRecv := make(chan struct{})

	stream := &fakeRecvStream{
		sendFn: func(req *pb.QueuesDownstreamRequest) error {
			select {
			case reqIDCh <- req.RequestID:
			default:
			}
			return nil
		},
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			select {
			case reqID := <-reqIDCh:
				return &pb.QueuesDownstreamResponse{
					TransactionId:   "tx-roundtrip",
					RefRequestId:    reqID,
					RequestTypeData: pb.QueuesDownstreamRequestType_Get,
					Messages: []*pb.QueueMessage{
						{
							MessageID: "rt-msg",
							Channel:   "test-ch",
							Body:      []byte("rt-body"),
							Attributes: &pb.QueueMessageAttributes{
								Sequence: 7,
							},
						},
					},
				}, nil
			case <-stopRecv:
				return nil, errors.New("stopped")
			}
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)
	done := waitRecvLoopDone(r)

	resp, err := r.Poll(context.Background(), &PollRequest{
		Channel:            "test-ch",
		MaxItems:           5,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "tx-roundtrip", resp.TransactionID)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "rt-msg", resp.Messages[0].Message.ID)
	assert.Equal(t, uint64(7), resp.Messages[0].Sequence)

	// Since AutoAck is false by default, sendFn should be set
	assert.NotNil(t, resp.sendFn)

	close(stopRecv)
	r.cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

// =========================================================================
// recvLoop: ctx cancelled during Recv (before error processing)
// =========================================================================

func TestRecvLoop_CtxCancelledDuringRecv(t *testing.T) {
	// When Recv returns an error AND ctx is already done,
	// recvLoop should exit immediately (the ctx.Done check after recvErr != nil).
	callCount := atomic.Int32{}
	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			callCount.Add(1)
			// Simulate: by the time Recv returns error, ctx is cancelled
			return nil, errors.New("any-error")
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
		isConnErr:       false,
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &QueueDownstreamReceiver{
		transport:   mt,
		clientID:    "test-client",
		logger:      noopLogger{},
		ctx:         ctx,
		cancel:      cancel,
		stream:      stream,
		sendCh:      make(chan *pb.QueuesDownstreamRequest, 512),
		responseCh:  make(chan *pb.QueuesDownstreamResponse, 1),
		errCh:       make(chan error, 8),
		stopCh:      make(chan struct{}),
		reconnectCh: make(chan struct{}),
		openTxns:    make(map[string]struct{}),
		openStream: func() (pb.Kubemq_QueuesDownstreamClient, error) {
			return stream, nil
		},
	}
	t.Cleanup(cancel)

	// Cancel context BEFORE starting recvLoop
	cancel()

	done := waitRecvLoopDone(r)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recvLoop did not exit when ctx was cancelled")
	}
}

// =========================================================================
// recvLoop: CloseByServer when errCh is full
// =========================================================================

func TestRecvLoop_CloseByServer_ErrChFull(t *testing.T) {
	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			return &pb.QueuesDownstreamResponse{
				RequestTypeData: pb.QueuesDownstreamRequestType_CloseByServer,
			}, nil
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	// Fill errCh
	for i := 0; i < cap(r.errCh); i++ {
		r.errCh <- fmt.Errorf("filler-%d", i)
	}

	done := waitRecvLoopDone(r)
	// recvLoop should still exit even with full errCh (non-blocking send)
	waitChanClosed(t, r.errCh, 2*time.Second)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

// =========================================================================
// recvLoop: ErrorResponse Get when responseCh is occupied (ctx.Done branch)
// =========================================================================

func TestRecvLoop_ErrorResponse_Get_CtxCancel(t *testing.T) {
	// Error Get response where responseCh is full and we cancel ctx.
	// This exercises the <-r.ctx.Done() branch in the error+Get responseCh send.
	callCount := atomic.Int32{}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			n := callCount.Add(1)
			if n == 1 {
				return &pb.QueuesDownstreamResponse{
					RequestTypeData: pb.QueuesDownstreamRequestType_Get,
					IsError:         true,
					Error:           "err-ctx-cancel",
					RefRequestId:    "req-ctx",
				}, nil
			}
			// After ctx.Done fires for responseCh send, the loop continues
			// and calls Recv again. Block here until ctx is done, then return
			// an error so recvLoop exits via the ctx.Done check.
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}

	r := &QueueDownstreamReceiver{
		transport:   mt,
		clientID:    "test-client",
		logger:      noopLogger{},
		ctx:         ctx,
		cancel:      cancel,
		stream:      stream,
		sendCh:      make(chan *pb.QueuesDownstreamRequest, 512),
		responseCh:  make(chan *pb.QueuesDownstreamResponse, 1),
		errCh:       make(chan error, 8),
		stopCh:      make(chan struct{}),
		reconnectCh: make(chan struct{}),
		openTxns:    make(map[string]struct{}),
		openStream: func() (pb.Kubemq_QueuesDownstreamClient, error) {
			return stream, nil
		},
	}

	// Fill responseCh so the response send blocks
	r.responseCh <- &pb.QueuesDownstreamResponse{RefRequestId: "occupier"}

	done := waitRecvLoopDone(r)

	// Give recvLoop time to receive the error response and block on responseCh
	time.Sleep(100 * time.Millisecond)

	// Cancel ctx to unblock the responseCh send
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recvLoop did not exit after ctx cancel with full responseCh")
	}
}

// =========================================================================
// recvLoop: normal Get response when responseCh is occupied (ctx.Done branch)
// =========================================================================

func TestRecvLoop_NormalGet_CtxCancel(t *testing.T) {
	// Normal (non-error) Get response where responseCh is full and ctx cancelled.
	// This exercises the <-r.ctx.Done() branch in the normal Get responseCh send.
	callCount := atomic.Int32{}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			n := callCount.Add(1)
			if n == 1 {
				return &pb.QueuesDownstreamResponse{
					RequestTypeData: pb.QueuesDownstreamRequestType_Get,
					IsError:         false,
					RefRequestId:    "req-normal",
					Messages: []*pb.QueueMessage{
						{MessageID: "msg-normal"},
					},
				}, nil
			}
			// After ctx.Done fires for responseCh send, the loop continues.
			// Block here until ctx is done, then return error.
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}

	r := &QueueDownstreamReceiver{
		transport:   mt,
		clientID:    "test-client",
		logger:      noopLogger{},
		ctx:         ctx,
		cancel:      cancel,
		stream:      stream,
		sendCh:      make(chan *pb.QueuesDownstreamRequest, 512),
		responseCh:  make(chan *pb.QueuesDownstreamResponse, 1),
		errCh:       make(chan error, 8),
		stopCh:      make(chan struct{}),
		reconnectCh: make(chan struct{}),
		openTxns:    make(map[string]struct{}),
		openStream: func() (pb.Kubemq_QueuesDownstreamClient, error) {
			return stream, nil
		},
	}

	// Fill responseCh
	r.responseCh <- &pb.QueuesDownstreamResponse{RefRequestId: "occupier"}

	done := waitRecvLoopDone(r)

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recvLoop did not exit after ctx cancel with full responseCh (normal Get)")
	}
}

// =========================================================================
// recvLoop: connection error with errCh full for reconnecting message
// =========================================================================

func TestRecvLoop_ConnectionError_ErrChFull(t *testing.T) {
	// Connection error when errCh is full — non-blocking send for reconnecting message.
	reconnectDone := make(chan struct{})
	close(reconnectDone)

	mt := &mockReceiverTransport{
		waitReconnectCh: reconnectDone,
		clientID:        "test-client",
		isConnErr:       true,
	}

	stream := &fakeRecvStream{
		recvFn: func() (*pb.QueuesDownstreamResponse, error) {
			return nil, errors.New("connection-lost")
		},
	}

	r := newTestReceiver(t, stream, mt)

	// Fill errCh
	for i := 0; i < cap(r.errCh); i++ {
		r.errCh <- fmt.Errorf("filler-%d", i)
	}

	r.openStream = func() (pb.Kubemq_QueuesDownstreamClient, error) {
		return nil, errors.New("open-failed")
	}

	done := waitRecvLoopDone(r)

	// recvLoop should still exit, even though errCh was full
	waitChanClosed(t, r.errCh, 2*time.Second)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recvLoop did not exit")
	}
}

// =========================================================================
// Poll validation tests
// =========================================================================

func TestPoll_EmptyChannel(t *testing.T) {
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	resp, err := r.Poll(context.Background(), &PollRequest{
		Channel:            "",
		MaxItems:           1,
		WaitTimeoutSeconds: 1,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "channel cannot be empty")
}

func TestPoll_DefaultMaxItems(t *testing.T) {
	// MaxItems <= 0 should default to 1, WaitTimeout < 1 should default to 1000ms
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	go func() {
		pbReq := <-r.sendCh
		// Verify defaults were applied
		assert.Equal(t, int32(1), pbReq.MaxItems)
		assert.Equal(t, int32(1000), pbReq.WaitTimeout)
		r.responseCh <- &pb.QueuesDownstreamResponse{
			RefRequestId:    pbReq.RequestID,
			TransactionId:   "tx-defaults",
			RequestTypeData: pb.QueuesDownstreamRequestType_Get,
			Messages: []*pb.QueueMessage{
				{MessageID: "msg-defaults"},
			},
		}
	}()

	resp, err := r.Poll(context.Background(), &PollRequest{
		Channel:            "test-ch",
		MaxItems:           0, // should default to 1
		WaitTimeoutSeconds: 0, // should default to 1000ms
		AutoAck:            true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "tx-defaults", resp.TransactionID)
}

// =========================================================================
// Close() with open transactions test
// =========================================================================

func TestClose_WithOpenTransactions(t *testing.T) {
	sentRequests := make(chan *pb.QueuesDownstreamRequest, 10)
	stream := &fakeRecvStream{
		sendFn: func(req *pb.QueuesDownstreamRequest) error {
			select {
			case sentRequests <- req:
			default:
			}
			return nil
		},
	}

	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	// Register some open transactions
	r.txnMu.Lock()
	r.openTxns["txn-a"] = struct{}{}
	r.openTxns["txn-b"] = struct{}{}
	r.txnMu.Unlock()

	// Start a send loop so Close can drain sendCh
	streamCtx, streamCancel := context.WithCancel(r.ctx)
	defer streamCancel()
	sendDone := r.startSendLoop(streamCtx)
	r.mu.Lock()
	r.sendDone = sendDone
	r.streamCancel = streamCancel
	r.mu.Unlock()

	err := r.Close()
	require.NoError(t, err)

	// Verify closed state
	assert.True(t, r.closed.Load())

	// Verify open txns were cleared
	r.txnMu.Lock()
	assert.Empty(t, r.openTxns)
	r.txnMu.Unlock()

	// Collect sent CloseByClient requests
	close(sentRequests)
	var closeReqs []*pb.QueuesDownstreamRequest
	for req := range sentRequests {
		if req.RequestTypeData == pb.QueuesDownstreamRequestType_CloseByClient {
			closeReqs = append(closeReqs, req)
		}
	}
	assert.Len(t, closeReqs, 2, "expected 2 CloseByClient requests for open txns")
}

func TestClose_SendDoneTimeout(t *testing.T) {
	// Test the timeout path in Close when sendDone never completes.
	stream := &fakeRecvStream{}
	mt := &mockReceiverTransport{
		waitReconnectCh: make(chan struct{}),
		clientID:        "test-client",
	}
	r := newTestReceiver(t, stream, mt)

	// Create a sendDone channel that never closes (simulates stuck send goroutine)
	neverDone := make(chan error)
	r.mu.Lock()
	r.sendDone = neverDone
	r.mu.Unlock()

	// Close should not hang (has 5s timeout internally, but we test it doesn't panic)
	// Use a shorter external timeout to keep the test fast
	done := make(chan struct{})
	go func() {
		_ = r.Close()
		close(done)
	}()

	select {
	case <-done:
		// Close returned (either via timeout or normal path)
	case <-time.After(10 * time.Second):
		t.Fatal("Close() hung")
	}
}
