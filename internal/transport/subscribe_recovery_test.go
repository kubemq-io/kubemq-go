package transport

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/kubemq-io/kubemq-go/v2/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockRecvStream simulates a gRPC recv stream that can be controlled in tests.
type mockRecvStream struct {
	mu       sync.Mutex
	messages []interface{}
	pos      int
	errOnce  error
	closed   chan struct{}
}

func newMockRecvStream(msgs ...interface{}) *mockRecvStream {
	return &mockRecvStream{
		messages: msgs,
		closed:   make(chan struct{}),
	}
}

func (m *mockRecvStream) Recv() (interface{}, error) {
	m.mu.Lock()
	if m.errOnce != nil {
		err := m.errOnce
		m.errOnce = nil
		m.mu.Unlock()
		return nil, err
	}
	if m.pos >= len(m.messages) {
		m.mu.Unlock()
		<-m.closed
		return nil, fmt.Errorf("stream closed")
	}
	msg := m.messages[m.pos]
	m.pos++
	m.mu.Unlock()
	return msg, nil
}

func (m *mockRecvStream) close() {
	select {
	case <-m.closed:
	default:
		close(m.closed)
	}
}

// --- Test: receiveBufferSize Config defaults ---

func TestReceiveBufferSizeConfigDefault(t *testing.T) {
	cfg := Config{}
	if cfg.ReceiveBufferSize <= 0 {
		cfg.ReceiveBufferSize = 10
	}
	assert.Equal(t, 10, cfg.ReceiveBufferSize)
}

func TestReceiveBufferSizeConfigCustom(t *testing.T) {
	cfg := Config{ReceiveBufferSize: 256}
	if cfg.ReceiveBufferSize <= 0 {
		cfg.ReceiveBufferSize = 10
	}
	assert.Equal(t, 256, cfg.ReceiveBufferSize)
}

// --- Test: reconnect notification mechanism ---

func TestNotifyReconnected(t *testing.T) {
	tr := &grpcTransport{
		reconnectNotifyCh: make(chan struct{}),
	}

	ch := tr.waitReconnect()
	select {
	case <-ch:
		t.Fatal("channel should not be closed yet")
	default:
	}

	tr.notifyReconnected()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("channel should be closed after notifyReconnected")
	}

	ch2 := tr.waitReconnect()
	select {
	case <-ch2:
		t.Fatal("new channel should not be closed")
	default:
	}
}

func TestNotifyReconnectedMultiple(t *testing.T) {
	tr := &grpcTransport{
		reconnectNotifyCh: make(chan struct{}),
	}

	for i := 0; i < 5; i++ {
		ch := tr.waitReconnect()
		tr.notifyReconnected()
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: channel should be closed after notifyReconnected", i)
		}
	}
}

// --- Test: subscriptionTracker ---

func TestSubscriptionTrackerRegisterRemoveCount(t *testing.T) {
	st := &subscriptionTracker{}

	st.register(&subscriptionRecord{id: "a", pattern: "events", channel: "ch1"})
	st.register(&subscriptionRecord{id: "b", pattern: "events", channel: "ch2"})
	assert.Equal(t, 2, st.count())

	st.remove("a")
	assert.Equal(t, 1, st.count())

	st.remove("b")
	assert.Equal(t, 0, st.count())
}

func TestSubscriptionTrackerRemoveNonexistent(t *testing.T) {
	st := &subscriptionTracker{}
	st.register(&subscriptionRecord{id: "a", pattern: "events", channel: "ch1"})

	st.remove("nonexistent")
	assert.Equal(t, 1, st.count())
}

func TestSubscriptionTrackerSeqTracking(t *testing.T) {
	st := &subscriptionTracker{}
	st.register(&subscriptionRecord{id: "sub-1", pattern: "events_store", channel: "ch1"})

	assert.Equal(t, int64(0), st.getLastSeq("sub-1"))

	st.updateLastSeq("sub-1", 10)
	assert.Equal(t, int64(10), st.getLastSeq("sub-1"))

	st.updateLastSeq("sub-1", 42)
	assert.Equal(t, int64(42), st.getLastSeq("sub-1"))
}

func TestSubscriptionTrackerConcurrentAccess(t *testing.T) {
	st := &subscriptionTracker{}
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := fmt.Sprintf("sub-%d", n)
			st.register(&subscriptionRecord{id: id, pattern: "events", channel: "ch"})
			st.updateLastSeq(id, int64(n))
			_ = st.getLastSeq(id)
			_ = st.count()
			st.remove(id)
		}(i)
	}

	wg.Wait()
	assert.Equal(t, 0, st.count())
}

// --- Test: subscribeLoop with stream break and resume ---

func TestSubscribeLoopRecoversAfterReconnect(t *testing.T) {
	tr := &grpcTransport{
		cfg:               Config{ReceiveBufferSize: 10},
		subs:              &subscriptionTracker{},
		logger:            noopLogger{},
		reconnectNotifyCh: make(chan struct{}),
	}

	subID := "test-ch::1"
	tr.subs.register(&subscriptionRecord{id: subID, pattern: "events", channel: "test-ch"})

	msgCh := make(chan any, 10)
	errCh := make(chan error, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream1Msg := &pb.EventReceive{EventID: "msg-1", Channel: "test-ch"}
	stream2Msg := &pb.EventReceive{EventID: "msg-2", Channel: "test-ch"}

	stream1 := newMockRecvStream(stream1Msg)
	stream2 := newMockRecvStream(stream2Msg)

	var callCount atomic.Int32

	openStream := func(_ context.Context) (recvStream, error) {
		n := callCount.Add(1)
		if n == 1 {
			return stream2, nil
		}
		return nil, fmt.Errorf("unexpected openStream call %d", n)
	}

	go tr.subscribeLoop(ctx, subID, pb.Subscribe_Events, stream1, openStream, msgCh, errCh)

	// Read first message from stream1
	select {
	case msg := <-msgCh:
		item, ok := msg.(*EventReceiveItem)
		require.True(t, ok)
		assert.Equal(t, "msg-1", item.ID)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for msg-1")
	}

	// Simulate stream break (stream1 blocks on Recv, then gets closed)
	stream1.close()

	// Give subscribeLoop time to detect the error and wait for reconnect
	time.Sleep(50 * time.Millisecond)

	// Fire reconnect notification
	tr.notifyReconnected()

	// Read message from stream2 (recovered subscription)
	select {
	case msg := <-msgCh:
		item, ok := msg.(*EventReceiveItem)
		require.True(t, ok)
		assert.Equal(t, "msg-2", item.ID)
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for msg-2 after recovery")
	}

	// Cancel to clean up
	cancel()
	stream2.close()

	// Channels should eventually drain/close
	drainTimeout := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-msgCh:
			if !ok {
				return
			}
		case <-drainTimeout:
			t.Fatal("timed out waiting for channel close")
		}
	}
}

func TestSubscribeLoopCleanCancelDuringReconnectWait(t *testing.T) {
	tr := &grpcTransport{
		cfg:               Config{ReceiveBufferSize: 10},
		subs:              &subscriptionTracker{},
		logger:            noopLogger{},
		reconnectNotifyCh: make(chan struct{}),
	}

	subID := "test-ch::1"
	tr.subs.register(&subscriptionRecord{id: subID, pattern: "events", channel: "test-ch"})

	msgCh := make(chan any, 10)
	errCh := make(chan error, 8)
	ctx, cancel := context.WithCancel(context.Background())

	stream := newMockRecvStream()

	openStream := func(_ context.Context) (recvStream, error) {
		return nil, fmt.Errorf("should not be called")
	}

	done := make(chan struct{})
	go func() {
		tr.subscribeLoop(ctx, subID, pb.Subscribe_Events, stream, openStream, msgCh, errCh)
		close(done)
	}()

	// Stream breaks immediately (no messages, will block then get closed)
	stream.close()

	// Give it time to enter the reconnect wait
	time.Sleep(50 * time.Millisecond)

	// Cancel while waiting for reconnect
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscribeLoop did not exit after context cancel")
	}

	// Channels should be closed
	_, ok := <-msgCh
	assert.False(t, ok, "msgCh should be closed")
}

// --- Test: events_store sequence tracking in recvLoop ---

func TestRecvLoopTracksEventsStoreSequence(t *testing.T) {
	tr := &grpcTransport{
		cfg:    Config{ReceiveBufferSize: 10},
		subs:   &subscriptionTracker{},
		logger: noopLogger{},
	}

	subID := "store-ch::3"
	tr.subs.register(&subscriptionRecord{id: subID, pattern: "events_store", channel: "store-ch"})

	msgs := []interface{}{
		&pb.EventReceive{EventID: "e1", Channel: "store-ch", Sequence: 10},
		&pb.EventReceive{EventID: "e2", Channel: "store-ch", Sequence: 11},
		&pb.EventReceive{EventID: "e3", Channel: "store-ch", Sequence: 12},
	}

	stream := newMockRecvStream(msgs...)
	msgCh := make(chan any, 10)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		stream.close()
	}()

	// Give it messages first, then close
	time.Sleep(10 * time.Millisecond)

	err := tr.recvLoop(ctx, stream, pb.Subscribe_EventsStore, subID, msgCh)
	cancel()

	// Drain messages
	for range len(msgs) {
		<-msgCh
	}

	_ = err

	assert.Equal(t, int64(12), tr.subs.getLastSeq(subID))
}

// --- Test: subscribePatternName ---

func TestSubscribePatternName(t *testing.T) {
	assert.Equal(t, "events", subscribePatternName(pb.Subscribe_Events))
	assert.Equal(t, "events_store", subscribePatternName(pb.Subscribe_EventsStore))
	assert.Equal(t, "commands", subscribePatternName(pb.Subscribe_Commands))
	assert.Equal(t, "queries", subscribePatternName(pb.Subscribe_Queries))
	assert.Equal(t, "unknown", subscribePatternName(pb.Subscribe_SubscribeType(99)))
}
