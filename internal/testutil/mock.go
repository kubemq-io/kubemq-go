package testutil

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/kubemq-io/kubemq-go/v2/internal/types"
)

// MockTransport implements the internal Transport interface for unit testing.
// All methods are configurable via function fields.
type MockTransport struct {
	mu sync.Mutex

	state atomic.Int32

	sendEventFn      func(ctx context.Context, req *transport.SendEventRequest) error
	sendEventStoreFn func(ctx context.Context, req *transport.SendEventStoreRequest) (*transport.SendEventStoreResult, error)
	sendCommandFn    func(ctx context.Context, req *transport.SendCommandRequest) (*transport.SendCommandResult, error)
	sendQueryFn      func(ctx context.Context, req *transport.SendQueryRequest) (*transport.SendQueryResult, error)
	sendResponseFn   func(ctx context.Context, req *transport.SendResponseRequest) error
	sendQueueMsgFn   func(ctx context.Context, req *transport.QueueMessageItem) (*transport.SendQueueMessageResultItem, error)
	sendQueueMsgsFn  func(ctx context.Context, req *transport.SendQueueMessagesRequest) (*transport.SendQueueMessagesResult, error)
	recvQueueMsgsFn  func(ctx context.Context, req *transport.ReceiveQueueMessagesReq) (*transport.ReceiveQueueMessagesResp, error)
	ackAllFn         func(ctx context.Context, req *transport.AckAllQueueMessagesReq) (*transport.AckAllQueueMessagesResp, error)
	pingFn           func(ctx context.Context) (*transport.ServerInfoResult, error)
	closeFn          func() error

	subscribeToEventsFn      func(ctx context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error)
	subscribeToEventsStoreFn func(ctx context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error)
	subscribeToCommandsFn    func(ctx context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error)
	subscribeToQueriesFn     func(ctx context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error)
	sendEventsStreamFn       func(ctx context.Context) (*transport.EventStreamHandle, error)
	queueUpstreamFn          func(ctx context.Context) (*transport.QueueUpstreamHandle, error)
	queueDownstreamFn        func(ctx context.Context, req *transport.QueueDownstreamRequest) (*transport.QueueDownstreamHandle, error)
	createChannelFn          func(ctx context.Context, req *transport.CreateChannelRequest) error
	deleteChannelFn          func(ctx context.Context, req *transport.DeleteChannelRequest) error
	listChannelsFn           func(ctx context.Context, req *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error)

	SendEventCalls      int
	SendEventStoreCalls int
	SendCommandCalls    int
	SendQueryCalls      int
	PingCalls           int
	CloseCalls          int
}

// NewMockTransport returns a MockTransport with default no-op implementations.
func NewMockTransport() *MockTransport {
	mt := &MockTransport{}
	mt.state.Store(int32(types.StateReady))
	mt.sendEventFn = func(_ context.Context, _ *transport.SendEventRequest) error { return nil }
	mt.sendEventStoreFn = func(_ context.Context, _ *transport.SendEventStoreRequest) (*transport.SendEventStoreResult, error) {
		return &transport.SendEventStoreResult{Sent: true}, nil
	}
	mt.sendCommandFn = func(_ context.Context, _ *transport.SendCommandRequest) (*transport.SendCommandResult, error) {
		return &transport.SendCommandResult{Executed: true}, nil
	}
	mt.sendQueryFn = func(_ context.Context, _ *transport.SendQueryRequest) (*transport.SendQueryResult, error) {
		return &transport.SendQueryResult{Executed: true}, nil
	}
	mt.sendResponseFn = func(_ context.Context, _ *transport.SendResponseRequest) error { return nil }
	mt.sendQueueMsgFn = func(_ context.Context, _ *transport.QueueMessageItem) (*transport.SendQueueMessageResultItem, error) {
		return &transport.SendQueueMessageResultItem{}, nil
	}
	mt.sendQueueMsgsFn = func(_ context.Context, _ *transport.SendQueueMessagesRequest) (*transport.SendQueueMessagesResult, error) {
		return &transport.SendQueueMessagesResult{}, nil
	}
	mt.recvQueueMsgsFn = func(_ context.Context, _ *transport.ReceiveQueueMessagesReq) (*transport.ReceiveQueueMessagesResp, error) {
		return &transport.ReceiveQueueMessagesResp{}, nil
	}
	mt.ackAllFn = func(_ context.Context, _ *transport.AckAllQueueMessagesReq) (*transport.AckAllQueueMessagesResp, error) {
		return &transport.AckAllQueueMessagesResp{}, nil
	}
	mt.pingFn = func(_ context.Context) (*transport.ServerInfoResult, error) {
		return &transport.ServerInfoResult{Host: "mock", Version: "1.0.0"}, nil
	}
	mt.closeFn = func() error { return nil }
	return mt
}

func (m *MockTransport) State() types.ConnectionState {
	return types.ConnectionState(m.state.Load())
}

func (m *MockTransport) SetState(s types.ConnectionState) {
	m.state.Store(int32(s))
}

func (m *MockTransport) Close() error {
	m.mu.Lock()
	m.CloseCalls++
	fn := m.closeFn
	m.mu.Unlock()
	m.state.Store(int32(types.StateClosed))
	return fn()
}

func (m *MockTransport) Ping(ctx context.Context) (*transport.ServerInfoResult, error) {
	m.mu.Lock()
	m.PingCalls++
	fn := m.pingFn
	m.mu.Unlock()
	return fn(ctx)
}

func (m *MockTransport) SendEvent(ctx context.Context, req *transport.SendEventRequest) error {
	m.mu.Lock()
	m.SendEventCalls++
	fn := m.sendEventFn
	m.mu.Unlock()
	return fn(ctx, req)
}

func (m *MockTransport) SendEventStore(ctx context.Context, req *transport.SendEventStoreRequest) (*transport.SendEventStoreResult, error) {
	m.mu.Lock()
	m.SendEventStoreCalls++
	fn := m.sendEventStoreFn
	m.mu.Unlock()
	return fn(ctx, req)
}

func (m *MockTransport) SendCommand(ctx context.Context, req *transport.SendCommandRequest) (*transport.SendCommandResult, error) {
	m.mu.Lock()
	m.SendCommandCalls++
	fn := m.sendCommandFn
	m.mu.Unlock()
	return fn(ctx, req)
}

func (m *MockTransport) SendQuery(ctx context.Context, req *transport.SendQueryRequest) (*transport.SendQueryResult, error) {
	m.mu.Lock()
	m.SendQueryCalls++
	fn := m.sendQueryFn
	m.mu.Unlock()
	return fn(ctx, req)
}

func (m *MockTransport) SendResponse(ctx context.Context, req *transport.SendResponseRequest) error {
	m.mu.Lock()
	fn := m.sendResponseFn
	m.mu.Unlock()
	return fn(ctx, req)
}

func (m *MockTransport) SendQueueMessage(ctx context.Context, req *transport.QueueMessageItem) (*transport.SendQueueMessageResultItem, error) {
	m.mu.Lock()
	fn := m.sendQueueMsgFn
	m.mu.Unlock()
	return fn(ctx, req)
}

func (m *MockTransport) SendQueueMessages(ctx context.Context, req *transport.SendQueueMessagesRequest) (*transport.SendQueueMessagesResult, error) {
	m.mu.Lock()
	fn := m.sendQueueMsgsFn
	m.mu.Unlock()
	return fn(ctx, req)
}

func (m *MockTransport) ReceiveQueueMessages(ctx context.Context, req *transport.ReceiveQueueMessagesReq) (*transport.ReceiveQueueMessagesResp, error) {
	m.mu.Lock()
	fn := m.recvQueueMsgsFn
	m.mu.Unlock()
	return fn(ctx, req)
}

func (m *MockTransport) AckAllQueueMessages(ctx context.Context, req *transport.AckAllQueueMessagesReq) (*transport.AckAllQueueMessagesResp, error) {
	m.mu.Lock()
	fn := m.ackAllFn
	m.mu.Unlock()
	return fn(ctx, req)
}

func (m *MockTransport) SubscribeToEvents(ctx context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error) {
	m.mu.Lock()
	fn := m.subscribeToEventsFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return nil, nil
}

func (m *MockTransport) SubscribeToEventsStore(ctx context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error) {
	m.mu.Lock()
	fn := m.subscribeToEventsStoreFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return nil, nil
}

func (m *MockTransport) SubscribeToCommands(ctx context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error) {
	m.mu.Lock()
	fn := m.subscribeToCommandsFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return nil, nil
}

func (m *MockTransport) SubscribeToQueries(ctx context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error) {
	m.mu.Lock()
	fn := m.subscribeToQueriesFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return nil, nil
}

func (m *MockTransport) SendEventsStream(ctx context.Context) (*transport.EventStreamHandle, error) {
	m.mu.Lock()
	fn := m.sendEventsStreamFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	resultCh := make(chan *transport.EventStreamResult, 16)
	doneCh := make(chan struct{})
	return &transport.EventStreamHandle{
		Results: resultCh,
		Done:    doneCh,
		SendFn: func(item *transport.EventStreamItem) error {
			if item.Store {
				select {
				case resultCh <- &transport.EventStreamResult{EventID: item.ID, Sent: true}:
				default:
				}
			}
			return nil
		},
	}, nil
}

func (m *MockTransport) QueueUpstream(ctx context.Context) (*transport.QueueUpstreamHandle, error) {
	m.mu.Lock()
	fn := m.queueUpstreamFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return nil, nil
}

func (m *MockTransport) QueueDownstream(ctx context.Context, req *transport.QueueDownstreamRequest) (*transport.QueueDownstreamHandle, error) {
	m.mu.Lock()
	fn := m.queueDownstreamFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return nil, nil
}

func (m *MockTransport) CreateChannel(ctx context.Context, req *transport.CreateChannelRequest) error {
	m.mu.Lock()
	fn := m.createChannelFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return nil
}

func (m *MockTransport) DeleteChannel(ctx context.Context, req *transport.DeleteChannelRequest) error {
	m.mu.Lock()
	fn := m.deleteChannelFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return nil
}

func (m *MockTransport) ListChannels(ctx context.Context, req *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
	m.mu.Lock()
	fn := m.listChannelsFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return nil, nil
}

// OnSendEvent sets the handler for SendEvent calls.
func (m *MockTransport) OnSendEvent(fn func(ctx context.Context, req *transport.SendEventRequest) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendEventFn = fn
}

// OnPing sets the handler for Ping calls.
func (m *MockTransport) OnPing(fn func(ctx context.Context) (*transport.ServerInfoResult, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pingFn = fn
}

// OnClose sets the handler for Close calls.
func (m *MockTransport) OnClose(fn func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeFn = fn
}

// OnSendCommand sets the handler for SendCommand calls.
func (m *MockTransport) OnSendCommand(fn func(ctx context.Context, req *transport.SendCommandRequest) (*transport.SendCommandResult, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendCommandFn = fn
}

// OnSendQuery sets the handler for SendQuery calls.
func (m *MockTransport) OnSendQuery(fn func(ctx context.Context, req *transport.SendQueryRequest) (*transport.SendQueryResult, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendQueryFn = fn
}

// OnSubscribeToEvents sets the handler for SubscribeToEvents calls.
func (m *MockTransport) OnSubscribeToEvents(fn func(ctx context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribeToEventsFn = fn
}

// OnSubscribeToEventsStore sets the handler for SubscribeToEventsStore calls.
func (m *MockTransport) OnSubscribeToEventsStore(fn func(ctx context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribeToEventsStoreFn = fn
}

// OnSubscribeToCommands sets the handler for SubscribeToCommands calls.
func (m *MockTransport) OnSubscribeToCommands(fn func(ctx context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribeToCommandsFn = fn
}

// OnSubscribeToQueries sets the handler for SubscribeToQueries calls.
func (m *MockTransport) OnSubscribeToQueries(fn func(ctx context.Context, req *transport.SubscribeRequest) (*transport.StreamHandle, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribeToQueriesFn = fn
}

// OnCreateChannel sets the handler for CreateChannel calls.
func (m *MockTransport) OnCreateChannel(fn func(ctx context.Context, req *transport.CreateChannelRequest) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createChannelFn = fn
}

// OnDeleteChannel sets the handler for DeleteChannel calls.
func (m *MockTransport) OnDeleteChannel(fn func(ctx context.Context, req *transport.DeleteChannelRequest) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteChannelFn = fn
}

// OnListChannels sets the handler for ListChannels calls.
func (m *MockTransport) OnListChannels(fn func(ctx context.Context, req *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listChannelsFn = fn
}

// OnSendEventStore sets the handler for SendEventStore calls.
func (m *MockTransport) OnSendEventStore(fn func(ctx context.Context, req *transport.SendEventStoreRequest) (*transport.SendEventStoreResult, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendEventStoreFn = fn
}

// OnSendResponse sets the handler for SendResponse calls.
func (m *MockTransport) OnSendResponse(fn func(ctx context.Context, req *transport.SendResponseRequest) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendResponseFn = fn
}

// OnSendQueueMessage sets the handler for unary SendQueueMessage calls.
func (m *MockTransport) OnSendQueueMessage(fn func(ctx context.Context, req *transport.QueueMessageItem) (*transport.SendQueueMessageResultItem, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendQueueMsgFn = fn
}

// OnSendQueueMessages sets the handler for SendQueueMessages calls.
func (m *MockTransport) OnSendQueueMessages(fn func(ctx context.Context, req *transport.SendQueueMessagesRequest) (*transport.SendQueueMessagesResult, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendQueueMsgsFn = fn
}

// OnReceiveQueueMessages sets the handler for ReceiveQueueMessages calls.
func (m *MockTransport) OnReceiveQueueMessages(fn func(ctx context.Context, req *transport.ReceiveQueueMessagesReq) (*transport.ReceiveQueueMessagesResp, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recvQueueMsgsFn = fn
}

// OnAckAllQueueMessages sets the handler for AckAllQueueMessages calls.
func (m *MockTransport) OnAckAllQueueMessages(fn func(ctx context.Context, req *transport.AckAllQueueMessagesReq) (*transport.AckAllQueueMessagesResp, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackAllFn = fn
}

// OnSendEventsStream sets the handler for SendEventsStream calls.
func (m *MockTransport) OnSendEventsStream(fn func(ctx context.Context) (*transport.EventStreamHandle, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendEventsStreamFn = fn
}

// OnQueueUpstream sets the handler for QueueUpstream calls.
func (m *MockTransport) OnQueueUpstream(fn func(ctx context.Context) (*transport.QueueUpstreamHandle, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queueUpstreamFn = fn
}

// OnQueueDownstream sets the handler for QueueDownstream calls.
func (m *MockTransport) OnQueueDownstream(fn func(ctx context.Context, req *transport.QueueDownstreamRequest) (*transport.QueueDownstreamHandle, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queueDownstreamFn = fn
}
