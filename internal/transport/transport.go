// Package transport provides the internal transport interface and types.
// Only internal/transport/grpc.go implements it. The public Client holds
// a Transport and delegates through the middleware chain.
//
// This package is internal — external users cannot import it.
package transport

import (
	"context"
	"fmt"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	pb "github.com/kubemq-io/kubemq-go/v2/pb"
)

// Transport is the internal transport interface for communicating with the
// KubeMQ server. It operates on internal request/response types, not public
// API types. Conversion between public and internal types happens at the
// Client layer.
type Transport interface {
	// Lifecycle
	Close() error
	State() types.ConnectionState

	// Unary operations
	Ping(ctx context.Context) (*ServerInfoResult, error)
	SendEvent(ctx context.Context, req *SendEventRequest) error
	SendEventStore(ctx context.Context, req *SendEventStoreRequest) (*SendEventStoreResult, error)
	SendCommand(ctx context.Context, req *SendCommandRequest) (*SendCommandResult, error)
	SendQuery(ctx context.Context, req *SendQueryRequest) (*SendQueryResult, error)
	SendResponse(ctx context.Context, req *SendResponseRequest) error

	// Queue operations
	SendQueueMessage(ctx context.Context, req *QueueMessageItem) (*QueueSendResultItem, error)
	SendQueueMessages(ctx context.Context, req *SendQueueMessagesRequest) (*SendQueueMessagesResult, error)
	AckAllQueueMessages(ctx context.Context, req *AckAllQueueMessagesReq) (*AckAllQueueMessagesResp, error)

	// Streaming operations
	SubscribeToEvents(ctx context.Context, req *SubscribeRequest) (*StreamHandle, error)
	SubscribeToEventsStore(ctx context.Context, req *SubscribeRequest) (*StreamHandle, error)
	SubscribeToCommands(ctx context.Context, req *SubscribeRequest) (*StreamHandle, error)
	SubscribeToQueries(ctx context.Context, req *SubscribeRequest) (*StreamHandle, error)

	// Event streaming
	SendEventsStream(ctx context.Context) (*EventStreamHandle, error)

	// Queue streaming
	QueueUpstream(ctx context.Context) (*QueueUpstreamHandle, error)
	QueueDownstream(ctx context.Context) (pb.Kubemq_QueuesDownstreamClient, error)

	// Reconnection support
	WaitReconnect() <-chan struct{}
	IsConnectionError(err error) bool
	ClientID() string

	// Channel management
	CreateChannel(ctx context.Context, req *CreateChannelRequest) error
	DeleteChannel(ctx context.Context, req *DeleteChannelRequest) error
	ListChannels(ctx context.Context, req *ListChannelsRequest) ([]*ChannelInfo, error)
}

// StreamHandle is returned by Subscribe* methods. The caller reads messages
// from the channel and calls Close() when done.
type StreamHandle struct {
	Messages <-chan any
	Errors   <-chan error
	closeFn  func()
}

// Close terminates the subscription stream and releases resources.
func (h *StreamHandle) Close() {
	if h.closeFn != nil {
		h.closeFn()
	}
}

// NewStreamHandle creates a new StreamHandle.
func NewStreamHandle(messages <-chan any, errors <-chan error, closeFn func()) *StreamHandle {
	return &StreamHandle{
		Messages: messages,
		Errors:   errors,
		closeFn:  closeFn,
	}
}

// EventStreamHandle manages a bidirectional event send stream.
type EventStreamHandle struct {
	Results <-chan *EventStreamResult
	Done    <-chan struct{}
	SendFn  func(item *EventStreamItem) error
	closeFn func()
}

// Send sends an event on the stream.
func (h *EventStreamHandle) Send(item *EventStreamItem) error {
	if h.SendFn == nil {
		return fmt.Errorf("kubemq: event stream handle not initialized")
	}
	return h.SendFn(item)
}

// Close terminates the event stream.
func (h *EventStreamHandle) Close() {
	if h.closeFn != nil {
		h.closeFn()
	}
}

// QueueUpstreamHandle manages a bidirectional queue upstream (send) stream.
type QueueUpstreamHandle struct {
	Done    <-chan struct{}
	Results <-chan *QueueUpstreamResult
	Errors  <-chan error
	SendFn  func(requestID string, msgs []*QueueMessageItem) error
	closeFn func()
}

// Send publishes messages through the upstream stream.
func (h *QueueUpstreamHandle) Send(requestID string, msgs []*QueueMessageItem) error {
	if h.SendFn == nil {
		return fmt.Errorf("kubemq: upstream handle not initialized")
	}
	return h.SendFn(requestID, msgs)
}

// Close terminates the upstream stream.
func (h *QueueUpstreamHandle) Close() {
	if h.closeFn != nil {
		h.closeFn()
	}
}
