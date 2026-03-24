package kubemq

import (
	"context"
	"fmt"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/kubemq-io/kubemq-go/v2/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
	"go.opentelemetry.io/otel/trace"
)

// SendQueueMessage sends a single message to a queue channel.
// Unlike events, queue messages are persistent — they are stored until a
// consumer receives and acknowledges them, or until they expire.
//
// Parameters:
//   - ctx: controls the deadline for the send operation. If the context expires
//     before the server acknowledges receipt, a TIMEOUT error is returned.
//   - msg: the queue message to send. QueueMessage.Channel is required unless
//     the client was created with WithDefaultChannel. QueueMessage.ID and
//     QueueMessage.ClientID are auto-populated if empty. At least one of Body
//     or Metadata must be set. An optional QueueMessage.Policy controls
//     expiration, delay, and dead-letter routing.
//
// Returns a *QueueSendResult containing the server-assigned message ID,
// send timestamp, and any computed expiration/delay times. Check
// QueueSendResult.IsError to detect server-side rejections (e.g., queue
// does not exist).
//
// Possible errors:
//   - VALIDATION: channel is empty, body and metadata are both nil/empty
//   - TRANSIENT: temporary network issue (retryable)
//   - TIMEOUT: operation exceeded ctx deadline
//   - AUTHENTICATION: invalid or missing auth token
//   - AUTHORIZATION: insufficient permissions for this channel
//   - BACKPRESSURE: server or client buffer is full (retryable after backoff)
//
// See also: QueueMessage, QueueSendResult, SendQueueMessages,
// PollQueue.
func (c *Client) SendQueueMessage(ctx context.Context, msg *QueueMessage) (*QueueSendResult, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	if msg.Channel == "" && c.opts.defaultChannel != "" {
		msg.Channel = c.opts.defaultChannel
	}
	if msg.ClientID == "" {
		msg.ClientID = c.opts.clientId
	}
	if msg.ID == "" {
		msg.ID = uuid.New()
	}
	if err := validateQueueMessage(msg, c.opts); err != nil {
		return nil, err
	}

	item := &transport.QueueMessageItem{
		ID:       msg.ID,
		ClientID: msg.ClientID,
		Channel:  msg.Channel,
		Metadata: msg.Metadata,
		Body:     msg.Body,
		Tags:     msg.Tags,
	}
	if msg.Policy != nil {
		item.Policy = &transport.QueueMessagePolicy{
			ExpirationSeconds: msg.Policy.ExpirationSeconds,
			DelaySeconds:      msg.Policy.DelaySeconds,
			MaxReceiveCount:   msg.Policy.MaxReceiveCount,
			MaxReceiveQueue:   msg.Policy.MaxReceiveQueue,
		}
	}

	channel := msg.Channel
	ctx, finish := c.otel.StartSpan(ctx, middleware.SpanConfig{
		Operation:  "queue_send",
		Channel:    channel,
		SpanKind:   trace.SpanKindProducer,
		ClientID:   c.opts.clientId,
		BatchCount: 1,
	})
	r, err := c.transport.SendQueueMessage(ctx, item)
	finish(err)
	if err != nil {
		return nil, err
	}
	return &QueueSendResult{
		MessageID:    r.MessageID,
		SentAt:       r.SentAt,
		ExpirationAt: r.ExpirationAt,
		DelayedTo:    r.DelayedTo,
		IsError:      r.IsError,
		Error:        r.Error,
	}, nil
}

// SendQueueMessages sends multiple messages to a queue in a batch.
// Validates each message before sending.
func (c *Client) SendQueueMessages(ctx context.Context, msgs []*QueueMessage) ([]*QueueSendResult, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	for _, m := range msgs {
		if m.Channel == "" && c.opts.defaultChannel != "" {
			m.Channel = c.opts.defaultChannel
		}
		if m.ClientID == "" {
			m.ClientID = c.opts.clientId
		}
		if m.ID == "" {
			m.ID = uuid.New()
		}
		if err := validateQueueMessage(m, c.opts); err != nil {
			return nil, err
		}
	}
	items := make([]*transport.QueueMessageItem, 0, len(msgs))
	for _, m := range msgs {
		item := &transport.QueueMessageItem{
			ID:       m.ID,
			ClientID: m.ClientID,
			Channel:  m.Channel,
			Metadata: m.Metadata,
			Body:     m.Body,
			Tags:     m.Tags,
		}
		if m.Policy != nil {
			item.Policy = &transport.QueueMessagePolicy{
				ExpirationSeconds: m.Policy.ExpirationSeconds,
				DelaySeconds:      m.Policy.DelaySeconds,
				MaxReceiveCount:   m.Policy.MaxReceiveCount,
				MaxReceiveQueue:   m.Policy.MaxReceiveQueue,
			}
		}
		items = append(items, item)
	}

	channel := ""
	if len(items) > 0 {
		channel = items[0].Channel
	}
	ctx, finish := c.otel.StartSpan(ctx, middleware.SpanConfig{
		Operation:  "queue_send",
		Channel:    channel,
		SpanKind:   trace.SpanKindProducer,
		ClientID:   c.opts.clientId,
		BatchCount: len(items),
	})
	result, err := c.transport.SendQueueMessages(ctx, &transport.SendQueueMessagesRequest{
		Messages: items,
	})
	finish(err)
	if err != nil {
		return nil, err
	}

	out := make([]*QueueSendResult, 0, len(result.Results))
	for _, r := range result.Results {
		out = append(out, &QueueSendResult{
			MessageID:    r.MessageID,
			SentAt:       r.SentAt,
			ExpirationAt: r.ExpirationAt,
			DelayedTo:    r.DelayedTo,
			IsError:      r.IsError,
			Error:        r.Error,
		})
	}
	return out, nil
}

// AckAllQueueMessages acknowledges all messages in a queue.
// Validates the channel before sending.
func (c *Client) AckAllQueueMessages(ctx context.Context, req *AckAllQueueMessagesRequest) (*AckAllQueueMessagesResponse, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	if req.ClientID == "" {
		req.ClientID = c.opts.clientId
	}
	if req.Channel != "" {
		if err := validateChannelStrict(req.Channel); err != nil {
			return nil, err
		}
	}
	tReq := &transport.AckAllQueueMessagesReq{
		RequestID:       req.RequestID,
		ClientID:        req.ClientID,
		Channel:         req.Channel,
		WaitTimeSeconds: req.WaitTimeSeconds,
	}
	result, err := c.transport.AckAllQueueMessages(ctx, tReq)
	if err != nil {
		return nil, err
	}
	return &AckAllQueueMessagesResponse{
		RequestID:        result.RequestID,
		AffectedMessages: result.AffectedMessages,
		IsError:          result.IsError,
		Error:            result.Error,
	}, nil
}

// QueueUpstream opens a bidirectional stream for high-throughput queue message publishing.
// Returns a handle with Send, Results, and Close.
func (c *Client) QueueUpstream(ctx context.Context) (*QueueUpstreamHandle, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	handle, err := c.transport.QueueUpstream(ctx)
	if err != nil {
		return nil, err
	}

	pubResultCh := make(chan *QueueUpstreamResult, 4096)
	streamCtx := ctx
	go func() {
		for r := range handle.Results {
			if r != nil {
				results := make([]*QueueSendResult, 0, len(r.Results))
				for _, ri := range r.Results {
					results = append(results, &QueueSendResult{
						MessageID:    ri.MessageID,
						SentAt:       ri.SentAt,
						ExpirationAt: ri.ExpirationAt,
						DelayedTo:    ri.DelayedTo,
						IsError:      ri.IsError,
						Error:        ri.Error,
					})
				}
				select {
				case pubResultCh <- &QueueUpstreamResult{
					RefRequestID: r.RefRequestID,
					Results:      results,
					IsError:      r.IsError,
					Error:        r.Error,
				}:
				case <-streamCtx.Done():
					return
				}
			}
		}
		close(pubResultCh)
	}()

	return &QueueUpstreamHandle{
		Send: func(requestID string, msgs []*QueueMessage) error {
			if requestID == "" {
				requestID = uuid.New()
			}
			items := make([]*transport.QueueMessageItem, 0, len(msgs))
			for _, m := range msgs {
				if m.ID == "" {
					m.ID = uuid.New()
				}
				if m.ClientID == "" {
					m.ClientID = c.opts.clientId
				}
				item := &transport.QueueMessageItem{
					ID:       m.ID,
					ClientID: m.ClientID,
					Channel:  m.Channel,
					Metadata: m.Metadata,
					Body:     m.Body,
					Tags:     m.Tags,
				}
				if m.Policy != nil {
					item.Policy = &transport.QueueMessagePolicy{
						ExpirationSeconds: m.Policy.ExpirationSeconds,
						DelaySeconds:      m.Policy.DelaySeconds,
						MaxReceiveCount:   m.Policy.MaxReceiveCount,
						MaxReceiveQueue:   m.Policy.MaxReceiveQueue,
					}
				}
				items = append(items, item)
			}
			return handle.Send(requestID, items)
		},
		Results: pubResultCh,
		Done:    handle.Done,
		Close:   handle.Close,
	}, nil
}

// NewQueueDownstreamReceiver creates a new QueueDownstreamReceiver for continuously
// polling queue messages. The receiver manages a persistent downstream stream with
// automatic reconnection support.
//
// Call Poll() in a loop to receive messages. Call Close() when done.
func (c *Client) NewQueueDownstreamReceiver(ctx context.Context) (*QueueDownstreamReceiver, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}

	receiverCtx, receiverCancel := context.WithCancel(ctx)

	stream, err := c.transport.QueueDownstream(receiverCtx)
	if err != nil {
		receiverCancel()
		return nil, fmt.Errorf("kubemq: queue downstream: %w", err)
	}

	log := Logger(noopLogger{})
	if c.opts.logger != nil {
		log = c.opts.logger
	}

	r := &QueueDownstreamReceiver{
		transport:   c.transport,
		clientID:    c.opts.clientId,
		logger:      log,
		ctx:         receiverCtx,
		cancel:      receiverCancel,
		stream:      stream,
		sendCh:      make(chan *pb.QueuesDownstreamRequest, 4096),
		responseCh:  make(chan *pb.QueuesDownstreamResponse, 1),
		errCh:       make(chan error, 8),
		stopCh:      make(chan struct{}),
		reconnectCh: make(chan struct{}),
		openTxns:    make(map[string]struct{}),
		openStream: func() (pb.Kubemq_QueuesDownstreamClient, error) {
			return c.transport.QueueDownstream(receiverCtx)
		},
	}

	go r.recvLoop()

	return r, nil
}

// PollQueue performs a single auto-ack poll operation on a queue.
// This is a convenience wrapper that creates a temporary receiver, polls once,
// and closes the receiver. For continuous polling, use NewQueueDownstreamReceiver.
func (c *Client) PollQueue(ctx context.Context, req *PollRequest) (*PollResponse, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	if req.Channel != "" {
		if err := validateChannelStrict(req.Channel); err != nil {
			return nil, err
		}
	}

	req.AutoAck = true
	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = receiver.Close() }()

	return receiver.Poll(ctx, req)
}

// NewQueueMessage creates a new QueueMessage pre-populated with client defaults.
func (c *Client) NewQueueMessage() *QueueMessage {
	return &QueueMessage{
		ClientID: c.opts.clientId,
		Tags:     map[string]string{},
	}
}

// NewQueueMessages creates an empty QueueMessages batch.
func (c *Client) NewQueueMessages() *QueueMessages {
	return &QueueMessages{
		Messages: []*QueueMessage{},
	}
}
