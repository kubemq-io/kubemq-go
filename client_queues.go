package kubemq

import (
	"context"
	"fmt"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/kubemq-io/kubemq-go/v2/pkg/uuid"
	"go.opentelemetry.io/otel/trace"
)

// SendQueueMessage sends a single message to a queue.
// Validates the message before sending.
func (c *Client) SendQueueMessage(ctx context.Context, msg *QueueMessage) (*SendQueueMessageResult, error) {
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
	return &SendQueueMessageResult{
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
func (c *Client) SendQueueMessages(ctx context.Context, msgs []*QueueMessage) ([]*SendQueueMessageResult, error) {
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

	out := make([]*SendQueueMessageResult, 0, len(result.Results))
	for _, r := range result.Results {
		out = append(out, &SendQueueMessageResult{
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

// ReceiveQueueMessages receives messages from a queue.
// Validates the channel before receiving.
func (c *Client) ReceiveQueueMessages(ctx context.Context, req *ReceiveQueueMessagesRequest) (*ReceiveQueueMessagesResponse, error) {
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
	if err := validateQueueReceive(req.MaxNumberOfMessages, req.WaitTimeSeconds); err != nil {
		return nil, err
	}
	tReq := &transport.ReceiveQueueMessagesReq{
		RequestID:           req.RequestID,
		ClientID:            req.ClientID,
		Channel:             req.Channel,
		MaxNumberOfMessages: req.MaxNumberOfMessages,
		WaitTimeSeconds:     req.WaitTimeSeconds,
		IsPeak:              req.IsPeak,
	}
	result, err := c.transport.ReceiveQueueMessages(ctx, tReq)
	if err != nil {
		return nil, err
	}

	msgs := make([]*QueueMessage, 0, len(result.Messages))
	for _, m := range result.Messages {
		msg := &QueueMessage{
			ID:       m.ID,
			ClientID: m.ClientID,
			Channel:  m.Channel,
			Metadata: m.Metadata,
			Body:     m.Body,
			Tags:     m.Tags,
		}
		if m.Policy != nil {
			msg.Policy = &QueuePolicy{
				ExpirationSeconds: m.Policy.ExpirationSeconds,
				DelaySeconds:      m.Policy.DelaySeconds,
				MaxReceiveCount:   m.Policy.MaxReceiveCount,
				MaxReceiveQueue:   m.Policy.MaxReceiveQueue,
			}
		}
		if m.Attributes != nil {
			msg.Attributes = &QueueMessageAttributes{
				Timestamp:         m.Attributes.Timestamp,
				Sequence:          m.Attributes.Sequence,
				MD5OfBody:         m.Attributes.MD5OfBody,
				ReceiveCount:      m.Attributes.ReceiveCount,
				ReRouted:          m.Attributes.ReRouted,
				ReRoutedFromQueue: m.Attributes.ReRoutedFromQueue,
				ExpirationAt:      m.Attributes.ExpirationAt,
				DelayedTo:         m.Attributes.DelayedTo,
			}
		}
		msgs = append(msgs, msg)
	}

	return &ReceiveQueueMessagesResponse{
		RequestID:        result.RequestID,
		Messages:         msgs,
		MessagesReceived: result.MessagesReceived,
		MessagesExpired:  result.MessagesExpired,
		IsPeak:           result.IsPeak,
		IsError:          result.IsError,
		Error:            result.Error,
	}, nil
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

	pubResultCh := make(chan *QueueUpstreamResult, 16)
	go func() {
		for r := range handle.Results {
			if r != nil {
				results := make([]*SendQueueMessageResult, 0, len(r.Results))
				for _, ri := range r.Results {
					results = append(results, &SendQueueMessageResult{
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
				default:
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

// QueueDownstream opens a bidirectional stream for receiving and managing queue messages.
// Returns a handle with Messages, Errors, Send, and Close.
func (c *Client) QueueDownstream(ctx context.Context) (*QueueDownstreamHandle, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	tHandle, err := c.transport.QueueDownstream(ctx, &transport.QueueDownstreamRequest{})
	if err != nil {
		return nil, err
	}

	sendDownstream := func(req *QueueDownstreamRequest) error {
		if req.RequestID == "" {
			req.RequestID = uuid.New()
		}
		waitTimeout := req.WaitTimeout
		if req.WaitTimeoutSeconds > 0 {
			waitTimeout = req.WaitTimeoutSeconds * 1000
		}
		return tHandle.Send(&transport.QueueDownstreamSendRequest{
			RequestID:        req.RequestID,
			ClientID:         req.ClientID,
			RequestType:      req.RequestType,
			Channel:          req.Channel,
			MaxItems:         req.MaxItems,
			WaitTimeout:      waitTimeout,
			AutoAck:          req.AutoAck,
			ReQueueChannel:   req.ReQueueChannel,
			SequenceRange:    req.SequenceRange,
			RefTransactionID: req.RefTransactionID,
			Metadata:         req.Metadata,
		})
	}

	msgCh := make(chan *QueueTransactionMessage, 16)
	go func() {
		for msg := range tHandle.Messages {
			if r, ok := msg.(*transport.QueueDownstreamResult); ok {
				msgs := make([]*QueueMessage, 0, len(r.Messages))
				for _, m := range r.Messages {
					qm := &QueueMessage{
						ID:       m.ID,
						ClientID: m.ClientID,
						Channel:  m.Channel,
						Metadata: m.Metadata,
						Body:     m.Body,
						Tags:     m.Tags,
					}
					if m.Policy != nil {
						qm.Policy = &QueuePolicy{
							ExpirationSeconds: m.Policy.ExpirationSeconds,
							DelaySeconds:      m.Policy.DelaySeconds,
							MaxReceiveCount:   m.Policy.MaxReceiveCount,
							MaxReceiveQueue:   m.Policy.MaxReceiveQueue,
						}
					}
					if m.Attributes != nil {
						qm.Attributes = &QueueMessageAttributes{
							Timestamp:         m.Attributes.Timestamp,
							Sequence:          m.Attributes.Sequence,
							MD5OfBody:         m.Attributes.MD5OfBody,
							ReceiveCount:      m.Attributes.ReceiveCount,
							ReRouted:          m.Attributes.ReRouted,
							ReRoutedFromQueue: m.Attributes.ReRoutedFromQueue,
							ExpirationAt:      m.Attributes.ExpirationAt,
							DelayedTo:         m.Attributes.DelayedTo,
						}
					}
					msgs = append(msgs, qm)
				}
				for _, qm := range msgs {
					select {
					case msgCh <- &QueueTransactionMessage{
						Message:       qm,
						TransactionID: r.TransactionID,
						RefRequestID:  r.RefRequestID,
						ActiveOffsets: r.ActiveOffsets,
						Metadata:      r.Metadata,
						sendFn:        sendDownstream,
					}:
					case <-ctx.Done():
						close(msgCh)
						return
					}
				}
			}
		}
		close(msgCh)
	}()

	return &QueueDownstreamHandle{
		Messages: msgCh,
		Errors:   tHandle.Errors,
		Send:     sendDownstream,
		Close:    tHandle.Close,
	}, nil
}

// PollQueue performs a single poll operation on a queue, returning messages.
// This is a high-level abstraction that opens a downstream stream, sends a Get request,
// reads the response, and closes the stream.
func (c *Client) PollQueue(ctx context.Context, req *QueuePollRequest) (*QueuePollResponse, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	if req.Channel != "" {
		if err := validateChannelStrict(req.Channel); err != nil {
			return nil, err
		}
	}

	handle, err := c.QueueDownstream(ctx)
	if err != nil {
		return nil, err
	}
	defer handle.Close()

	waitTimeout := req.WaitTimeout
	if req.WaitTimeoutSeconds > 0 {
		waitTimeout = req.WaitTimeoutSeconds * 1000
	}

	err = handle.Send(&QueueDownstreamRequest{
		RequestID:   uuid.New(),
		ClientID:    c.opts.clientId,
		RequestType: QueueDownstreamGet,
		Channel:     req.Channel,
		MaxItems:    req.MaxItems,
		WaitTimeout: waitTimeout,
		AutoAck:     req.AutoAck,
	})
	if err != nil {
		return nil, fmt.Errorf("poll queue send: %w", err)
	}

	resp := &QueuePollResponse{}
	select {
	case msg, ok := <-handle.Messages:
		if !ok {
			return resp, nil
		}
		resp.TransactionID = msg.TransactionID
		resp.Messages = append(resp.Messages, msg.Message)
		for {
			select {
			case m, ok := <-handle.Messages:
				if !ok {
					return resp, nil
				}
				resp.Messages = append(resp.Messages, m.Message)
			default:
				return resp, nil
			}
		}
	case err, ok := <-handle.Errors:
		if !ok {
			return resp, nil
		}
		resp.IsError = true
		resp.Error = err.Error()
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
