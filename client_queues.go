package kubemq

import (
	"context"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
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
	if err := validateQueueMessage(msg, c.opts); err != nil {
		return nil, err
	}
	results, err := c.SendQueueMessages(ctx, []*QueueMessage{msg})
	if err != nil {
		return nil, err
	}
	if len(results) > 0 {
		return results[0], nil
	}
	return nil, nil
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
		if err := validateChannel(req.Channel); err != nil {
			return nil, err
		}
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
		if err := validateChannel(req.Channel); err != nil {
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

// QueuesInfo returns information about queues matching the filter.
func (c *Client) QueuesInfo(ctx context.Context, filter string) (*QueuesInfo, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	result, err := c.transport.QueuesInfo(ctx, filter)
	if err != nil {
		return nil, err
	}
	out := &QueuesInfo{
		TotalQueue: result.TotalQueue,
		Sent:       result.Sent,
		Delivered:  result.Delivered,
		Waiting:    result.Waiting,
	}
	for _, q := range result.Queues {
		out.Queues = append(out.Queues, &QueueInfo{
			Name:        q.Name,
			Messages:    q.Messages,
			Bytes:       q.Bytes,
			FirstSeq:    q.FirstSeq,
			LastSeq:     q.LastSeq,
			Sent:        q.Sent,
			Delivered:   q.Delivered,
			Waiting:     q.Waiting,
			Subscribers: q.Subscribers,
		})
	}
	return out, nil
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
