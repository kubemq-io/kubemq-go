package kubemq

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"
)

// NewClientFromAddress creates a KubeMQ client using a "host:port" address string.
// This is a convenience wrapper around NewClient for the minimal-code path.
//
// Example:
//
//	client, err := kubemq.NewClientFromAddress("localhost:50000")
func NewClientFromAddress(address string, opts ...Option) (*Client, error) {
	host, port, err := parseAddress(address)
	if err != nil {
		return nil, &KubeMQError{
			Code:      ErrCodeValidation,
			Message:   fmt.Sprintf("invalid address %q: %v", address, err),
			Operation: "NewClientFromAddress",
			Cause:     ErrValidation,
		}
	}
	allOpts := append([]Option{WithAddress(host, port)}, opts...)
	return NewClient(context.Background(), allOpts...)
}

func parseAddress(address string) (host string, port int, err error) {
	if address == "" {
		return "", 0, fmt.Errorf("address is required")
	}
	h, p, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, fmt.Errorf("expected host:port format: %w", err)
	}
	if h == "" {
		return "", 0, fmt.Errorf("host is required")
	}
	portNum, err := strconv.Atoi(p)
	if err != nil || portNum <= 0 || portNum > 65535 {
		return "", 0, fmt.Errorf("port must be between 1 and 65535, got %q", p)
	}
	return h, portNum, nil
}

// PublishOption configures convenience publish methods (PublishEvent, PublishEventStore).
type PublishOption interface {
	applyPublish(e *Event)
	applyPublishStore(es *EventStore)
}

type publishOption struct {
	fn      func(e *Event)
	fnStore func(es *EventStore)
}

func (o *publishOption) applyPublish(e *Event) {
	if o.fn != nil {
		o.fn(e)
	}
}
func (o *publishOption) applyPublishStore(es *EventStore) {
	if o.fnStore != nil {
		o.fnStore(es)
	}
}

// WithMetadata sets the metadata string on the published event.
func WithMetadata(metadata string) PublishOption {
	return &publishOption{
		fn:      func(e *Event) { e.Metadata = metadata },
		fnStore: func(es *EventStore) { es.Metadata = metadata },
	}
}

// WithTags sets key-value tags on the published event.
func WithTags(tags map[string]string) PublishOption {
	return &publishOption{
		fn:      func(e *Event) { e.Tags = tags },
		fnStore: func(es *EventStore) { es.Tags = tags },
	}
}

// WithID sets the message ID. If not set, a UUID is auto-generated.
func WithID(id string) PublishOption {
	return &publishOption{
		fn:      func(e *Event) { e.Id = id },
		fnStore: func(es *EventStore) { es.Id = id },
	}
}

// QueueSendOption configures convenience queue send methods.
type QueueSendOption interface {
	applyQueueSend(m *QueueMessage)
}

type queueSendOption struct {
	fn func(m *QueueMessage)
}

func (o *queueSendOption) applyQueueSend(m *QueueMessage) { o.fn(m) }

// WithExpiration sets the message expiration in seconds. 0 = never expires.
func WithExpiration(seconds int) QueueSendOption {
	return &queueSendOption{fn: func(m *QueueMessage) {
		if m.Policy == nil {
			m.Policy = &QueuePolicy{}
		}
		m.Policy.ExpirationSeconds = seconds
	}}
}

// WithDelay sets the message delivery delay in seconds. 0 = immediate.
func WithDelay(seconds int) QueueSendOption {
	return &queueSendOption{fn: func(m *QueueMessage) {
		if m.Policy == nil {
			m.Policy = &QueuePolicy{}
		}
		m.Policy.DelaySeconds = seconds
	}}
}

// WithMaxReceive sets the maximum delivery attempts before dead-lettering.
func WithMaxReceive(count int, deadLetterQueue string) QueueSendOption {
	return &queueSendOption{fn: func(m *QueueMessage) {
		if m.Policy == nil {
			m.Policy = &QueuePolicy{}
		}
		m.Policy.MaxReceiveCount = count
		m.Policy.MaxReceiveQueue = deadLetterQueue
	}}
}

// PublishEvent publishes a single event to the specified channel.
// This is the minimal-code path — for full control, use NewEvent() with the builder pattern.
//
// Example:
//
//	err := client.PublishEvent(ctx, "orders.events", []byte(`{"id": 123}`))
func (c *Client) PublishEvent(ctx context.Context, channel string, body []byte, opts ...PublishOption) error {
	event := c.NewEvent()
	event.Channel = channel
	event.Body = body
	for _, opt := range opts {
		opt.applyPublish(event)
	}
	if err := validateEvent(event, c.opts); err != nil {
		return err
	}
	return c.SendEvent(ctx, event)
}

// PublishEventStore publishes a single event to the events store on the specified channel.
//
// Example:
//
//	result, err := client.PublishEventStore(ctx, "orders.store", []byte(`{"id": 123}`))
func (c *Client) PublishEventStore(ctx context.Context, channel string, body []byte, opts ...PublishOption) (*EventStoreResult, error) {
	es := c.NewEventStore()
	es.Channel = channel
	es.Body = body
	for _, opt := range opts {
		opt.applyPublishStore(es)
	}
	if err := validateEventStore(es, c.opts); err != nil {
		return nil, err
	}
	return c.SendEventStore(ctx, es)
}

// SendQueueMessageSimple sends a single message to the specified queue channel.
// This is the minimal-code path convenience wrapper.
//
// Example:
//
//	result, err := client.SendQueueMessageSimple(ctx, "orders.queue", []byte(`{"id": 123}`))
func (c *Client) SendQueueMessageSimple(ctx context.Context, channel string, body []byte, opts ...QueueSendOption) (*SendQueueMessageResult, error) {
	msg := c.NewQueueMessage()
	msg.Channel = channel
	msg.Body = body
	for _, opt := range opts {
		opt.applyQueueSend(msg)
	}
	if err := validateQueueMessage(msg, c.opts); err != nil {
		return nil, err
	}
	return c.SendQueueMessage(ctx, msg)
}

// SendCommandSimple sends a command to the specified channel and waits for a response.
// This is the minimal-code path convenience wrapper.
//
// Example:
//
//	resp, err := client.SendCommandSimple(ctx, "orders.commands", []byte("process"), 10*time.Second)
func (c *Client) SendCommandSimple(ctx context.Context, channel string, body []byte, timeout time.Duration) (*CommandResponse, error) {
	cmd := c.NewCommand()
	cmd.Channel = channel
	cmd.Body = body
	cmd.Timeout = timeout
	if err := validateCommand(cmd, c.opts); err != nil {
		return nil, err
	}
	return c.SendCommand(ctx, cmd)
}

// SendQuerySimple sends a query to the specified channel and waits for a response.
// This is the minimal-code path convenience wrapper.
//
// Example:
//
//	resp, err := client.SendQuerySimple(ctx, "orders.queries", []byte("get-status"), 10*time.Second)
func (c *Client) SendQuerySimple(ctx context.Context, channel string, body []byte, timeout time.Duration) (*QueryResponse, error) {
	q := c.NewQuery()
	q.Channel = channel
	q.Body = body
	q.Timeout = timeout
	if err := validateQuery(q, c.opts); err != nil {
		return nil, err
	}
	return c.SendQuery(ctx, q)
}
