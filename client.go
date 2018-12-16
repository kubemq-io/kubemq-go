package kubemq

import (
	"context"
	"github.com/google/uuid"
	"time"
)

const (
	defaultRequestTimeout = time.Second * 5
)

type Client struct {
	opts      *Options
	transport Transport
}

func generateUUID() string {
	return uuid.New().String()
}

// NewClient - create client instance to be use to communicate with KubeMQ server
func NewClient(ctx context.Context, op ...Option) (*Client, error) {
	opts := GetDefaultOptions()
	for _, o := range op {
		o.apply(opts)
	}
	client := &Client{
		opts: opts,
	}
	var err error
	switch opts.transportType {
	case TransportTypeGRPC:
		client.transport, err = newGRPCTransport(ctx, opts)

	}
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Close - closing client connection. any on going transactions will be aborted
func (c *Client) Close() error {
	return c.transport.Close()
}

// E - create an empty event object
func (c *Client) E() *Event {
	return &Event{
		Id:        generateUUID(),
		Channel:   c.opts.defaultChannel,
		Metadata:  "",
		Body:      nil,
		ClientId:  c.opts.clientId,
		transport: c.transport,
	}
}

// ES - create an empty event store object
func (c *Client) ES() *EventStore {
	return &EventStore{
		Id:        generateUUID(),
		Channel:   c.opts.defaultChannel,
		Metadata:  "",
		Body:      nil,
		ClientId:  c.opts.clientId,
		transport: c.transport,
	}
}

// StreamEvents - send stream of events in a single call
func (c *Client) StreamEvents(ctx context.Context, eventsCh chan *Event, errCh chan error) {
	c.transport.StreamEvents(ctx, eventsCh, errCh)
}

// StreamEventsStore - send stream of events store in a single call
func (c *Client) StreamEventsStore(ctx context.Context, eventsCh chan *EventStore, eventsResultCh chan *EventStoreResult, errCh chan error) {
	c.transport.StreamEventsStore(ctx, eventsCh, eventsResultCh, errCh)
}

// C - create an empty command object
func (c *Client) C() *Command {
	return &Command{
		Id:        generateUUID(),
		Channel:   c.opts.defaultChannel,
		Metadata:  "",
		Body:      nil,
		Timeout:   defaultRequestTimeout,
		ClientId:  c.opts.clientId,
		transport: c.transport,
	}
}

// Q - create an empty query object
func (c *Client) Q() *Query {
	return &Query{
		Id:        generateUUID(),
		Channel:   c.opts.defaultChannel,
		Metadata:  "",
		Body:      nil,
		Timeout:   defaultRequestTimeout,
		ClientId:  c.opts.clientId,
		CacheKey:  "",
		CacheTTL:  c.opts.defaultCacheTTL,
		transport: c.transport,
	}
}

// R - create an empty response object for command or query responses
func (c *Client) R() *Response {
	return &Response{
		RequestId:  "",
		ResponseTo: "",
		Metadata:   "",
		Body:       nil,
		ClientId:   c.opts.clientId,
		ExecutedAt: time.Time{},
		Err:        nil,
		transport:  c.transport,
	}
}

// SubscribeToEvents - subscribe to events by channel and group. return channel of events or en error
func (c *Client) SubscribeToEvents(ctx context.Context, channel, group string, errCh chan error) (<-chan *Event, error) {
	return c.transport.SubscribeToEvents(ctx, channel, group, errCh)
}

// SubscribeToEventsStore - subscribe to events store by channel and group with subscription option. return channel of events or en error
func (c *Client) SubscribeToEventsStore(ctx context.Context, channel, group string, errCh chan error, opt SubscriptionOption) (<-chan *EventStoreReceive, error) {
	return c.transport.SubscribeToEventsStore(ctx, channel, group, errCh, opt)
}

// SubscribeToCommands - subscribe to commands requests by channel and group. return channel of CommandReceived or en error
func (c *Client) SubscribeToCommands(ctx context.Context, channel, group string, errCh chan error) (<-chan *CommandReceive, error) {
	return c.transport.SubscribeToCommands(ctx, channel, group, errCh)
}

// SubscribeToQueries - subscribe to queries requests by channel and group. return channel of QueryReceived or en error
func (c *Client) SubscribeToQueries(ctx context.Context, channel, group string, errCh chan error) (<-chan *QueryReceive, error) {
	return c.transport.SubscribeToQueries(ctx, channel, group, errCh)
}
