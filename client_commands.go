package kubemq

import (
	"context"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/kubemq-io/kubemq-go/v2/pkg/uuid"
	"go.opentelemetry.io/otel/trace"
)

// SubscribeToCommands subscribes to commands on the given channel, returning a
// Subscription that delivers received commands via the WithOnCommandReceive
// handler and supports Unsubscribe(). Use SendResponse to reply to received commands.
//
// The group parameter enables load-balanced consumption across command handlers.
func (c *Client) SubscribeToCommands(ctx context.Context, channel, group string, opts ...SubscribeOption) (*Subscription, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateChannel(channel); err != nil {
		return nil, err
	}
	cfg := &subscribeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.onError == nil {
		cfg.onError = c.defaultErrorHandler
	}
	if cfg.onCommandReceive == nil {
		return nil, &KubeMQError{
			Code:      ErrCodeValidation,
			Message:   "WithOnCommandReceive handler is required for SubscribeToCommands",
			Operation: "SubscribeToCommands",
			Channel:   channel,
			Cause:     ErrValidation,
		}
	}

	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	handle, err := c.transport.SubscribeToCommands(subCtx, &transport.SubscribeRequest{
		Channel:  channel,
		Group:    group,
		ClientID: c.opts.clientId,
	})
	if err != nil {
		cancel()
		return nil, err
	}

	go func() {
		defer close(done)
		for {
			select {
			case msg, ok := <-handle.Messages:
				if !ok {
					return
				}
				if cmd, ok := msg.(*transport.CommandReceiveItem); ok {
					cfg.onCommandReceive(&CommandReceive{
						Id:         cmd.ID,
						ClientId:   cmd.ClientID,
						Channel:    cmd.Channel,
						Metadata:   cmd.Metadata,
						Body:       cmd.Body,
						ResponseTo: cmd.ResponseTo,
						Tags:       cmd.Tags,
					})
				}
			case err, ok := <-handle.Errors:
				if !ok {
					return
				}
				cfg.onError(err)
			case <-subCtx.Done():
				handle.Close()
				return
			}
		}
	}()

	return newSubscription(uuid.New(), cancel, done), nil
}

// SendCommand sends a command and waits for a response or timeout.
// Validates the command before sending.
func (c *Client) SendCommand(ctx context.Context, command *Command) (*CommandResponse, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	if command.Channel == "" && c.opts.defaultChannel != "" {
		command.Channel = c.opts.defaultChannel
	}
	if command.ClientId == "" {
		command.ClientId = c.opts.clientId
	}
	if command.Timeout == 0 {
		command.Timeout = defaultRequestTimeout
	}
	if err := validateCommand(command, c.opts); err != nil {
		return nil, err
	}
	ctx, finish := c.otel.StartSpan(ctx, middleware.SpanConfig{
		Operation: "send_command",
		Channel:   command.Channel,
		SpanKind:  trace.SpanKindClient,
		ClientID:  command.ClientId,
		MessageID: command.Id,
		BodySize:  len(command.Body),
	})
	req := &transport.SendCommandRequest{
		ID:       command.Id,
		ClientID: command.ClientId,
		Channel:  command.Channel,
		Metadata: command.Metadata,
		Body:     command.Body,
		Timeout:  command.Timeout,
		Tags:     command.Tags,
	}
	result, err := c.transport.SendCommand(ctx, req)
	finish(err)
	if err != nil {
		return nil, err
	}
	return &CommandResponse{
		CommandId:        result.CommandID,
		ResponseClientId: result.ResponseClientID,
		Executed:         result.Executed,
		ExecutedAt:       result.ExecutedAt,
		Error:            result.Error,
		Tags:             result.Tags,
	}, nil
}

// SendResponse sends a response to a received command or query.
// Validates the response before sending.
func (c *Client) SendResponse(ctx context.Context, response *Response) error {
	if err := c.checkClosed(); err != nil {
		return err
	}
	if response.ClientId == "" {
		response.ClientId = c.opts.clientId
	}
	if err := validateResponse(response); err != nil {
		return err
	}
	req := &transport.SendResponseRequest{
		RequestID:  response.RequestId,
		ResponseTo: response.ResponseTo,
		Metadata:   response.Metadata,
		Body:       response.Body,
		ClientID:   response.ClientId,
		ExecutedAt: response.ExecutedAt,
		Err:        response.Err,
		Tags:       response.Tags,
	}
	return c.transport.SendResponse(ctx, req)
}

// NewCommand creates a new Command pre-populated with client defaults.
func (c *Client) NewCommand() *Command {
	return &Command{
		ClientId: c.opts.clientId,
		Channel:  c.opts.defaultChannel,
		Timeout:  defaultRequestTimeout,
		Tags:     map[string]string{},
	}
}

// NewResponse creates a new Response pre-populated with client defaults.
func (c *Client) NewResponse() *Response {
	return &Response{
		ClientId: c.opts.clientId,
		Tags:     map[string]string{},
	}
}
