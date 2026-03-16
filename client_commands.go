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
// Parameters:
//   - ctx: parent context. Cancelling ctx tears down the subscription.
//   - channel: the exact channel name to subscribe to (required, no wildcards).
//   - group: enables load-balanced consumption across command handlers. When
//     multiple subscribers share the same group on the same channel, each
//     command is delivered to exactly one subscriber. Pass "" to receive all
//     commands (broadcast).
//   - opts: subscription options. WithOnCommandReceive is required — it sets the
//     callback for each received command. The handler must call SendResponse to
//     reply before Command.Timeout expires. WithOnError is optional.
//
// Returns a *Subscription on success. The subscription automatically reconnects
// on transient errors when auto-reconnect is enabled.
//
// Possible errors:
//   - VALIDATION: channel is empty, or WithOnCommandReceive handler is not provided
//   - TRANSIENT: temporary network issue establishing the subscription (retryable)
//   - AUTHENTICATION: invalid or missing auth token
//   - AUTHORIZATION: insufficient permissions for this channel
//   - CANCELLATION: ctx was cancelled before subscription was established
//
// See also: CommandReceive, SendCommand, SendResponse, Subscription.
func (c *Client) SubscribeToCommands(ctx context.Context, channel, group string, opts ...SubscribeOption) (*Subscription, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateChannelStrict(channel); err != nil {
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
						Span:       cmd.Span,
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

// SendCommand sends a command to a subscriber on the given channel and blocks
// until a response is received or the timeout expires. Commands implement a
// request-reply pattern: exactly one subscriber must handle the command and
// call SendResponse to reply.
//
// Parameters:
//   - ctx: controls the overall deadline. If ctx expires before a response
//     arrives, a context.DeadlineExceeded error is returned. The ctx deadline
//     is independent of Command.Timeout (whichever fires first wins).
//   - command: the command to send.
//   - Command.Channel: target channel (required unless WithDefaultChannel is set).
//   - Command.Timeout: server-side timeout for waiting for a subscriber response.
//     If zero, defaults to 5s. If no subscriber responds within this duration,
//     the server returns a TIMEOUT error. This is distinct from ctx — it
//     controls how long the server waits, not the client.
//   - Command.Id: auto-generated UUID if empty.
//   - Command.ClientId: auto-populated from client defaults if empty.
//   - At least one of Body or Metadata must be set.
//
// Returns a *CommandResponse on success. Check CommandResponse.Executed to
// determine if the subscriber successfully processed the command.
// CommandResponse.Error contains an error message from the subscriber if
// execution failed.
//
// Possible errors:
//   - VALIDATION: channel is empty, body and metadata are both nil/empty
//   - TRANSIENT: temporary network issue (retryable)
//   - TIMEOUT: no subscriber responded within Command.Timeout or ctx expired
//   - AUTHENTICATION: invalid or missing auth token
//   - AUTHORIZATION: insufficient permissions for this channel
//   - NOT_FOUND: no subscriber is listening on the channel
//   - CANCELLATION: ctx was cancelled before a response arrived
//
// See also: Command, CommandResponse, SubscribeToCommands, SendResponse.
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
	if command.Id == "" {
		command.Id = uuid.New()
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
		Span:     command.Span,
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

// SendResponse sends a response to a received command or query. This must be
// called by the subscriber within the sender's Timeout to deliver the reply.
//
// Parameters:
//   - ctx: controls the deadline for the send operation.
//   - response: the response to send. Response.RequestId must match the original
//     CommandReceive.Id or QueryReceive.Id. Response.ResponseTo must match the
//     CommandReceive.ResponseTo or QueryReceive.ResponseTo routing address.
//     Response.ClientId is auto-populated from client defaults if empty.
//
// Possible errors:
//   - VALIDATION: RequestId or ResponseTo is empty
//   - TRANSIENT: temporary network issue (retryable)
//   - TIMEOUT: operation exceeded ctx deadline
//   - AUTHENTICATION: invalid or missing auth token
//
// See also: Response, CommandReceive, QueryReceive, SubscribeToCommands,
// SubscribeToQueries.
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
		Span:       response.Span,
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
