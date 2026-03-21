package kubemq

import (
	"fmt"
	"time"
)

// Command is an outbound command request. It is NOT safe for concurrent use —
// create a new Command for each send operation.
//
// Fields:
//   - Id: unique request identifier. Auto-generated UUID if empty at send time.
//   - Channel: target channel name (required unless WithDefaultChannel is set).
//   - Metadata: arbitrary string metadata delivered to the subscriber.
//   - Body: binary payload delivered to the subscriber.
//   - Timeout: server-side deadline for receiving a subscriber response. If zero,
//     defaults to 5s. The server returns a TIMEOUT error if no response arrives.
//   - ClientId: sender identifier. Auto-populated from client defaults if empty.
//   - Tags: key-value pairs delivered alongside the command.
//   - Span: OpenTelemetry span context for distributed tracing (internal use).
//
// See also: SendCommand, CommandResponse, CommandReceive.
type Command struct {
	Id       string
	Channel  string
	Metadata string
	Body     []byte
	Timeout  time.Duration
	ClientId string
	Tags     map[string]string
	Span     []byte
}

// NewCommand creates an empty Command.
func NewCommand() *Command {
	return &Command{}
}

// SetId sets the command request ID.
func (c *Command) SetId(id string) *Command {
	c.Id = id
	return c
}

// SetClientId sets the client identifier for this command.
func (c *Command) SetClientId(clientId string) *Command {
	c.ClientId = clientId
	return c
}

// SetChannel sets the target channel for this command.
func (c *Command) SetChannel(channel string) *Command {
	c.Channel = channel
	return c
}

// SetMetadata sets the command metadata.
func (c *Command) SetMetadata(metadata string) *Command {
	c.Metadata = metadata
	return c
}

// SetBody sets the command body payload.
func (c *Command) SetBody(body []byte) *Command {
	c.Body = body
	return c
}

// SetTimeout sets the timeout for waiting for a command response.
func (c *Command) SetTimeout(timeout time.Duration) *Command {
	c.Timeout = timeout
	return c
}

// SetTags replaces all tags on this command.
func (c *Command) SetTags(tags map[string]string) *Command {
	c.Tags = make(map[string]string, len(tags))
	for key, value := range tags {
		c.Tags[key] = value
	}
	return c
}

// AddTag adds a single key-value tag to this command.
func (c *Command) AddTag(key, value string) *Command {
	if c.Tags == nil {
		c.Tags = map[string]string{}
	}
	c.Tags[key] = value
	return c
}

// Validate checks all required fields and constraints.
// Called automatically before send operations; can also be called explicitly.
func (c *Command) Validate() error {
	return validateCommand(c, nil)
}

// CommandReceive is a received command request delivered to subscription
// callbacks. It is safe to read from multiple goroutines but must not be
// modified after receipt.
//
// Fields:
//   - Id: the original Command.Id set by the sender.
//   - ClientId: the sender's client identifier.
//   - Channel: the channel this command was published to.
//   - Metadata: string metadata from the sender.
//   - Body: binary payload from the sender.
//   - ResponseTo: internal routing address used by SendCommandResponse to deliver the
//     reply back to the sender. Pass this value to CommandReply.ResponseTo.
//   - Tags: key-value pairs from the sender.
//   - Span: OpenTelemetry span context for distributed tracing.
//
// See also: SubscribeToCommands, SendCommandResponse, CommandReply.
type CommandReceive struct {
	Id         string
	ClientId   string
	Channel    string
	Metadata   string
	Body       []byte
	ResponseTo string
	Tags       map[string]string
	Span       []byte
}

// String returns a human-readable representation of the received command.
func (cr *CommandReceive) String() string {
	return fmt.Sprintf("Id: %s, ClientId: %s, Channel: %s, Metadata: %s, Body: %s, ResponseTo: %s, Tags: %v",
		cr.Id, cr.ClientId, cr.Channel, cr.Metadata, string(cr.Body), cr.ResponseTo, cr.Tags)
}

// CommandResponse contains the result of a command execution returned by
// SendCommand. Immutable after construction. Safe to read from multiple
// goroutines.
//
// Fields:
//   - CommandId: the original Command.Id echoed back for correlation.
//   - ResponseClientId: the ClientId of the subscriber that handled the command.
//   - Executed: true if the subscriber indicated successful execution.
//   - ExecutedAt: timestamp when the subscriber processed the command.
//   - Error: non-empty if the subscriber reported a processing error. This is
//     the application-level error message set by the subscriber, not a transport
//     error.
//   - Tags: key-value pairs returned by the subscriber.
//
// See also: SendCommand, Command, SubscribeToCommands.
type CommandResponse struct {
	CommandId        string
	ResponseClientId string
	Executed         bool
	ExecutedAt       time.Time
	Error            string
	Tags             map[string]string
}

// String returns a human-readable representation of the command response.
func (cr *CommandResponse) String() string {
	return fmt.Sprintf("CommandId: %s, ResponseClientId: %s, Executed: %v, ExecutedAt: %s, Error: %s, Tags: %v",
		cr.CommandId, cr.ResponseClientId, cr.Executed, cr.ExecutedAt.String(), cr.Error, cr.Tags)
}
