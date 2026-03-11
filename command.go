package kubemq

import (
	"fmt"
	"time"
)

// Command is an outbound command request. It is NOT safe for concurrent use —
// create a new Command for each send operation.
type Command struct {
	Id       string
	Channel  string
	Metadata string
	Body     []byte
	Timeout  time.Duration
	ClientId string
	Tags     map[string]string
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
type CommandReceive struct {
	Id         string
	ClientId   string
	Channel    string
	Metadata   string
	Body       []byte
	ResponseTo string
	Tags       map[string]string
}

// String returns a human-readable representation of the received command.
func (cr *CommandReceive) String() string {
	return fmt.Sprintf("Id: %s, ClientId: %s, Channel: %s, Metadata: %s, Body: %s, ResponseTo: %s, Tags: %v",
		cr.Id, cr.ClientId, cr.Channel, cr.Metadata, string(cr.Body), cr.ResponseTo, cr.Tags)
}

// CommandResponse contains the result of a command execution.
// Immutable after construction. Safe to read from multiple goroutines.
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
