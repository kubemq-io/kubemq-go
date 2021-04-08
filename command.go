package kubemq

import (
	"context"
	"time"
)

type Command struct {
	Id        string
	Channel   string
	Metadata  string
	Body      []byte
	Timeout   time.Duration
	ClientId  string
	Tags      map[string]string
	transport Transport
	trace     *Trace
}

func NewCommand() *Command {
	return &Command{}
}

// SetId - set command requestId, otherwise new random uuid will be set
func (c *Command) SetId(id string) *Command {
	c.Id = id
	return c
}

// SetClientId - set command ClientId - mandatory if default client was not set
func (c *Command) SetClientId(clientId string) *Command {
	c.ClientId = clientId
	return c
}

// SetChannel - set command channel - mandatory if default channel was not set
func (c *Command) SetChannel(channel string) *Command {
	c.Channel = channel
	return c
}

// SetMetadata - set command metadata - mandatory if body field is empty
func (c *Command) SetMetadata(metadata string) *Command {
	c.Metadata = metadata
	return c
}

// SetBody - set command body - mandatory if metadata field is empty
func (c *Command) SetBody(body []byte) *Command {
	c.Body = body
	return c
}

// SetTimeout - set timeout for command to be returned. if timeout expired , send command will result with an error
func (c *Command) SetTimeout(timeout time.Duration) *Command {
	c.Timeout = timeout
	return c
}

// SetTags - set key value tags to command message
func (c *Command) SetTags(tags map[string]string) *Command {
	c.Tags = map[string]string{}
	for key, value := range tags {
		c.Tags[key] = value
	}
	return c
}

// AddTag - add key value tags to command message
func (c *Command) AddTag(key, value string) *Command {
	if c.Tags == nil {
		c.Tags = map[string]string{}
	}
	c.Tags[key] = value
	return c
}

// AddTrace - add tracing support to command
func (c *Command) AddTrace(name string) *Trace {
	c.trace = CreateTrace(name)
	return c.trace
}

// Send - sending command , waiting for response or timeout
func (c *Command) Send(ctx context.Context) (*CommandResponse, error) {
	if c.transport == nil {
		return nil, ErrNoTransportDefined
	}
	return c.transport.SendCommand(ctx, c)
}

type CommandReceive struct {
	Id         string
	ClientId   string
	Channel    string
	Metadata   string
	Body       []byte
	ResponseTo string
	Tags       map[string]string
}

type CommandResponse struct {
	CommandId        string
	ResponseClientId string
	Executed         bool
	ExecutedAt       time.Time
	Error            string
	Tags             map[string]string
}
