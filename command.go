package kubemq

import (
	"context"
	"github.com/kubemq-io/go/pb"
	"time"
)

type Command struct {
	Id       string
	Channel  string
	Metadata string
	Body     []byte
	Timeout  time.Duration
	ClientId string
	client   pb.KubemqClient
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

// Send - sending command , waiting for response or timeout
func (c *Command) Send(ctx context.Context) (*CommandResponse, error) {

	grpcRequest := &pb.Request{
		RequestID:       c.Id,
		RequestTypeData: pb.Command,
		ClientID:        c.ClientId,
		Channel:         c.Channel,
		Metadata:        c.Metadata,
		Body:            c.Body,
		Timeout:         int32(c.Timeout.Nanoseconds() / 1e6),
	}
	grpcResponse, err := c.client.SendRequest(ctx, grpcRequest)
	if err != nil {
		return nil, err
	}
	commandResponse := &CommandResponse{
		CommandId:        grpcResponse.RequestID,
		ResponseClientId: grpcResponse.ClientID,
		Executed:         grpcResponse.Executed,
		ExecutedAt:       time.Unix(grpcResponse.Timestamp, 0),
		Error:            grpcResponse.Error,
	}
	return commandResponse, nil
}

type CommandReceive struct {
	Id         string
	Channel    string
	Metadata   string
	Body       []byte
	ResponseTo string
}

type CommandResponse struct {
	CommandId        string
	ResponseClientId string
	Executed         bool
	ExecutedAt       time.Time
	Error            string
}
