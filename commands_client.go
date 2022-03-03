package kubemq

import (
	"context"
	"fmt"
)

type CommandsClient struct {
	client *Client
}

type CommandsSubscription struct {
	Channel  string
	Group    string
	ClientId string
}

func (cs *CommandsSubscription) Complete(opts *Options) *CommandsSubscription {
	if cs.ClientId == "" {
		cs.ClientId = opts.clientId
	}
	return cs
}
func (cs *CommandsSubscription) Validate() error {
	if cs.Channel == "" {
		return fmt.Errorf("commands subscription must have a channel")
	}
	if cs.ClientId == "" {
		return fmt.Errorf("commands subscription must have a clientId")
	}
	return nil
}

func NewCommandsClient(ctx context.Context, op ...Option) (*CommandsClient, error) {
	client, err := NewClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	return &CommandsClient{
		client: client,
	}, nil
}

func (c *CommandsClient) Send(ctx context.Context, request *Command) (*CommandResponse, error) {
	if err:=c.isClientReady();err!=nil{
		return nil,err
	}
	request.transport = c.client.transport
	return c.client.SetCommand(request).Send(ctx)
}
func (c *CommandsClient) Response(ctx context.Context, response *Response) error {
	if err:=c.isClientReady();err!=nil{
		return err
	}
	response.transport = c.client.transport
	return c.client.SetResponse(response).Send(ctx)
}
func (c *CommandsClient) Subscribe(ctx context.Context, request *CommandsSubscription, onCommandReceive func(cmd *CommandReceive, err error)) error {
	if err:=c.isClientReady();err!=nil{
		return err
	}
	if onCommandReceive == nil {
		return fmt.Errorf("commands request subscription callback function is required")
	}
	if err := request.Complete(c.client.opts).Validate(); err != nil {
		return err
	}
	errCh := make(chan error, 1)
	commandsCh, err := c.client.SubscribeToCommandsWithRequest(ctx, request, errCh)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case cmd := <-commandsCh:
				onCommandReceive(cmd, nil)
			case err := <-errCh:
				onCommandReceive(nil, err)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (c *CommandsClient) Close() error {
	return c.client.Close()
}


func (c *CommandsClient) isClientReady() error {
	if c.client==nil {
		return fmt.Errorf("client is not initialized")
	}
	return nil
}
