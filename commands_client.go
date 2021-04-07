package kubemq

import (
	"context"
	"fmt"
)

type CommandsMessageHandler func(receive *CommandReceive)
type CommandsErrorsHandler func(error)

type CommandsClient struct {
	client *Client
}

type CommandsSubscription struct {
	Channel string
	Group   string
	CommandsMessageHandler
	CommandsErrorsHandler
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
	return c.client.SetCommand(request).Send(ctx)
}
func (c *CommandsClient) Response(ctx context.Context, response *Response) error {
	return c.client.SetResponse(response).Send(ctx)
}
func (c *CommandsClient) Subscribe(ctx context.Context, channel, group string, onCommandReceive func(cmd *CommandReceive, err error)) error {
	if onCommandReceive == nil {
		return fmt.Errorf("commands request subscription callback function is required")
	}
	errCh := make(chan error, 1)
	commandsCh, err := c.client.SubscribeToCommands(ctx, channel, group, errCh)
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
