package kubemq

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/common"
)

// CommandsClient represents a client that can be used to send commands to a server.
// It contains a reference to the underlying client that handles the communication.
type CommandsClient struct {
	client *Client
}

// CommandsSubscription represents a subscription to commands requests by channel and group.
// It contains the following fields:
//   - Channel: the channel to subscribe to
//   - Group: the group to subscribe to
//   - ClientId: the ID of the client subscribing to the commands
//
// Usage example:
//
//	commandsSubscription := &CommandsSubscription{
//	  Channel:  "channel",
//	  Group:    "group",
//	  ClientId: "clientID",
//	}
//	err := commandsSubscription.Validate()
//	if err != nil {
//	  // handle validation error
//	}
//	commandsCh, err := client.SubscribeToCommands(context.Background(), commandsSubscription, errCh)
//	if err != nil {
//	  // handle subscribe error
//	}
//	for command := range commandsCh {
//	  // handle received command
//	}
//
// It also has the following methods:
//
// Complete(opts *Options) *CommandsSubscription: completes the commands subscription with the given options
// Validate() error: validates the commands subscription, ensuring that it has a channel and client ID
type CommandsSubscription struct {
	Channel  string
	Group    string
	ClientId string
}

// Complete method sets the `ClientId` field of the `CommandsSubscription` struct if it is empty.
// It takes an `Options` object as a parameter, and uses the `clientId` field of the `Options` object
// to set the `ClientId` field of `CommandsSubscription` if it is empty. It returns a pointer to the
// modified `CommandsSubscription` object.
//
// Example usage:
//
//	request := &CommandsSubscription{
//	    Channel: "my-channel",
//	    Group: "my-group",
//	}
//	options := &Options{
//	    clientId: "my-client-id",
//	}
//	request.Complete(options)
//	// Now the `ClientId` field of `request` will be set as "my-client-id" if it was empty.
func (cs *CommandsSubscription) Complete(opts *Options) *CommandsSubscription {
	if cs.ClientId == "" {
		cs.ClientId = opts.clientId
	}
	return cs
}

// Validate checks if a CommandsSubscription object has valid channel and clientId values.
// It returns an error if any of the required fields is empty. Otherwise, it returns nil.
func (cs *CommandsSubscription) Validate() error {
	if cs.Channel == "" {
		return fmt.Errorf("commands subscription must have a channel")
	}
	if cs.ClientId == "" {
		return fmt.Errorf("commands subscription must have a clientId")
	}
	return nil
}

// NewCommandsClient creates a new instance of CommandsClient with the provided context and options.
// It returns the created CommandsClient instance and an error if any.
func NewCommandsClient(ctx context.Context, op ...Option) (*CommandsClient, error) {
	client, err := NewClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	return &CommandsClient{
		client: client,
	}, nil
}

// Send sends a command using the provided context and command request. It checks if the client is ready to send the command. It sets the transport for the request and calls the Send method on the client to send the command. It returns the command response and any error that occurred during the process.
func (c *CommandsClient) Send(ctx context.Context, request *Command) (*CommandResponse, error) {
	if err := c.isClientReady(); err != nil {
		return nil, err
	}
	request.transport = c.client.transport
	return c.client.SetCommand(request).Send(ctx)
}

// Response sets the response object in the CommandsClient
// and sends the response using the client's transport.
//
// This method requires the client to be initialized.
//
// Parameters:
//
//	ctx: The context.Context object for the request.
//	response: The Response object to set and send.
//
// Returns:
//   - error: An error if the client is not ready or if sending the response fails.
func (c *CommandsClient) Response(ctx context.Context, response *Response) error {
	if err := c.isClientReady(); err != nil {
		return err
	}
	response.transport = c.client.transport
	return c.client.SetResponse(response).Send(ctx)
}

// Subscribe starts a subscription to receive commands from the server.
// It takes a context and a CommandsSubscription request as input,
// along with a callback function onCommandReceive that will be invoked
// whenever a command is received or an error occurs.
// It returns an error if the client is not ready,
// if the callback function is nil, or if the request fails validation.
// The Subscribe method launches a goroutine that listens for incoming
// commands and triggers the callback function accordingly.
func (c *CommandsClient) Subscribe(ctx context.Context, request *CommandsSubscription, onCommandReceive func(cmd *CommandReceive, err error)) error {
	if err := c.isClientReady(); err != nil {
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

// Create sends a request to create a channel of type "commands" with the given channel name.
//
// It returns an error if there was a failure in sending the create channel request, or if there was an error creating the channel.
func (c *CommandsClient) Create(ctx context.Context, channel string) error {
	return CreateChannel(ctx, c.client, c.client.opts.clientId, channel, "commands")
}

// Delete deletes a channel from the commands client.
//
// It sends a delete channel request to the KubeMQ server and returns an error if there is any.
// The function constructs a delete channel query, sets the required metadata and timeout, and makes the request through the client's query service.
// If the response contains an error message, it returns an error.
//
// ctx: The context.Context object for the request.
// channel: The name of the channel to be deleted.
//
// Returns:
// - nil if the channel was deleted successfully.
// - An error if the channel deletion failed.
func (c *CommandsClient) Delete(ctx context.Context, channel string) error {
	return DeleteChannel(ctx, c.client, c.client.opts.clientId, channel, "commands")
}

// List returns a list of CQChannels that match the given search criteria.
// It uses the ListCQChannels function to retrieve the data and decode it into CQChannel objects.
// The search parameter is optional and can be used to filter the results.
// The function requires a context, a client, a client ID, a channel type, and a search string.
// It returns a slice of CQChannel objects and an error if any occurred.
func (c *CommandsClient) List(ctx context.Context, search string) ([]*common.CQChannel, error) {
	return ListCQChannels(ctx, c.client, c.client.opts.clientId, "commands", search)
}

// Close closes the connection to the server by invoking the Close method of the underlying client.
// It returns an error if the close operation fails.
func (c *CommandsClient) Close() error {
	return c.client.Close()
}

// isClientReady checks if the client is ready for use. If the client is not
// initialized, it returns an error. If the client is initialized, it returns nil.
func (c *CommandsClient) isClientReady() error {
	if c.client == nil {
		return fmt.Errorf("client is not initialized")
	}
	return nil
}
