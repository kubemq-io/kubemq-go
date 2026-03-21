package kubemq

import (
	"context"
	"fmt"

	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
)

var validChannelTypes = map[string]bool{
	ChannelTypeEvents:      true,
	ChannelTypeEventsStore: true,
	ChannelTypeCommands:    true,
	ChannelTypeQueries:     true,
	ChannelTypeQueues:      true,
}

func validateChannelType(channelType, operation string) error {
	if channelType == "" {
		return &KubeMQError{
			Code:      ErrCodeValidation,
			Message:   "channel type is required",
			Operation: operation,
			Cause:     ErrValidation,
		}
	}
	if !validChannelTypes[channelType] {
		return &KubeMQError{
			Code:      ErrCodeValidation,
			Message:   fmt.Sprintf("invalid channel type %q; must be one of: events, events_store, commands, queries, queues", channelType),
			Operation: operation,
			Cause:     ErrValidation,
		}
	}
	return nil
}

// Channel type constants for use with CreateChannel, DeleteChannel, and ListChannels.
const (
	ChannelTypeEvents      = "events"
	ChannelTypeEventsStore = "events_store"
	ChannelTypeCommands    = "commands"
	ChannelTypeQueries     = "queries"
	ChannelTypeQueues      = "queues"
)

// ChannelInfo represents information about a KubeMQ channel.
type ChannelInfo struct {
	Name         string
	Type         string
	LastActivity int64
	IsActive     bool
	Incoming     *ChannelStats
	Outgoing     *ChannelStats
}

// ChannelStats holds directional statistics for a channel.
type ChannelStats struct {
	Messages  int64
	Volume    int64
	Responses int64
	Waiting   int64
	Expired   int64
	Delayed   int64
}

// CreateChannel creates a channel of the specified type.
// channelType must be one of: ChannelTypeEvents, ChannelTypeEventsStore,
// ChannelTypeCommands, ChannelTypeQueries, ChannelTypeQueues.
func (c *Client) CreateChannel(ctx context.Context, channelName, channelType string) error {
	if err := c.checkClosed(); err != nil {
		return err
	}
	if channelName == "" {
		return &KubeMQError{
			Code:      ErrCodeValidation,
			Message:   "channel name is required",
			Operation: "CreateChannel",
			Cause:     ErrValidation,
		}
	}
	if err := validateChannelType(channelType, "CreateChannel"); err != nil {
		return err
	}
	return c.transport.CreateChannel(ctx, &transport.CreateChannelRequest{
		ClientID:    c.opts.clientId,
		Channel:     channelName,
		ChannelType: channelType,
	})
}

// DeleteChannel deletes a channel of the specified type.
func (c *Client) DeleteChannel(ctx context.Context, channelName, channelType string) error {
	if err := c.checkClosed(); err != nil {
		return err
	}
	if channelName == "" {
		return &KubeMQError{
			Code:      ErrCodeValidation,
			Message:   "channel name is required",
			Operation: "DeleteChannel",
			Cause:     ErrValidation,
		}
	}
	if err := validateChannelType(channelType, "DeleteChannel"); err != nil {
		return err
	}
	return c.transport.DeleteChannel(ctx, &transport.DeleteChannelRequest{
		ClientID:    c.opts.clientId,
		Channel:     channelName,
		ChannelType: channelType,
	})
}

// ListChannels lists channels of the specified type, optionally filtered by search string.
func (c *Client) ListChannels(ctx context.Context, channelType, search string) ([]*ChannelInfo, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateChannelType(channelType, "ListChannels"); err != nil {
		return nil, err
	}
	result, err := c.transport.ListChannels(ctx, &transport.ListChannelsRequest{
		ClientID:    c.opts.clientId,
		ChannelType: channelType,
		Search:      search,
	})
	if err != nil {
		return nil, err
	}
	out := make([]*ChannelInfo, 0, len(result))
	for _, ch := range result {
		ci := &ChannelInfo{
			Name:         ch.Name,
			Type:         ch.Type,
			LastActivity: ch.LastActivity,
			IsActive:     ch.IsActive,
		}
		if ch.Incoming != nil {
			ci.Incoming = &ChannelStats{
				Messages:  ch.Incoming.Messages,
				Volume:    ch.Incoming.Volume,
				Responses: ch.Incoming.Responses,
				Waiting:   ch.Incoming.Waiting,
				Expired:   ch.Incoming.Expired,
				Delayed:   ch.Incoming.Delayed,
			}
		}
		if ch.Outgoing != nil {
			ci.Outgoing = &ChannelStats{
				Messages:  ch.Outgoing.Messages,
				Volume:    ch.Outgoing.Volume,
				Responses: ch.Outgoing.Responses,
				Waiting:   ch.Outgoing.Waiting,
				Expired:   ch.Outgoing.Expired,
				Delayed:   ch.Outgoing.Delayed,
			}
		}
		out = append(out, ci)
	}
	return out, nil
}

// CreateEventsChannel creates an events channel with the given name.
func (c *Client) CreateEventsChannel(ctx context.Context, name string) error {
	return c.CreateChannel(ctx, name, ChannelTypeEvents)
}

// CreateEventsStoreChannel creates an events-store channel with the given name.
func (c *Client) CreateEventsStoreChannel(ctx context.Context, name string) error {
	return c.CreateChannel(ctx, name, ChannelTypeEventsStore)
}

// CreateCommandsChannel creates a commands channel with the given name.
func (c *Client) CreateCommandsChannel(ctx context.Context, name string) error {
	return c.CreateChannel(ctx, name, ChannelTypeCommands)
}

// CreateQueriesChannel creates a queries channel with the given name.
func (c *Client) CreateQueriesChannel(ctx context.Context, name string) error {
	return c.CreateChannel(ctx, name, ChannelTypeQueries)
}

// CreateQueuesChannel creates a queues channel with the given name.
func (c *Client) CreateQueuesChannel(ctx context.Context, name string) error {
	return c.CreateChannel(ctx, name, ChannelTypeQueues)
}

// DeleteEventsChannel deletes an events channel with the given name.
func (c *Client) DeleteEventsChannel(ctx context.Context, name string) error {
	return c.DeleteChannel(ctx, name, ChannelTypeEvents)
}

// DeleteEventsStoreChannel deletes an events-store channel with the given name.
func (c *Client) DeleteEventsStoreChannel(ctx context.Context, name string) error {
	return c.DeleteChannel(ctx, name, ChannelTypeEventsStore)
}

// DeleteCommandsChannel deletes a commands channel with the given name.
func (c *Client) DeleteCommandsChannel(ctx context.Context, name string) error {
	return c.DeleteChannel(ctx, name, ChannelTypeCommands)
}

// DeleteQueriesChannel deletes a queries channel with the given name.
func (c *Client) DeleteQueriesChannel(ctx context.Context, name string) error {
	return c.DeleteChannel(ctx, name, ChannelTypeQueries)
}

// DeleteQueuesChannel deletes a queues channel with the given name.
func (c *Client) DeleteQueuesChannel(ctx context.Context, name string) error {
	return c.DeleteChannel(ctx, name, ChannelTypeQueues)
}

// ListEventsChannels lists events channels, optionally filtered by search string.
func (c *Client) ListEventsChannels(ctx context.Context, search string) ([]*ChannelInfo, error) {
	return c.ListChannels(ctx, ChannelTypeEvents, search)
}

// ListEventsStoreChannels lists events-store channels, optionally filtered by search string.
func (c *Client) ListEventsStoreChannels(ctx context.Context, search string) ([]*ChannelInfo, error) {
	return c.ListChannels(ctx, ChannelTypeEventsStore, search)
}

// ListCommandsChannels lists commands channels, optionally filtered by search string.
func (c *Client) ListCommandsChannels(ctx context.Context, search string) ([]*ChannelInfo, error) {
	return c.ListChannels(ctx, ChannelTypeCommands, search)
}

// ListQueriesChannels lists queries channels, optionally filtered by search string.
func (c *Client) ListQueriesChannels(ctx context.Context, search string) ([]*ChannelInfo, error) {
	return c.ListChannels(ctx, ChannelTypeQueries, search)
}

// ListQueuesChannels lists queues channels, optionally filtered by search string.
func (c *Client) ListQueuesChannels(ctx context.Context, search string) ([]*ChannelInfo, error) {
	return c.ListChannels(ctx, ChannelTypeQueues, search)
}
