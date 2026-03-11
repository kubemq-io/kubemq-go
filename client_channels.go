package kubemq

import (
	"context"

	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
)

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
	if channelType == "" {
		return &KubeMQError{
			Code:      ErrCodeValidation,
			Message:   "channel type is required",
			Operation: "CreateChannel",
			Cause:     ErrValidation,
		}
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
	if channelType == "" {
		return &KubeMQError{
			Code:      ErrCodeValidation,
			Message:   "channel type is required",
			Operation: "DeleteChannel",
			Cause:     ErrValidation,
		}
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
	if channelType == "" {
		return nil, &KubeMQError{
			Code:      ErrCodeValidation,
			Message:   "channel type is required",
			Operation: "ListChannels",
			Cause:     ErrValidation,
		}
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
