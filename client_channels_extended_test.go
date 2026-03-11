package kubemq

import (
	"context"
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateChannel_PropagatesClientID(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	c.opts.clientId = "test-client"
	ctx := context.Background()

	mt.OnCreateChannel(func(_ context.Context, req *transport.CreateChannelRequest) error {
		assert.Equal(t, "test-client", req.ClientID)
		assert.Equal(t, "my-ch", req.Channel)
		assert.Equal(t, ChannelTypeQueues, req.ChannelType)
		return nil
	})

	err := c.CreateChannel(ctx, "my-ch", ChannelTypeQueues)
	require.NoError(t, err)
}

func TestDeleteChannel_PropagatesClientID(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	c.opts.clientId = "del-client"
	ctx := context.Background()

	mt.OnDeleteChannel(func(_ context.Context, req *transport.DeleteChannelRequest) error {
		assert.Equal(t, "del-client", req.ClientID)
		assert.Equal(t, "old-ch", req.Channel)
		return nil
	})

	err := c.DeleteChannel(ctx, "old-ch", ChannelTypeCommands)
	require.NoError(t, err)
}

func TestListChannels_WithSearch(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	ctx := context.Background()

	mt.OnListChannels(func(_ context.Context, req *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		assert.Equal(t, "prefix", req.Search)
		return []*transport.ChannelInfo{
			{Name: "prefix-ch1", Type: "events", IsActive: true},
		}, nil
	})

	channels, err := c.ListChannels(ctx, ChannelTypeEvents, "prefix")
	require.NoError(t, err)
	require.Len(t, channels, 1)
	assert.Equal(t, "prefix-ch1", channels[0].Name)
}

func TestListChannels_WithStats(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	ctx := context.Background()

	mt.OnListChannels(func(_ context.Context, _ *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		return []*transport.ChannelInfo{
			{
				Name:     "ch-stats",
				IsActive: true,
				Incoming: &transport.ChannelStats{
					Messages: 100,
					Volume:   5000,
				},
				Outgoing: &transport.ChannelStats{
					Messages: 50,
				},
			},
		}, nil
	})

	channels, err := c.ListChannels(ctx, ChannelTypeEvents, "")
	require.NoError(t, err)
	require.Len(t, channels, 1)
	assert.NotNil(t, channels[0].Incoming)
	assert.Equal(t, int64(100), channels[0].Incoming.Messages)
	assert.NotNil(t, channels[0].Outgoing)
	assert.Equal(t, int64(50), channels[0].Outgoing.Messages)
}

func TestChannelTypeConstants(t *testing.T) {
	assert.Equal(t, "events", ChannelTypeEvents)
	assert.Equal(t, "events_store", ChannelTypeEventsStore)
	assert.Equal(t, "commands", ChannelTypeCommands)
	assert.Equal(t, "queries", ChannelTypeQueries)
	assert.Equal(t, "queues", ChannelTypeQueues)
}
