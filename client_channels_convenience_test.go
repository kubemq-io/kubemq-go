package kubemq

import (
	"context"
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateEventsChannel(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnCreateChannel(func(_ context.Context, req *transport.CreateChannelRequest) error {
		assert.Equal(t, "ch1", req.Channel)
		assert.Equal(t, ChannelTypeEvents, req.ChannelType)
		return nil
	})
	require.NoError(t, c.CreateEventsChannel(context.Background(), "ch1"))
}

func TestCreateEventsStoreChannel(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnCreateChannel(func(_ context.Context, req *transport.CreateChannelRequest) error {
		assert.Equal(t, "ch1", req.Channel)
		assert.Equal(t, ChannelTypeEventsStore, req.ChannelType)
		return nil
	})
	require.NoError(t, c.CreateEventsStoreChannel(context.Background(), "ch1"))
}

func TestCreateCommandsChannel(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnCreateChannel(func(_ context.Context, req *transport.CreateChannelRequest) error {
		assert.Equal(t, "ch1", req.Channel)
		assert.Equal(t, ChannelTypeCommands, req.ChannelType)
		return nil
	})
	require.NoError(t, c.CreateCommandsChannel(context.Background(), "ch1"))
}

func TestCreateQueriesChannel(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnCreateChannel(func(_ context.Context, req *transport.CreateChannelRequest) error {
		assert.Equal(t, "ch1", req.Channel)
		assert.Equal(t, ChannelTypeQueries, req.ChannelType)
		return nil
	})
	require.NoError(t, c.CreateQueriesChannel(context.Background(), "ch1"))
}

func TestCreateQueuesChannel(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnCreateChannel(func(_ context.Context, req *transport.CreateChannelRequest) error {
		assert.Equal(t, "ch1", req.Channel)
		assert.Equal(t, ChannelTypeQueues, req.ChannelType)
		return nil
	})
	require.NoError(t, c.CreateQueuesChannel(context.Background(), "ch1"))
}

func TestDeleteEventsChannel(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnDeleteChannel(func(_ context.Context, req *transport.DeleteChannelRequest) error {
		assert.Equal(t, "ch1", req.Channel)
		assert.Equal(t, ChannelTypeEvents, req.ChannelType)
		return nil
	})
	require.NoError(t, c.DeleteEventsChannel(context.Background(), "ch1"))
}

func TestDeleteEventsStoreChannel(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnDeleteChannel(func(_ context.Context, req *transport.DeleteChannelRequest) error {
		assert.Equal(t, "ch1", req.Channel)
		assert.Equal(t, ChannelTypeEventsStore, req.ChannelType)
		return nil
	})
	require.NoError(t, c.DeleteEventsStoreChannel(context.Background(), "ch1"))
}

func TestDeleteCommandsChannel(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnDeleteChannel(func(_ context.Context, req *transport.DeleteChannelRequest) error {
		assert.Equal(t, "ch1", req.Channel)
		assert.Equal(t, ChannelTypeCommands, req.ChannelType)
		return nil
	})
	require.NoError(t, c.DeleteCommandsChannel(context.Background(), "ch1"))
}

func TestDeleteQueriesChannel(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnDeleteChannel(func(_ context.Context, req *transport.DeleteChannelRequest) error {
		assert.Equal(t, "ch1", req.Channel)
		assert.Equal(t, ChannelTypeQueries, req.ChannelType)
		return nil
	})
	require.NoError(t, c.DeleteQueriesChannel(context.Background(), "ch1"))
}

func TestDeleteQueuesChannel(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnDeleteChannel(func(_ context.Context, req *transport.DeleteChannelRequest) error {
		assert.Equal(t, "ch1", req.Channel)
		assert.Equal(t, ChannelTypeQueues, req.ChannelType)
		return nil
	})
	require.NoError(t, c.DeleteQueuesChannel(context.Background(), "ch1"))
}

func TestListEventsChannels(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnListChannels(func(_ context.Context, req *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		assert.Equal(t, ChannelTypeEvents, req.ChannelType)
		assert.Equal(t, "search", req.Search)
		return []*transport.ChannelInfo{{Name: "ev1"}}, nil
	})
	ch, err := c.ListEventsChannels(context.Background(), "search")
	require.NoError(t, err)
	require.Len(t, ch, 1)
	assert.Equal(t, "ev1", ch[0].Name)
}

func TestListEventsStoreChannels(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnListChannels(func(_ context.Context, req *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		assert.Equal(t, ChannelTypeEventsStore, req.ChannelType)
		return []*transport.ChannelInfo{{Name: "es1"}}, nil
	})
	ch, err := c.ListEventsStoreChannels(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, ch, 1)
	assert.Equal(t, "es1", ch[0].Name)
}

func TestListCommandsChannels(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnListChannels(func(_ context.Context, req *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		assert.Equal(t, ChannelTypeCommands, req.ChannelType)
		return []*transport.ChannelInfo{{Name: "cmd1"}}, nil
	})
	ch, err := c.ListCommandsChannels(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, ch, 1)
	assert.Equal(t, "cmd1", ch[0].Name)
}

func TestListQueriesChannels(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnListChannels(func(_ context.Context, req *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		assert.Equal(t, ChannelTypeQueries, req.ChannelType)
		return []*transport.ChannelInfo{{Name: "q1"}}, nil
	})
	ch, err := c.ListQueriesChannels(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, ch, 1)
	assert.Equal(t, "q1", ch[0].Name)
}

func TestListQueuesChannels(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnListChannels(func(_ context.Context, req *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		assert.Equal(t, ChannelTypeQueues, req.ChannelType)
		return []*transport.ChannelInfo{{Name: "qu1"}}, nil
	})
	ch, err := c.ListQueuesChannels(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, ch, 1)
	assert.Equal(t, "qu1", ch[0].Name)
}

func TestConvenienceWrappers_PropagateValidationErrors(t *testing.T) {
	c, _ := newTestClientWithMock(t)
	ctx := context.Background()

	// Create wrappers with empty name should fail
	assert.Error(t, c.CreateEventsChannel(ctx, ""))
	assert.Error(t, c.CreateEventsStoreChannel(ctx, ""))
	assert.Error(t, c.CreateCommandsChannel(ctx, ""))
	assert.Error(t, c.CreateQueriesChannel(ctx, ""))
	assert.Error(t, c.CreateQueuesChannel(ctx, ""))

	// Delete wrappers with empty name should fail
	assert.Error(t, c.DeleteEventsChannel(ctx, ""))
	assert.Error(t, c.DeleteEventsStoreChannel(ctx, ""))
	assert.Error(t, c.DeleteCommandsChannel(ctx, ""))
	assert.Error(t, c.DeleteQueriesChannel(ctx, ""))
	assert.Error(t, c.DeleteQueuesChannel(ctx, ""))
}

func TestConvenienceWrappers_ClosedClient(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	ctx := context.Background()

	// Close the client by setting transport to closed state and calling Close
	mt.OnClose(func() error { return nil })
	_ = c.Close()

	assert.Error(t, c.CreateEventsChannel(ctx, "ch"))
	assert.Error(t, c.DeleteEventsChannel(ctx, "ch"))
	_, err := c.ListEventsChannels(ctx, "")
	assert.Error(t, err)
}

func TestListChannels_NilStats(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnListChannels(func(_ context.Context, _ *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		return []*transport.ChannelInfo{
			{Name: "ch-no-stats", IsActive: false, Incoming: nil, Outgoing: nil},
		}, nil
	})
	ch, err := c.ListChannels(context.Background(), ChannelTypeEvents, "")
	require.NoError(t, err)
	require.Len(t, ch, 1)
	assert.Nil(t, ch[0].Incoming)
	assert.Nil(t, ch[0].Outgoing)
}

func TestListChannels_EmptyResult(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnListChannels(func(_ context.Context, _ *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		return []*transport.ChannelInfo{}, nil
	})
	ch, err := c.ListChannels(context.Background(), ChannelTypeQueues, "")
	require.NoError(t, err)
	assert.Empty(t, ch)
}

func TestListChannels_AllStatsFields(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	mt.OnListChannels(func(_ context.Context, _ *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		return []*transport.ChannelInfo{
			{
				Name:         "full-stats",
				Type:         "queues",
				LastActivity: 999,
				IsActive:     true,
				Incoming: &transport.ChannelStats{
					Messages: 10, Volume: 20, Responses: 30, Waiting: 40, Expired: 50, Delayed: 60,
				},
				Outgoing: &transport.ChannelStats{
					Messages: 11, Volume: 21, Responses: 31, Waiting: 41, Expired: 51, Delayed: 61,
				},
			},
		}, nil
	})
	ch, err := c.ListChannels(context.Background(), ChannelTypeQueues, "")
	require.NoError(t, err)
	require.Len(t, ch, 1)
	ci := ch[0]
	assert.Equal(t, int64(10), ci.Incoming.Messages)
	assert.Equal(t, int64(20), ci.Incoming.Volume)
	assert.Equal(t, int64(30), ci.Incoming.Responses)
	assert.Equal(t, int64(40), ci.Incoming.Waiting)
	assert.Equal(t, int64(50), ci.Incoming.Expired)
	assert.Equal(t, int64(60), ci.Incoming.Delayed)
	assert.Equal(t, int64(11), ci.Outgoing.Messages)
	assert.Equal(t, int64(21), ci.Outgoing.Volume)
	assert.Equal(t, int64(31), ci.Outgoing.Responses)
	assert.Equal(t, int64(41), ci.Outgoing.Waiting)
	assert.Equal(t, int64(51), ci.Outgoing.Expired)
	assert.Equal(t, int64(61), ci.Outgoing.Delayed)
}
