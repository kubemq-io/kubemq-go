package kubemq

import (
	"context"
	"errors"
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/testutil"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestClientWithMock(t *testing.T) (*Client, *testutil.MockTransport) {
	t.Helper()
	mt := testutil.NewMockTransport()
	otel := middleware.NewOTelInterceptor(nil, nil, nil, "test", 0, middleware.CardinalityConfig{}, "test")
	c := &Client{
		opts:      GetDefaultOptions(),
		transport: mt,
		otel:      otel,
	}
	return c, mt
}

func TestCreateChannel_Validation(t *testing.T) {
	c, _ := newTestClientWithMock(t)
	ctx := context.Background()

	err := c.CreateChannel(ctx, "", ChannelTypeEvents)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)

	err = c.CreateChannel(ctx, "test", "")
	require.Error(t, err)
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestCreateChannel_Success(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	ctx := context.Background()

	called := false
	mt.OnCreateChannel(func(_ context.Context, req *transport.CreateChannelRequest) error {
		called = true
		assert.Equal(t, "my-channel", req.Channel)
		assert.Equal(t, ChannelTypeEvents, req.ChannelType)
		return nil
	})

	err := c.CreateChannel(ctx, "my-channel", ChannelTypeEvents)
	require.NoError(t, err)
	assert.True(t, called)
}

func TestDeleteChannel_Success(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	ctx := context.Background()

	called := false
	mt.OnDeleteChannel(func(_ context.Context, req *transport.DeleteChannelRequest) error {
		called = true
		assert.Equal(t, "my-channel", req.Channel)
		return nil
	})

	err := c.DeleteChannel(ctx, "my-channel", ChannelTypeQueues)
	require.NoError(t, err)
	assert.True(t, called)
}

func TestDeleteChannel_Validation(t *testing.T) {
	c, _ := newTestClientWithMock(t)
	ctx := context.Background()

	err := c.DeleteChannel(ctx, "", ChannelTypeEvents)
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))

	err = c.DeleteChannel(ctx, "test", "")
	require.Error(t, err)
	require.True(t, errors.As(err, &kErr))
}

func TestListChannels_Success(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	ctx := context.Background()

	mt.OnListChannels(func(_ context.Context, req *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		assert.Equal(t, ChannelTypeEvents, req.ChannelType)
		return []*transport.ChannelInfo{
			{Name: "ch1", Type: "events", IsActive: true},
			{Name: "ch2", Type: "events", IsActive: false},
		}, nil
	})

	channels, err := c.ListChannels(ctx, ChannelTypeEvents, "")
	require.NoError(t, err)
	require.Len(t, channels, 2)
	assert.Equal(t, "ch1", channels[0].Name)
	assert.True(t, channels[0].IsActive)
}

func TestListChannels_Validation(t *testing.T) {
	c, _ := newTestClientWithMock(t)
	ctx := context.Background()

	_, err := c.ListChannels(ctx, "", "")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestListChannels_TransportError(t *testing.T) {
	c, mt := newTestClientWithMock(t)
	ctx := context.Background()

	mt.OnListChannels(func(_ context.Context, _ *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		return nil, errors.New("connection lost")
	})

	_, err := c.ListChannels(ctx, ChannelTypeEvents, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection lost")
}

// --- Additional channel type validation tests ---

func TestValidateChannelType_AllValidTypes(t *testing.T) {
	validTypes := []string{
		ChannelTypeEvents,
		ChannelTypeEventsStore,
		ChannelTypeCommands,
		ChannelTypeQueries,
		ChannelTypeQueues,
	}
	for _, ct := range validTypes {
		t.Run(ct, func(t *testing.T) {
			err := validateChannelType(ct, "TestOp")
			assert.NoError(t, err)
		})
	}
}

func TestValidateChannelType_EmptyType(t *testing.T) {
	err := validateChannelType("", "TestOp")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "channel type is required")
	assert.Equal(t, "TestOp", kErr.Operation)
}

func TestValidateChannelType_InvalidType(t *testing.T) {
	err := validateChannelType("invalid_type", "CreateChannel")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "invalid channel type")
	assert.Contains(t, kErr.Message, "invalid_type")
}

func TestCreateChannel_InvalidChannelType(t *testing.T) {
	c, _ := newTestClientWithMock(t)
	err := c.CreateChannel(context.Background(), "my-channel", "invalid_type")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "invalid channel type")
}

func TestDeleteChannel_InvalidChannelType(t *testing.T) {
	c, _ := newTestClientWithMock(t)
	err := c.DeleteChannel(context.Background(), "my-channel", "bad_type")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "invalid channel type")
}

func TestListChannels_InvalidChannelType(t *testing.T) {
	c, _ := newTestClientWithMock(t)
	_, err := c.ListChannels(context.Background(), "wrong_type", "")
	require.Error(t, err)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
	assert.Contains(t, kErr.Message, "invalid channel type")
}

func TestCreateChannel_AllValidTypes(t *testing.T) {
	validTypes := []string{
		ChannelTypeEvents,
		ChannelTypeEventsStore,
		ChannelTypeCommands,
		ChannelTypeQueries,
		ChannelTypeQueues,
	}
	for _, ct := range validTypes {
		t.Run(ct, func(t *testing.T) {
			c, mt := newTestClientWithMock(t)
			mt.OnCreateChannel(func(_ context.Context, req *transport.CreateChannelRequest) error {
				assert.Equal(t, ct, req.ChannelType)
				return nil
			})
			err := c.CreateChannel(context.Background(), "test-ch", ct)
			assert.NoError(t, err)
		})
	}
}

func TestListChannels_WithDetailedStats(t *testing.T) {
	c, mt := newTestClientWithMock(t)

	mt.OnListChannels(func(_ context.Context, _ *transport.ListChannelsRequest) ([]*transport.ChannelInfo, error) {
		return []*transport.ChannelInfo{
			{
				Name:         "ch-with-stats",
				Type:         "events",
				IsActive:     true,
				LastActivity: 1234567890,
				Incoming: &transport.ChannelStats{
					Messages: 100, Volume: 5000, Responses: 0,
				},
				Outgoing: &transport.ChannelStats{
					Messages: 95, Volume: 4750, Waiting: 5,
				},
			},
		}, nil
	})

	channels, err := c.ListChannels(context.Background(), ChannelTypeEvents, "")
	require.NoError(t, err)
	require.Len(t, channels, 1)

	ch := channels[0]
	assert.Equal(t, "ch-with-stats", ch.Name)
	assert.True(t, ch.IsActive)
	assert.Equal(t, int64(1234567890), ch.LastActivity)

	require.NotNil(t, ch.Incoming)
	assert.Equal(t, int64(100), ch.Incoming.Messages)
	assert.Equal(t, int64(5000), ch.Incoming.Volume)

	require.NotNil(t, ch.Outgoing)
	assert.Equal(t, int64(95), ch.Outgoing.Messages)
	assert.Equal(t, int64(5), ch.Outgoing.Waiting)
}
