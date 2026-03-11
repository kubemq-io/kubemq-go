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
