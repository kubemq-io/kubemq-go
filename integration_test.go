//go:build integration

package kubemq

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getTestAddress() (string, int) {
	host := os.Getenv("KUBEMQ_HOST")
	if host == "" {
		host = "localhost"
	}
	return host, 50000
}

func newIntegrationClient(t *testing.T) *Client {
	t.Helper()
	host, port := getTestAddress()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := NewClient(ctx,
		WithAddress(host, port),
		WithClientId(fmt.Sprintf("test-%d", time.Now().UnixNano())),
	)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

func TestIntegration_Ping(t *testing.T) {
	c := newIntegrationClient(t)
	ctx := context.Background()

	info, err := c.Ping(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, info.Host)
	assert.NotEmpty(t, info.Version)
}

func TestIntegration_EventRoundTrip(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := fmt.Sprintf("test-events-%d", time.Now().UnixNano())
	received := make(chan *Event, 1)

	sub, err := c.SubscribeToEvents(ctx, ch, "",
		WithOnEvent(func(e *Event) {
			received <- e
		}),
		WithOnError(func(err error) {
			t.Logf("subscription error: %v", err)
		}),
	)
	require.NoError(t, err)
	defer sub.Unsubscribe()

	time.Sleep(500 * time.Millisecond)

	err = c.SendEvent(ctx, NewEvent().
		SetChannel(ch).
		SetBody([]byte("integration test")).
		SetMetadata("test"))
	require.NoError(t, err)

	select {
	case e := <-received:
		assert.Equal(t, ch, e.Channel)
		assert.Equal(t, "integration test", string(e.Body))
	case <-ctx.Done():
		t.Fatal("timeout waiting for event")
	}
}

func TestIntegration_ChannelManagement(t *testing.T) {
	c := newIntegrationClient(t)
	ctx := context.Background()

	ch := fmt.Sprintf("integ-ch-%d", time.Now().UnixNano())

	err := c.CreateChannel(ctx, ch, ChannelTypeEvents)
	require.NoError(t, err)

	channels, err := c.ListChannels(ctx, ChannelTypeEvents, ch)
	require.NoError(t, err)
	found := false
	for _, ci := range channels {
		if ci.Name == ch {
			found = true
			break
		}
	}
	assert.True(t, found, "created channel should appear in list")

	err = c.DeleteChannel(ctx, ch, ChannelTypeEvents)
	require.NoError(t, err)
}

func TestIntegration_QueueRoundTrip(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := fmt.Sprintf("test-queue-%d", time.Now().UnixNano())

	msg := NewQueueMessage().
		SetChannel(ch).
		SetBody([]byte("queue integration test"))

	result, err := c.SendQueueMessage(ctx, msg)
	require.NoError(t, err)
	assert.False(t, result.IsError, result.Error)

	resp, err := c.ReceiveQueueMessages(ctx, &ReceiveQueueMessagesRequest{
		Channel:             ch,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     5,
	})
	require.NoError(t, err)
	require.False(t, resp.IsError, resp.Error)
	require.GreaterOrEqual(t, len(resp.Messages), 1)
	assert.Equal(t, "queue integration test", string(resp.Messages[0].Body))
}
