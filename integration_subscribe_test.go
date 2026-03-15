//go:build integration

package kubemq

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_EventsStoreRoundTrip(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := fmt.Sprintf("test-es-%d", time.Now().UnixNano())
	body := []byte("event store integration")

	_, err := c.SendEventStore(ctx, NewEventStore().
		SetChannel(ch).
		SetBody(body).
		SetMetadata("es-meta"))
	require.NoError(t, err)

	received := make(chan *EventStoreReceive, 1)
	sub, err := c.SubscribeToEventsStore(ctx, ch, "", StartFromFirstEvent(),
		WithOnEventStoreReceive(func(e *EventStoreReceive) {
			received <- e
		}),
		WithOnError(func(err error) {
			t.Logf("es subscription error: %v", err)
		}),
	)
	require.NoError(t, err)
	defer sub.Unsubscribe()

	select {
	case e := <-received:
		assert.Equal(t, ch, e.Channel)
		assert.Equal(t, "event store integration", string(e.Body))
		assert.Greater(t, e.Sequence, uint64(0))
	case <-ctx.Done():
		t.Fatal("timeout waiting for event store message")
	}
}

func TestIntegration_CommandRoundTrip(t *testing.T) {
	sender := newIntegrationClient(t)
	receiver := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := fmt.Sprintf("test-cmd-%d", time.Now().UnixNano())

	sub, err := receiver.SubscribeToCommands(ctx, ch, "",
		WithOnCommandReceive(func(cmd *CommandReceive) {
			_ = receiver.SendResponse(ctx, NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetBody([]byte("cmd-ack")))
		}),
		WithOnError(func(err error) {
			t.Logf("cmd subscription error: %v", err)
		}),
	)
	require.NoError(t, err)
	defer sub.Unsubscribe()

	time.Sleep(500 * time.Millisecond)

	resp, err := sender.SendCommand(ctx, NewCommand().
		SetChannel(ch).
		SetBody([]byte("do-something")).
		SetTimeout(10*time.Second))
	require.NoError(t, err)
	assert.True(t, resp.Executed)
}

func TestIntegration_QueryRoundTrip(t *testing.T) {
	sender := newIntegrationClient(t)
	receiver := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := fmt.Sprintf("test-query-%d", time.Now().UnixNano())

	sub, err := receiver.SubscribeToQueries(ctx, ch, "",
		WithOnQueryReceive(func(q *QueryReceive) {
			_ = receiver.SendResponse(ctx, NewResponse().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetBody([]byte("query-result")).
				SetMetadata("result-meta"))
		}),
		WithOnError(func(err error) {
			t.Logf("query subscription error: %v", err)
		}),
	)
	require.NoError(t, err)
	defer sub.Unsubscribe()

	time.Sleep(500 * time.Millisecond)

	resp, err := sender.SendQuery(ctx, NewQuery().
		SetChannel(ch).
		SetBody([]byte("get-data")).
		SetTimeout(10*time.Second))
	require.NoError(t, err)
	assert.True(t, resp.Executed)
	assert.Equal(t, "query-result", string(resp.Body))
}

func TestIntegration_SubscribeCancellation(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ch := fmt.Sprintf("test-cancel-%d", time.Now().UnixNano())
	subCtx, subCancel := context.WithCancel(ctx)

	sub, err := c.SubscribeToEvents(subCtx, ch, "",
		WithOnEvent(func(e *Event) {}),
		WithOnError(func(err error) {}),
	)
	require.NoError(t, err)
	assert.False(t, sub.IsDone())

	subCancel()

	select {
	case <-sub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not close after cancel")
	}
	assert.True(t, sub.IsDone())
}

func TestIntegration_SubscribeClosedClient(t *testing.T) {
	host, port := getTestAddress()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := NewClient(ctx,
		WithAddress(host, port),
		WithClientId(fmt.Sprintf("close-test-%d", time.Now().UnixNano())),
	)
	require.NoError(t, err)
	c.Close()

	_, err = c.SubscribeToEvents(context.Background(), "test-ch", "",
		WithOnEvent(func(e *Event) {}),
		WithOnError(func(err error) {}),
	)
	assert.Error(t, err)
}

func TestIntegration_AckAllQueueMessages(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := fmt.Sprintf("test-ackall-%d", time.Now().UnixNano())

	for i := 0; i < 3; i++ {
		_, err := c.SendQueueMessage(ctx, NewQueueMessage().
			SetChannel(ch).
			SetBody([]byte(fmt.Sprintf("msg-%d", i))))
		require.NoError(t, err)
	}

	resp, err := c.AckAllQueueMessages(ctx, &AckAllQueueMessagesRequest{
		Channel:         ch,
		WaitTimeSeconds: 5,
	})
	require.NoError(t, err)
	assert.False(t, resp.IsError, resp.Error)
	assert.GreaterOrEqual(t, int(resp.AffectedMessages), 3)
}

func TestIntegration_EventsSubscribeWithGroup(t *testing.T) {
	c1 := newIntegrationClient(t)
	c2 := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := fmt.Sprintf("test-group-%d", time.Now().UnixNano())
	group := "test-group"
	var count1, count2 atomic.Int32

	sub1, err := c1.SubscribeToEvents(ctx, ch, group,
		WithOnEvent(func(e *Event) { count1.Add(1) }),
		WithOnError(func(err error) {}),
	)
	require.NoError(t, err)
	defer sub1.Unsubscribe()

	sub2, err := c2.SubscribeToEvents(ctx, ch, group,
		WithOnEvent(func(e *Event) { count2.Add(1) }),
		WithOnError(func(err error) {}),
	)
	require.NoError(t, err)
	defer sub2.Unsubscribe()

	time.Sleep(500 * time.Millisecond)

	sender := newIntegrationClient(t)
	for i := 0; i < 10; i++ {
		err := sender.SendEvent(ctx, NewEvent().
			SetChannel(ch).
			SetBody([]byte(fmt.Sprintf("group-msg-%d", i))))
		require.NoError(t, err)
	}

	time.Sleep(2 * time.Second)
	total := count1.Load() + count2.Load()
	assert.Equal(t, int32(10), total, "all messages should be received across the group")
}

func TestIntegration_NewClientFromAddress(t *testing.T) {
	c, err := NewClientFromAddress("localhost:50000",
		WithClientId(fmt.Sprintf("addr-test-%d", time.Now().UnixNano())),
	)
	require.NoError(t, err)
	defer c.Close()

	info, err := c.Ping(context.Background())
	require.NoError(t, err)
	assert.NotEmpty(t, info.Host)
}

func TestIntegration_SendCommandDefaultTimeout(t *testing.T) {
	sender := newIntegrationClient(t)
	receiver := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := fmt.Sprintf("test-cmd-dt-%d", time.Now().UnixNano())

	sub, err := receiver.SubscribeToCommands(ctx, ch, "",
		WithOnCommandReceive(func(cmd *CommandReceive) {
			_ = receiver.SendResponse(ctx, NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo))
		}),
		WithOnError(func(err error) {}),
	)
	require.NoError(t, err)
	defer sub.Unsubscribe()

	time.Sleep(500 * time.Millisecond)

	cmd := NewCommand().
		SetChannel(ch).
		SetBody([]byte("timeout-test"))
	resp, err := sender.SendCommand(ctx, cmd)
	require.NoError(t, err)
	assert.True(t, resp.Executed)
}

func TestIntegration_SendQueryDefaultTimeout(t *testing.T) {
	sender := newIntegrationClient(t)
	receiver := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := fmt.Sprintf("test-query-dt-%d", time.Now().UnixNano())

	sub, err := receiver.SubscribeToQueries(ctx, ch, "",
		WithOnQueryReceive(func(q *QueryReceive) {
			_ = receiver.SendResponse(ctx, NewResponse().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetBody([]byte("result")))
		}),
		WithOnError(func(err error) {}),
	)
	require.NoError(t, err)
	defer sub.Unsubscribe()

	time.Sleep(500 * time.Millisecond)

	query := NewQuery().
		SetChannel(ch).
		SetBody([]byte("query-dt"))
	resp, err := sender.SendQuery(ctx, query)
	require.NoError(t, err)
	assert.True(t, resp.Executed)
}
