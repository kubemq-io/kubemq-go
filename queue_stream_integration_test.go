//go:build integration

package kubemq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sendMessages is a test helper that sends n queue messages to the given channel.
func sendMessages(t *testing.T, ctx context.Context, c *Client, channel string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		msg := NewQueueMessage().
			SetChannel(channel).
			SetBody([]byte(fmt.Sprintf("msg-%d", i))).
			SetMetadata(fmt.Sprintf("meta-%d", i)).
			AddTag("index", fmt.Sprintf("%d", i))
		result, err := c.SendQueueMessage(ctx, msg)
		require.NoError(t, err, "send message %d", i)
		require.False(t, result.IsError, "send message %d: %s", i, result.Error)
	}
}

// uniqueChannel returns a channel name unique to this test.
func uniqueChannel(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("qs-%s-%s-%d", t.Name(), suffix, time.Now().UnixNano())
}

func TestIntegration_QueueStream_BasicFlow(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "basic")

	// Send 1 message
	sendMessages(t, ctx, c, ch, 1)

	// Create receiver and poll
	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.False(t, resp.IsError, resp.Error)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "msg-0", string(resp.Messages[0].Message.Body))

	// Ack the message
	err = resp.Messages[0].Ack()
	require.NoError(t, err)

	// Poll again with short timeout — should be empty
	resp2, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 2,
	})
	require.NoError(t, err)
	assert.Empty(t, resp2.Messages, "expected no messages after ack")
}

func TestIntegration_QueueStream_BatchFlow(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "batch")

	// Send 10 messages
	sendMessages(t, ctx, c, ch, 10)

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           10,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.False(t, resp.IsError, resp.Error)
	require.Len(t, resp.Messages, 10)

	// AckAll
	err = resp.AckAll()
	require.NoError(t, err)
}

func TestIntegration_QueueStream_NackFlow(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "nack")

	// Send 1 message
	sendMessages(t, ctx, c, ch, 1)

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	// First poll
	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)

	// Nack — returns message to queue
	err = resp.Messages[0].Nack()
	require.NoError(t, err)

	// Poll again — message should be redelivered
	resp2, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp2.Messages, 1)
	assert.Equal(t, "msg-0", string(resp2.Messages[0].Message.Body))

	// ReceiveCount should be > 1 indicating redelivery
	require.NotNil(t, resp2.Messages[0].Message.Attributes)
	assert.Greater(t, resp2.Messages[0].Message.Attributes.ReceiveCount, 1,
		"expected ReceiveCount > 1 after nack redelivery")

	// Clean up: ack the redelivered message
	err = resp2.Messages[0].Ack()
	require.NoError(t, err)
}

func TestIntegration_QueueStream_NackAllFlow(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "nackall")

	// Send 3 messages
	sendMessages(t, ctx, c, ch, 3)

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	// First poll
	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           3,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 3)

	// NackAll
	err = resp.NackAll()
	require.NoError(t, err)

	// Poll again — all 3 should be redelivered
	resp2, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           3,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp2.Messages, 3)

	// Clean up
	err = resp2.AckAll()
	require.NoError(t, err)
}

func TestIntegration_QueueStream_ReQueueFlow(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	chA := uniqueChannel(t, "requeue-a")
	chB := uniqueChannel(t, "requeue-b")

	// Send to channel A
	sendMessages(t, ctx, c, chA, 1)

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	// Poll from A
	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            chA,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)

	// ReQueue to B
	err = resp.Messages[0].ReQueue(chB)
	require.NoError(t, err)

	// Poll from B — message should arrive
	resp2, err := receiver.Poll(ctx, &PollRequest{
		Channel:            chB,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp2.Messages, 1)
	assert.Equal(t, "msg-0", string(resp2.Messages[0].Message.Body))

	// Clean up
	err = resp2.Messages[0].Ack()
	require.NoError(t, err)
}

func TestIntegration_QueueStream_ReQueueAllFlow(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	chA := uniqueChannel(t, "requeueall-a")
	chB := uniqueChannel(t, "requeueall-b")

	// Send 2 to channel A
	sendMessages(t, ctx, c, chA, 2)

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	// Poll from A
	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            chA,
		MaxItems:           2,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 2)

	// ReQueueAll to B
	err = resp.ReQueueAll(chB)
	require.NoError(t, err)

	// Poll from B — both should arrive
	resp2, err := receiver.Poll(ctx, &PollRequest{
		Channel:            chB,
		MaxItems:           2,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp2.Messages, 2)

	// Clean up
	err = resp2.AckAll()
	require.NoError(t, err)
}

func TestIntegration_QueueStream_AutoAckFlow(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "autoack")

	// Send 1 message
	sendMessages(t, ctx, c, ch, 1)

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
		AutoAck:            true,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)

	// Settlement methods should return error since AutoAck was used
	err = resp.Messages[0].Ack()
	assert.Error(t, err, "Ack should fail on auto-acked message")

	err = resp.Messages[0].Nack()
	assert.Error(t, err, "Nack should fail on auto-acked message")

	err = resp.Messages[0].ReQueue("some-channel")
	assert.Error(t, err, "ReQueue should fail on auto-acked message")

	err = resp.AckAll()
	assert.Error(t, err, "AckAll should fail on auto-acked response")

	err = resp.NackAll()
	assert.Error(t, err, "NackAll should fail on auto-acked response")

	// Poll again — should be empty since message was auto-acked
	resp2, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 2,
	})
	require.NoError(t, err)
	assert.Empty(t, resp2.Messages, "expected no messages after auto-ack")
}

func TestIntegration_QueueStream_PollQueue(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "pollqueue")

	// Send 1 message
	sendMessages(t, ctx, c, ch, 1)

	// PollQueue is a convenience wrapper — always auto-acks
	resp, err := c.PollQueue(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "msg-0", string(resp.Messages[0].Message.Body))

	// Verify AutoAck was forced — settlement should fail
	err = resp.Messages[0].Ack()
	assert.Error(t, err, "Ack should fail because PollQueue forces AutoAck")

	// Poll again — should be empty
	resp2, err := c.PollQueue(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 2,
	})
	require.NoError(t, err)
	assert.Empty(t, resp2.Messages, "expected no messages after PollQueue auto-ack")
}

func TestIntegration_QueueStream_EmptyPoll(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "empty")

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	// Poll on an empty queue with short timeout
	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Messages, "expected no messages on empty queue")
}

func TestIntegration_QueueStream_MultiplePolls(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "multipolls")

	// Send 3 batches of 3 = 9 messages total
	sendMessages(t, ctx, c, ch, 9)

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	totalReceived := 0
	for i := 0; i < 3; i++ {
		resp, err := receiver.Poll(ctx, &PollRequest{
			Channel:            ch,
			MaxItems:           3,
			WaitTimeoutSeconds: 5,
		})
		require.NoError(t, err, "poll %d", i)
		require.Len(t, resp.Messages, 3, "poll %d should return 3 messages", i)
		totalReceived += len(resp.Messages)

		err = resp.AckAll()
		require.NoError(t, err, "ack poll %d", i)
	}
	assert.Equal(t, 9, totalReceived)
}

func TestIntegration_QueueStream_MixedSettlement(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	chMain := uniqueChannel(t, "mixed")
	chRequeue := uniqueChannel(t, "mixed-rq")

	// Send 3 messages
	sendMessages(t, ctx, c, chMain, 3)

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            chMain,
		MaxItems:           3,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 3)

	// Ack msg[0]
	err = resp.Messages[0].Ack()
	require.NoError(t, err)

	// Nack msg[1] — returns to queue
	err = resp.Messages[1].Nack()
	require.NoError(t, err)

	// ReQueue msg[2] to different channel
	err = resp.Messages[2].ReQueue(chRequeue)
	require.NoError(t, err)

	// Verify: poll main — should get nacked message only (msg[1])
	resp2, err := receiver.Poll(ctx, &PollRequest{
		Channel:            chMain,
		MaxItems:           3,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp2.Messages, 1, "only nacked message should remain on main channel")
	assert.Equal(t, "msg-1", string(resp2.Messages[0].Message.Body))
	err = resp2.AckAll()
	require.NoError(t, err)

	// Verify: poll requeue channel — should get msg[2]
	resp3, err := receiver.Poll(ctx, &PollRequest{
		Channel:            chRequeue,
		MaxItems:           3,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp3.Messages, 1, "requeued message should be on requeue channel")
	assert.Equal(t, "msg-2", string(resp3.Messages[0].Message.Body))
	err = resp3.AckAll()
	require.NoError(t, err)
}

func TestIntegration_QueueStream_ReceiverReuse(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "reuse")

	// Send 3 messages
	sendMessages(t, ctx, c, ch, 3)

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)

	// Poll 3 times, 1 message each
	for i := 0; i < 3; i++ {
		resp, err := receiver.Poll(ctx, &PollRequest{
			Channel:            ch,
			MaxItems:           1,
			WaitTimeoutSeconds: 5,
		})
		require.NoError(t, err, "poll %d", i)
		require.Len(t, resp.Messages, 1, "poll %d", i)
		err = resp.Messages[0].Ack()
		require.NoError(t, err, "ack poll %d", i)
	}

	// Close should not panic
	err = receiver.Close()
	assert.NoError(t, err)
}

func TestIntegration_QueueStream_CloseIdle(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)

	// Close immediately without polling — should not panic or hang
	done := make(chan struct{})
	go func() {
		_ = receiver.Close()
		close(done)
	}()

	select {
	case <-done:
		// success — Close returned
	case <-time.After(5 * time.Second):
		t.Fatal("receiver.Close() hung on idle receiver")
	}
}

func TestIntegration_QueueStream_CloseAfterPoll(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "closeafter")

	sendMessages(t, ctx, c, ch, 1)

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)

	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)

	// Ack the message
	err = resp.Messages[0].Ack()
	require.NoError(t, err)

	// Close should not hang
	done := make(chan struct{})
	go func() {
		_ = receiver.Close()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("receiver.Close() hung after poll")
	}
}

func TestIntegration_QueueStream_ConcurrentSettlements(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "concurrent")

	// Send 5 messages
	sendMessages(t, ctx, c, ch, 5)

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           5,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 5)

	// Spawn 5 goroutines each Acking one message concurrently
	// The -race flag will detect any data races
	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = resp.Messages[idx].Ack()
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		assert.NoError(t, e, "concurrent ack %d", i)
	}
}

func TestIntegration_QueueStream_MessageFields(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "fields")

	// Send message with specific fields
	msg := NewQueueMessage().
		SetChannel(ch).
		SetBody([]byte("test-body")).
		SetMetadata("test-metadata").
		AddTag("key1", "value1").
		AddTag("key2", "value2")
	result, err := c.SendQueueMessage(ctx, msg)
	require.NoError(t, err)
	require.False(t, result.IsError, result.Error)

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)

	dm := resp.Messages[0]
	require.NotNil(t, dm.Message)

	// Verify all fields are populated
	assert.Equal(t, "test-body", string(dm.Message.Body))
	assert.Equal(t, "test-metadata", dm.Message.Metadata)
	assert.Equal(t, ch, dm.Message.Channel)
	require.NotNil(t, dm.Message.Tags)
	assert.Equal(t, "value1", dm.Message.Tags["key1"])
	assert.Equal(t, "value2", dm.Message.Tags["key2"])
	assert.NotEmpty(t, dm.TransactionID)
	assert.NotZero(t, dm.Sequence)

	// Verify attributes are populated
	require.NotNil(t, dm.Message.Attributes)
	assert.NotZero(t, dm.Message.Attributes.Timestamp)
	assert.NotZero(t, dm.Message.Attributes.Sequence)
	assert.GreaterOrEqual(t, dm.Message.Attributes.ReceiveCount, 1)

	// Clean up
	err = dm.Ack()
	require.NoError(t, err)
}

func TestIntegration_QueueStream_WaitTimeout(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "timeout")

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	// Poll with 2-second timeout on empty queue
	start := time.Now()
	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 2,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Empty(t, resp.Messages)

	// Should take approximately 2 seconds (allow 1s-5s window)
	assert.GreaterOrEqual(t, elapsed, 1*time.Second,
		"poll should wait at least ~1s (got %v)", elapsed)
	assert.LessOrEqual(t, elapsed, 5*time.Second,
		"poll should not wait much longer than timeout (got %v)", elapsed)
}

func TestIntegration_QueueStream_ErrorChannel(t *testing.T) {
	c := newIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch := uniqueChannel(t, "errchan")

	receiver, err := c.NewQueueDownstreamReceiver(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = receiver.Close() })

	// Drain errors in background
	var receivedErrors []error
	var mu sync.Mutex
	errDone := make(chan struct{})
	go func() {
		defer close(errDone)
		for e := range receiver.Errors() {
			mu.Lock()
			receivedErrors = append(receivedErrors, e)
			mu.Unlock()
		}
	}()

	// Perform normal operations
	sendMessages(t, ctx, c, ch, 1)

	resp, err := receiver.Poll(ctx, &PollRequest{
		Channel:            ch,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	err = resp.Messages[0].Ack()
	require.NoError(t, err)

	// Close receiver to terminate error channel
	err = receiver.Close()
	require.NoError(t, err)

	// Wait for error goroutine to finish
	select {
	case <-errDone:
	case <-time.After(5 * time.Second):
		t.Fatal("error drain goroutine did not finish")
	}

	// During normal operation, there should be no errors
	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, receivedErrors, "expected no errors during normal operation, got: %v", receivedErrors)
}
