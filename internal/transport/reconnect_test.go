package transport

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRingBufferEnqueueAndDrain(t *testing.T) {
	rb := newRingBuffer(3)

	var order []int
	for i := 0; i < 3; i++ {
		n := i
		err := rb.enqueue(bufferedMessage{
			sendFn: func(ctx context.Context) error {
				order = append(order, n)
				return nil
			},
		})
		require.NoError(t, err)
	}

	assert.Equal(t, 3, rb.count())

	msgs := rb.drainAll()
	assert.Len(t, msgs, 3)
	assert.Equal(t, 0, rb.count())

	ctx := context.Background()
	for _, msg := range msgs {
		_ = msg.sendFn(ctx)
	}
	assert.Equal(t, []int{0, 1, 2}, order, "FIFO order must be preserved")
}

func TestRingBufferOverflow(t *testing.T) {
	rb := newRingBuffer(2)

	err := rb.enqueue(bufferedMessage{sendFn: func(ctx context.Context) error { return nil }})
	require.NoError(t, err)

	err = rb.enqueue(bufferedMessage{sendFn: func(ctx context.Context) error { return nil }})
	require.NoError(t, err)

	err = rb.enqueue(bufferedMessage{sendFn: func(ctx context.Context) error { return nil }})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "buffer full")
}

func TestRingBufferDrainEmpty(t *testing.T) {
	rb := newRingBuffer(10)
	msgs := rb.drainAll()
	assert.Empty(t, msgs)
}

func TestReconnectLoopComputeDelay(t *testing.T) {
	rl := &reconnectLoop{
		policy: types.ReconnectPolicy{
			InitialDelay: 1 * time.Second,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			JitterMode:   types.JitterNone,
		},
	}

	assert.Equal(t, 1*time.Second, rl.computeDelay(1))
	assert.Equal(t, 2*time.Second, rl.computeDelay(2))
	assert.Equal(t, 4*time.Second, rl.computeDelay(3))
	assert.Equal(t, 8*time.Second, rl.computeDelay(4))
	assert.Equal(t, 16*time.Second, rl.computeDelay(5))
	assert.Equal(t, 30*time.Second, rl.computeDelay(6)) // capped at MaxDelay
}

func TestReconnectLoopComputeDelayFullJitter(t *testing.T) {
	rl := &reconnectLoop{
		policy: types.ReconnectPolicy{
			InitialDelay: 1 * time.Second,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			JitterMode:   types.JitterFull,
		},
	}

	for i := 0; i < 100; i++ {
		d := rl.computeDelay(1)
		assert.GreaterOrEqual(t, d, time.Duration(0))
		assert.LessOrEqual(t, d, 1*time.Second)
	}
}

func TestReconnectLoopComputeDelayEqualJitter(t *testing.T) {
	rl := &reconnectLoop{
		policy: types.ReconnectPolicy{
			InitialDelay: 1 * time.Second,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			JitterMode:   types.JitterEqual,
		},
	}

	for i := 0; i < 100; i++ {
		d := rl.computeDelay(1)
		assert.GreaterOrEqual(t, d, 500*time.Millisecond)
		assert.LessOrEqual(t, d, 1*time.Second)
	}
}

func TestReconnectLoopMaxAttempts(t *testing.T) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, nil)
	sm.transition(types.StateConnecting)
	sm.transition(types.StateReady)
	sm.transition(types.StateReconnecting)

	failCount := 0
	rl := newReconnectLoop(
		types.ReconnectPolicy{
			MaxAttempts:  2,
			InitialDelay: 1 * time.Millisecond,
			MaxDelay:     10 * time.Millisecond,
			Multiplier:   1.0,
			JitterMode:   types.JitterNone,
			BufferSize:   10,
		},
		sm,
		log,
		func(ctx context.Context) error {
			failCount++
			return fmt.Errorf("dial failed")
		},
		nil,
	)

	ctx := context.Background()
	rl.start(ctx)

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, types.StateClosed, sm.current(), "should transition to CLOSED after max attempts")
	assert.GreaterOrEqual(t, failCount, 2)
}

func TestReconnectLoopSuccess(t *testing.T) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, nil)
	sm.transition(types.StateConnecting)
	sm.transition(types.StateReady)
	sm.transition(types.StateReconnecting)

	var attemptCount atomic.Int32
	rl := newReconnectLoop(
		types.ReconnectPolicy{
			MaxAttempts:  10,
			InitialDelay: 1 * time.Millisecond,
			MaxDelay:     10 * time.Millisecond,
			Multiplier:   1.0,
			JitterMode:   types.JitterNone,
			BufferSize:   10,
		},
		sm,
		log,
		func(ctx context.Context) error {
			if attemptCount.Add(1) < 3 {
				return fmt.Errorf("not yet")
			}
			return nil
		},
		nil,
	)

	ctx := context.Background()
	rl.start(ctx)

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, types.StateReady, sm.current(), "should transition to READY on success")
}

func TestReconnectLoopCancel(t *testing.T) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, nil)
	sm.transition(types.StateConnecting)
	sm.transition(types.StateReady)
	sm.transition(types.StateReconnecting)

	rl := newReconnectLoop(
		types.ReconnectPolicy{
			MaxAttempts:  0,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     1 * time.Second,
			Multiplier:   2.0,
			JitterMode:   types.JitterNone,
			BufferSize:   10,
		},
		sm,
		log,
		func(ctx context.Context) error {
			return fmt.Errorf("fail")
		},
		nil,
	)

	ctx := context.Background()
	rl.start(ctx)
	time.Sleep(10 * time.Millisecond)

	rl.cancel()
	time.Sleep(50 * time.Millisecond)
	assert.False(t, rl.running.Load(), "should stop running after cancel")
}

func TestReconnectLoopPreventsDuplicateStart(t *testing.T) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, nil)
	sm.transition(types.StateConnecting)
	sm.transition(types.StateReady)
	sm.transition(types.StateReconnecting)

	var callCount atomic.Int32
	rl := newReconnectLoop(
		types.ReconnectPolicy{
			MaxAttempts:  0,
			InitialDelay: 50 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
			Multiplier:   1.0,
			JitterMode:   types.JitterNone,
			BufferSize:   10,
		},
		sm,
		log,
		func(ctx context.Context) error {
			callCount.Add(1)
			return fmt.Errorf("fail")
		},
		nil,
	)

	ctx := context.Background()
	rl.start(ctx)
	rl.start(ctx) // should be no-op

	time.Sleep(150 * time.Millisecond)
	rl.cancel()
	time.Sleep(50 * time.Millisecond)

	count := callCount.Load()
	assert.GreaterOrEqual(t, count, int32(1))
}

func TestReconnectLoopFlushBuffer(t *testing.T) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, nil)

	rl := newReconnectLoop(
		types.ReconnectPolicy{
			BufferSize:   10,
			InitialDelay: 1 * time.Second,
			MaxDelay:     1 * time.Second,
			Multiplier:   1.0,
		},
		sm,
		log,
		func(ctx context.Context) error { return nil },
		nil,
	)

	var order []int
	for i := 0; i < 3; i++ {
		n := i
		_ = rl.buffer.enqueue(bufferedMessage{
			sendFn: func(ctx context.Context) error {
				order = append(order, n)
				return nil
			},
		})
	}

	rl.flushBuffer()
	assert.Equal(t, []int{0, 1, 2}, order)
	assert.Equal(t, 0, rl.buffer.count())
}

func TestReconnectLoopDiscardBufferCallsDrain(t *testing.T) {
	var drainCount atomic.Int32
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, func(count int) {
		drainCount.Store(int32(count))
	})

	rl := newReconnectLoop(
		types.ReconnectPolicy{
			BufferSize:   10,
			InitialDelay: 1 * time.Second,
			MaxDelay:     1 * time.Second,
			Multiplier:   1.0,
		},
		sm,
		log,
		func(ctx context.Context) error { return nil },
		nil,
	)

	for i := 0; i < 5; i++ {
		_ = rl.buffer.enqueue(bufferedMessage{
			sendFn: func(ctx context.Context) error { return nil },
		})
	}

	rl.discardBuffer()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(5), drainCount.Load())
	assert.Equal(t, 0, rl.buffer.count())
}

func TestReconnectLoopBackoffResetAfterSuccess(t *testing.T) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, nil)
	sm.transition(types.StateConnecting)
	sm.transition(types.StateReady)
	sm.transition(types.StateReconnecting)

	rl := newReconnectLoop(
		types.ReconnectPolicy{
			MaxAttempts:  10,
			InitialDelay: 1 * time.Millisecond,
			MaxDelay:     10 * time.Millisecond,
			Multiplier:   1.0,
			JitterMode:   types.JitterNone,
			BufferSize:   10,
		},
		sm,
		log,
		func(ctx context.Context) error { return nil },
		nil,
	)

	ctx := context.Background()
	rl.start(ctx)
	time.Sleep(50 * time.Millisecond)

	rl.mu.Lock()
	attempt := rl.attempt
	rl.mu.Unlock()
	assert.Equal(t, 0, attempt, "attempt counter should reset after success")
}

func TestSubscriptionTrackerRegisterAndCount(t *testing.T) {
	st := &subscriptionTracker{}

	st.register(&subscriptionRecord{
		id:      "sub-1",
		pattern: "events",
		channel: "test-channel",
	})
	st.register(&subscriptionRecord{
		id:      "sub-2",
		pattern: "commands",
		channel: "cmd-channel",
	})

	assert.Equal(t, 2, st.count())
}

func TestSubscriptionTrackerRemove(t *testing.T) {
	st := &subscriptionTracker{}
	st.register(&subscriptionRecord{id: "sub-1", pattern: "events", channel: "ch1"})
	st.register(&subscriptionRecord{id: "sub-2", pattern: "events", channel: "ch2"})

	st.remove("sub-1")

	assert.Equal(t, 1, st.count())

	st.mu.RLock()
	defer st.mu.RUnlock()
	assert.NotNil(t, st.subscriptions["sub-2"])
	assert.Equal(t, "ch2", st.subscriptions["sub-2"].channel)
}

func TestSubscriptionTrackerUpdateLastSeq(t *testing.T) {
	st := &subscriptionTracker{}
	st.register(&subscriptionRecord{
		id:      "sub-store",
		pattern: "events_store",
		channel: "store-ch",
	})

	st.updateLastSeq("sub-store", 42)
	assert.Equal(t, int64(42), st.getLastSeq("sub-store"))
}

func TestSubscriptionTrackerGetLastSeqMissing(t *testing.T) {
	st := &subscriptionTracker{}
	assert.Equal(t, int64(0), st.getLastSeq("nonexistent"))
}

func TestIsConnectionError(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		expect bool
	}{
		{"nil error", nil, false},
		{"non-gRPC error", fmt.Errorf("random"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, isConnectionError(tt.err))
		})
	}
}
