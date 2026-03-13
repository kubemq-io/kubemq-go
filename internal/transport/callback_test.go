package transport

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallbackDispatcher_Dispatch(t *testing.T) {
	d := newCallbackDispatcher(2, &testLogger{})
	var count atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		d.dispatch(context.Background(), func() {
			count.Add(1)
			wg.Done()
		})
	}
	wg.Wait()
	assert.Equal(t, int32(5), count.Load())
}

func TestCallbackDispatcher_Dispatch_Closed(t *testing.T) {
	d := newCallbackDispatcher(1, &testLogger{})
	d.drain(time.Second)
	var called atomic.Bool
	d.dispatch(context.Background(), func() {
		called.Store(true)
	})
	time.Sleep(50 * time.Millisecond)
	assert.False(t, called.Load())
}

func TestCallbackDispatcher_Dispatch_ContextCanceled(t *testing.T) {
	d := newCallbackDispatcher(1, &testLogger{})
	// Fill the semaphore
	d.sem <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var called atomic.Bool
	d.dispatch(ctx, func() {
		called.Store(true)
	})
	time.Sleep(50 * time.Millisecond)
	d.drain(time.Second)
	<-d.sem
	assert.False(t, called.Load())
}

func TestCallbackDispatcher_SafeCall_PanicRecovery(t *testing.T) {
	d := newCallbackDispatcher(1, &testLogger{})
	var receivedErr error
	d.onError = func(err error) {
		receivedErr = err
	}
	d.safeCall(func() {
		panic("test panic")
	})
	require.NotNil(t, receivedErr)
	assert.Contains(t, receivedErr.Error(), "test panic")
}

func TestCallbackDispatcher_SafeCall_NoPanic(t *testing.T) {
	d := newCallbackDispatcher(1, &testLogger{})
	var called bool
	d.safeCall(func() {
		called = true
	})
	assert.True(t, called)
}

func TestCallbackDispatcher_Drain_Complete(t *testing.T) {
	d := newCallbackDispatcher(1, &testLogger{})
	ok := d.drain(time.Second)
	assert.True(t, ok)
}

func TestCallbackDispatcher_Drain_Timeout(t *testing.T) {
	d := newCallbackDispatcher(1, &testLogger{})
	d.wg.Add(1) // simulate in-flight callback
	ok := d.drain(50 * time.Millisecond)
	assert.False(t, ok)
	d.wg.Done() // cleanup
}

func TestCallbackDispatcher_ActiveConcurrent(t *testing.T) {
	d := newCallbackDispatcher(5, &testLogger{})
	assert.Equal(t, 0, d.activeConcurrent())
	d.sem <- struct{}{}
	assert.Equal(t, 1, d.activeConcurrent())
	<-d.sem
}

func TestCallbackDispatcher_ConcurrencyLimit(t *testing.T) {
	d := newCallbackDispatcher(2, &testLogger{})
	var maxConcurrent atomic.Int32
	var current atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		d.dispatch(context.Background(), func() {
			defer wg.Done()
			c := current.Add(1)
			for {
				old := maxConcurrent.Load()
				if c <= old || maxConcurrent.CompareAndSwap(old, c) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			current.Add(-1)
		})
	}
	wg.Wait()
	assert.LessOrEqual(t, maxConcurrent.Load(), int32(2))
}
