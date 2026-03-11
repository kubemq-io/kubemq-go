package transport

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// callbackDispatcher manages concurrent callback execution with a semaphore
// and tracks in-flight callbacks via a WaitGroup for shutdown drain.
//
// One callbackDispatcher is created per Client. All subscriptions on that
// client share the same dispatcher (and thus the same concurrency limit).
type callbackDispatcher struct {
	sem     chan struct{}  // semaphore limiting concurrent callbacks
	wg      sync.WaitGroup // tracks in-flight callbacks for shutdown drain
	closed  atomic.Bool    // true after drain() initiated
	logger  logger         // transport-local logger interface (avoids import cycle)
	onError func(error)    // error handler for panics in callbacks
}

// newCallbackDispatcher creates a dispatcher with the given concurrency limit.
// Uses primitive int to avoid importing root package (XS-4 import cycle resolution).
func newCallbackDispatcher(maxConcurrent int, log logger) *callbackDispatcher {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	return &callbackDispatcher{
		sem:    make(chan struct{}, maxConcurrent),
		logger: log,
	}
}

// dispatch executes fn with concurrency limiting and panic recovery.
// If the dispatcher is closed, the callback is silently dropped.
// The caller's context is checked before acquiring the semaphore.
//
// dispatch does NOT block the caller — it launches a goroutine that
// acquires the semaphore, ensuring the stream reader is never blocked
// by slow callbacks.
//
// Uses the double-check pattern (mirrors enterOperation in grpc.go)
// to prevent a race between closed.Store(true) and wg.Add(1).
func (d *callbackDispatcher) dispatch(ctx context.Context, fn func()) {
	if d.closed.Load() {
		return
	}

	d.wg.Add(1)
	if d.closed.Load() {
		d.wg.Done()
		return
	}

	go func() {
		defer d.wg.Done()

		select {
		case d.sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		defer func() { <-d.sem }()

		if d.closed.Load() {
			return
		}

		d.safeCall(fn)
	}()
}

// safeCall executes fn with panic recovery. Panics are logged and delivered
// to the error handler.
func (d *callbackDispatcher) safeCall(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("kubemq handler [subscription callback]: panic: %v", r)
			d.logger.Error("subscription callback panicked", "error", err)
			if d.onError != nil {
				d.onError(err)
			}
		}
	}()
	fn()
}

// drain waits for all in-flight callbacks to complete, with timeout.
// Called by Close() after the operation drain completes.
// Returns true if all callbacks completed, false if timeout was hit.
func (d *callbackDispatcher) drain(timeout time.Duration) bool {
	d.closed.Store(true)

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		d.logger.Warn("callback drain timeout exceeded, proceeding with shutdown",
			"timeout", timeout,
		)
		return false
	}
}

// activeConcurrent returns the number of callbacks currently holding a
// semaphore slot (i.e., actively executing). Used for observability and testing.
func (d *callbackDispatcher) activeConcurrent() int {
	return len(d.sem)
}
