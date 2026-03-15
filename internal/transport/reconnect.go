package transport

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// reconnectLoop manages automatic reconnection with exponential backoff
// and message buffering. It is owned by grpcTransport.
type reconnectLoop struct {
	policy        types.ReconnectPolicy
	state         *stateMachine
	logger        logger
	dialFn        func(ctx context.Context) error
	onReconnected func(ctx context.Context)
	cancelFn      context.CancelFunc

	mu      sync.Mutex
	buffer  *ringBuffer
	attempt int
	running atomic.Bool
}

func newReconnectLoop(
	policy types.ReconnectPolicy,
	sm *stateMachine,
	log logger,
	dialFn func(ctx context.Context) error,
	onReconnected func(ctx context.Context),
) *reconnectLoop {
	return &reconnectLoop{
		policy:        policy,
		state:         sm,
		logger:        log,
		dialFn:        dialFn,
		onReconnected: onReconnected,
		buffer:        newRingBuffer(policy.BufferSize),
	}
}

// start begins the reconnection loop. Called when connection loss is detected.
// Thread-safe: protected by running atomic bool to prevent duplicate loops.
func (r *reconnectLoop) start(ctx context.Context) {
	if !r.running.CompareAndSwap(false, true) {
		return
	}

	rctx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.cancelFn = cancel
	r.attempt = 0
	r.mu.Unlock()

	go r.loop(rctx)
}

func (r *reconnectLoop) loop(ctx context.Context) {
	defer r.running.Store(false)

	for {
		r.mu.Lock()
		r.attempt++
		attempt := r.attempt
		maxAttempts := r.policy.MaxAttempts
		r.mu.Unlock()

		if maxAttempts > 0 && attempt > maxAttempts {
			r.logger.Error("max reconnect attempts exhausted",
				"attempt", attempt,
				"max", maxAttempts,
			)
			r.state.transition(types.StateClosed)
			r.discardBuffer()
			return
		}

		delay := r.computeDelay(attempt)
		r.logger.Info("attempting reconnection",
			"attempt", attempt,
			"delay", delay,
			"state", types.StateReconnecting.String(),
		)

		// GO-57: use time.NewTimer, not time.After in loops
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		err := r.dialFn(ctx)
		if err != nil {
			r.logger.Warn("reconnection attempt failed",
				"attempt", attempt,
				"error", err,
			)
			continue
		}

		r.logger.Info("reconnection successful",
			"attempt", attempt,
			"state", types.StateReady.String(),
		)
		r.state.transition(types.StateReady)

		if r.onReconnected != nil {
			go r.onReconnected(ctx)
		}

		r.mu.Lock()
		r.attempt = 0
		r.mu.Unlock()

		r.flushBuffer()
		return
	}
}

// cancel stops the reconnection loop.
func (r *reconnectLoop) cancel() {
	r.mu.Lock()
	if r.cancelFn != nil {
		r.cancelFn()
		r.cancelFn = nil
	}
	r.mu.Unlock()
}

// computeDelay calculates the backoff delay with jitter for a given attempt.
// Formula: min(initialDelay * multiplier^(attempt-1) + jitter, maxDelay)
func (r *reconnectLoop) computeDelay(attempt int) time.Duration {
	base := float64(r.policy.InitialDelay) * math.Pow(r.policy.Multiplier, float64(attempt-1))
	if base > float64(r.policy.MaxDelay) {
		base = float64(r.policy.MaxDelay)
	}

	switch r.policy.JitterMode {
	case types.JitterFull:
		if int64(base) <= 0 {
			return 0
		}
		return time.Duration(rand.Int64N(int64(base) + 1)) //nolint:gosec // G404: jitter for reconnect backoff does not need cryptographic randomness
	case types.JitterEqual:
		half := int64(base) / 2
		if half <= 0 {
			return time.Duration(int64(base))
		}
		return time.Duration(half + rand.Int64N(half+1)) //nolint:gosec // G404: jitter for reconnect backoff does not need cryptographic randomness
	case types.JitterNone:
		return time.Duration(int64(base))
	default:
		return time.Duration(int64(base))
	}
}

// flushBuffer sends all buffered messages in FIFO order after reconnection.
func (r *reconnectLoop) flushBuffer() {
	msgs := r.buffer.drainAll()
	if len(msgs) == 0 {
		return
	}

	r.logger.Info("flushing reconnection buffer", "count", len(msgs))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, msg := range msgs {
		if err := msg.sendFn(ctx); err != nil {
			r.logger.Warn("failed to send buffered message after reconnection",
				"error", err,
			)
		}
	}
}

// discardBuffer drops all buffered messages and fires the OnBufferDrain callback.
func (r *reconnectLoop) discardBuffer() {
	msgs := r.buffer.drainAll()
	if len(msgs) == 0 {
		return
	}
	r.logger.Warn("discarding buffered messages", "count", len(msgs))
	r.state.fireBufferDrain(len(msgs))
}

// ringBuffer is a thread-safe message-count-limited FIFO buffer for messages
// published during the RECONNECTING state.
type ringBuffer struct {
	mu       sync.Mutex
	items    []bufferedMessage
	maxCount int
}

type bufferedMessage struct {
	sendFn func(ctx context.Context) error
}

func newRingBuffer(maxCount int) *ringBuffer {
	if maxCount <= 0 {
		maxCount = 1000
	}
	return &ringBuffer{
		items:    make([]bufferedMessage, 0, 64),
		maxCount: maxCount,
	}
}

func (rb *ringBuffer) enqueue(msg bufferedMessage) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if len(rb.items) >= rb.maxCount {
		return fmt.Errorf("kubemq: reconnection buffer full (capacity=%d, queued=%d)", rb.maxCount, len(rb.items))
	}

	rb.items = append(rb.items, msg)
	return nil
}

func (rb *ringBuffer) drainAll() []bufferedMessage {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	msgs := rb.items
	rb.items = make([]bufferedMessage, 0, 64)
	return msgs
}

func (rb *ringBuffer) count() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return len(rb.items)
}

// subscriptionTracker tracks active subscriptions for automatic recovery after reconnection.
type subscriptionTracker struct {
	mu            sync.RWMutex
	subscriptions map[string]*subscriptionRecord
}

type subscriptionRecord struct {
	id        string
	pattern   string
	channel   string
	group     string
	lastSeqNo int64
}

func (st *subscriptionTracker) register(rec *subscriptionRecord) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.subscriptions == nil {
		st.subscriptions = make(map[string]*subscriptionRecord)
	}
	st.subscriptions[rec.id] = rec
}

func (st *subscriptionTracker) updateLastSeq(id string, seq int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if r, ok := st.subscriptions[id]; ok {
		r.lastSeqNo = seq
	}
}

func (st *subscriptionTracker) getLastSeq(id string) int64 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	if r, ok := st.subscriptions[id]; ok {
		return r.lastSeqNo
	}
	return 0
}

func (st *subscriptionTracker) remove(id string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.subscriptions, id)
}

func (st *subscriptionTracker) count() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.subscriptions)
}

// isConnectionError returns true if the error indicates a transport-level connection loss
// that requires full reconnection.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return true
	}
	switch st.Code() {
	case codes.Unavailable:
		return true
	case codes.Internal:
		msg := st.Message()
		return strings.Contains(msg, "transport") ||
			strings.Contains(msg, "connection") ||
			strings.Contains(msg, "RST_STREAM")
	default:
		return false
	}
}
