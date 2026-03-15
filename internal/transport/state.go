package transport

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
)

// logger is a minimal logging interface satisfied by kubemq.Logger.
// Defined here to avoid importing the root package (import cycle prevention).
type logger interface {
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

// noopLogger satisfies the logger interface when no logger is configured.
type noopLogger struct{}

func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

// StateCallbacks mirrors the root-package StateCallbacks.
// Passed as primitives from the root package at construction time.
type StateCallbacks struct {
	OnConnected    func()
	OnDisconnected func()
	OnReconnecting func()
	OnReconnected  func()
	OnClosed       func()
}

// stateMachine manages connection state transitions with thread-safe
// state reads, validated transitions, and asynchronous callback dispatch.
type stateMachine struct {
	state     atomic.Int32
	callbacks StateCallbacks
	logger    logger

	waitersMu sync.Mutex
	waiters   map[types.ConnectionState][]chan struct{}

	onBufferDrain func(discardedCount int)
}

func newStateMachine(
	callbacks StateCallbacks,
	log logger,
	onBufferDrain func(discardedCount int),
) *stateMachine {
	sm := &stateMachine{
		callbacks:     callbacks,
		logger:        log,
		waiters:       make(map[types.ConnectionState][]chan struct{}),
		onBufferDrain: onBufferDrain,
	}
	sm.state.Store(int32(types.StateIdle))
	return sm
}

// current returns the current state. Thread-safe (atomic read).
func (sm *stateMachine) current() types.ConnectionState {
	return types.ConnectionState(sm.state.Load())
}

// transition atomically moves to a new state using a compare-and-swap loop.
// Validates the transition before swapping to prevent invalid intermediate states.
func (sm *stateMachine) transition(to types.ConnectionState) {
	for {
		from := types.ConnectionState(sm.state.Load())
		if from == to {
			return
		}

		if !isValidTransition(from, to) {
			sm.logger.Error("invalid state transition",
				"from", from.String(),
				"to", to.String(),
			)
			return
		}

		if sm.state.CompareAndSwap(int32(from), int32(to)) { //nolint:gosec // G115: ConnectionState enum values fit int32
			sm.logger.Info("connection state changed",
				"from", from.String(),
				"to", to.String(),
			)
			sm.notifyWaiters(to)
			sm.fireCallbacks(from, to)
			return
		}
	}
}

func isValidTransition(from, to types.ConnectionState) bool {
	switch from {
	case types.StateIdle:
		return to == types.StateConnecting || to == types.StateClosed
	case types.StateConnecting:
		return to == types.StateReady || to == types.StateClosed
	case types.StateReady:
		return to == types.StateReconnecting || to == types.StateClosed
	case types.StateReconnecting:
		return to == types.StateReady || to == types.StateClosed
	case types.StateClosed:
		return false
	default:
		return false
	}
}

func (sm *stateMachine) fireCallbacks(from, to types.ConnectionState) {
	var fn func()

	switch {
	case from == types.StateConnecting && to == types.StateReady:
		fn = sm.callbacks.OnConnected
	case from == types.StateReady && to == types.StateReconnecting:
		fn = sm.callbacks.OnDisconnected
		if sm.callbacks.OnReconnecting != nil {
			go sm.safeFire("OnReconnecting", sm.callbacks.OnReconnecting)
		}
	case from == types.StateReconnecting && to == types.StateReady:
		fn = sm.callbacks.OnReconnected
	case to == types.StateClosed:
		fn = sm.callbacks.OnClosed
	}

	if fn != nil {
		go sm.safeFire(fmt.Sprintf("%s→%s", from.String(), to.String()), fn)
	}
}

func (sm *stateMachine) safeFire(label string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			sm.logger.Error("state callback panicked",
				"callback", label,
				"panic", fmt.Sprintf("%v", r),
			)
		}
	}()
	fn()
}

// waitForState returns a channel that is closed when the state machine
// enters the target state. If already in that state, the returned
// channel is already closed.
func (sm *stateMachine) waitForState(target types.ConnectionState) <-chan struct{} {
	ch := make(chan struct{})
	sm.waitersMu.Lock()
	if sm.current() == target {
		sm.waitersMu.Unlock()
		close(ch)
		return ch
	}
	sm.waiters[target] = append(sm.waiters[target], ch)
	sm.waitersMu.Unlock()
	return ch
}

func (sm *stateMachine) notifyWaiters(state types.ConnectionState) {
	sm.waitersMu.Lock()
	waiters := sm.waiters[state]
	delete(sm.waiters, state)
	sm.waitersMu.Unlock()

	for _, ch := range waiters {
		close(ch)
	}
}

// fireBufferDrain invokes the OnBufferDrain callback if registered.
func (sm *stateMachine) fireBufferDrain(discardedCount int) {
	if sm.onBufferDrain != nil {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					sm.logger.Error("OnBufferDrain callback panicked",
						"panic", fmt.Sprintf("%v", r),
					)
				}
			}()
			sm.onBufferDrain(discardedCount)
		}()
	}
}
