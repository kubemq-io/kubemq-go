package transport

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testLogger struct {
	mu   sync.Mutex
	logs []string
}

func (l *testLogger) Info(msg string, keysAndValues ...any)  { l.record(msg) }
func (l *testLogger) Warn(msg string, keysAndValues ...any)  { l.record(msg) }
func (l *testLogger) Error(msg string, keysAndValues ...any) { l.record(msg) }

func (l *testLogger) record(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, msg)
}

func (l *testLogger) contains(msg string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.logs {
		if m == msg {
			return true
		}
	}
	return false
}

func TestStateMachineInitialState(t *testing.T) {
	sm := newStateMachine(StateCallbacks{}, &testLogger{}, nil)
	assert.Equal(t, types.StateIdle, sm.current())
}

func TestStateMachineValidTransitions(t *testing.T) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, nil)

	sm.transition(types.StateConnecting)
	assert.Equal(t, types.StateConnecting, sm.current())

	sm.transition(types.StateReady)
	assert.Equal(t, types.StateReady, sm.current())

	sm.transition(types.StateReconnecting)
	assert.Equal(t, types.StateReconnecting, sm.current())

	sm.transition(types.StateReady)
	assert.Equal(t, types.StateReady, sm.current())

	sm.transition(types.StateClosed)
	assert.Equal(t, types.StateClosed, sm.current())
}

func TestStateMachineInvalidTransition(t *testing.T) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, nil)

	// IDLE → READY is invalid (must go through CONNECTING)
	sm.transition(types.StateReady)
	assert.Equal(t, types.StateIdle, sm.current())

	time.Sleep(10 * time.Millisecond)
	assert.True(t, log.contains("invalid state transition"))
}

func TestStateMachineClosedIsTerminal(t *testing.T) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, nil)

	sm.transition(types.StateConnecting)
	sm.transition(types.StateClosed)
	assert.Equal(t, types.StateClosed, sm.current())

	sm.transition(types.StateConnecting)
	assert.Equal(t, types.StateClosed, sm.current())
}

func TestStateMachineNoOpSameState(t *testing.T) {
	sm := newStateMachine(StateCallbacks{}, &testLogger{}, nil)
	sm.transition(types.StateConnecting)
	sm.transition(types.StateConnecting)
	assert.Equal(t, types.StateConnecting, sm.current())
}

func TestStateMachineCallbacksFired(t *testing.T) {
	var connected, disconnected, reconnecting, reconnected, closed atomic.Bool

	sm := newStateMachine(StateCallbacks{
		OnConnected:    func() { connected.Store(true) },
		OnDisconnected: func() { disconnected.Store(true) },
		OnReconnecting: func() { reconnecting.Store(true) },
		OnReconnected:  func() { reconnected.Store(true) },
		OnClosed:       func() { closed.Store(true) },
	}, &testLogger{}, nil)

	sm.transition(types.StateConnecting)
	sm.transition(types.StateReady)
	time.Sleep(50 * time.Millisecond)
	assert.True(t, connected.Load(), "OnConnected should fire")

	sm.transition(types.StateReconnecting)
	time.Sleep(50 * time.Millisecond)
	assert.True(t, disconnected.Load(), "OnDisconnected should fire")
	assert.True(t, reconnecting.Load(), "OnReconnecting should fire")

	sm.transition(types.StateReady)
	time.Sleep(50 * time.Millisecond)
	assert.True(t, reconnected.Load(), "OnReconnected should fire")

	sm.transition(types.StateClosed)
	time.Sleep(50 * time.Millisecond)
	assert.True(t, closed.Load(), "OnClosed should fire")
}

func TestStateMachineCallbacksAsync(t *testing.T) {
	blockCh := make(chan struct{})
	sm := newStateMachine(StateCallbacks{
		OnConnected: func() {
			<-blockCh
		},
	}, &testLogger{}, nil)

	sm.transition(types.StateConnecting)

	start := time.Now()
	sm.transition(types.StateReady)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 50*time.Millisecond, "transition should not block on callback")

	close(blockCh)
}

func TestStateMachineCallbackPanicRecovery(t *testing.T) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{
		OnConnected: func() { panic("test panic") },
	}, log, nil)

	sm.transition(types.StateConnecting)
	sm.transition(types.StateReady)

	time.Sleep(50 * time.Millisecond)
	assert.True(t, log.contains("state callback panicked"))
}

func TestStateMachineWaitForState(t *testing.T) {
	sm := newStateMachine(StateCallbacks{}, &testLogger{}, nil)

	ch := sm.waitForState(types.StateReady)

	go func() {
		time.Sleep(10 * time.Millisecond)
		sm.transition(types.StateConnecting)
		sm.transition(types.StateReady)
	}()

	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		t.Fatal("waitForState did not resolve")
	}

	assert.Equal(t, types.StateReady, sm.current())
}

func TestStateMachineWaitForStateAlreadyInState(t *testing.T) {
	sm := newStateMachine(StateCallbacks{}, &testLogger{}, nil)
	sm.transition(types.StateConnecting)
	sm.transition(types.StateReady)

	ch := sm.waitForState(types.StateReady)
	select {
	case <-ch:
	default:
		t.Fatal("channel should be closed immediately when already in target state")
	}
}

func TestStateMachineConcurrentTransitions(t *testing.T) {
	sm := newStateMachine(StateCallbacks{}, &testLogger{}, nil)
	sm.transition(types.StateConnecting)
	sm.transition(types.StateReady)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sm.current()
		}()
	}
	wg.Wait()
}

func TestStateMachineFireBufferDrain(t *testing.T) {
	var drainCount atomic.Int32
	sm := newStateMachine(StateCallbacks{}, &testLogger{}, func(count int) {
		drainCount.Store(int32(count))
	})

	sm.fireBufferDrain(42)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(42), drainCount.Load())
}

func TestStateMachineStateIncludedInLogs(t *testing.T) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, nil)

	sm.transition(types.StateConnecting)

	time.Sleep(10 * time.Millisecond)
	require.True(t, log.contains("connection state changed"))
}

func TestNoopLoggerMethods(t *testing.T) {
	var l noopLogger
	l.Info("test", "k", "v")
	l.Warn("test", "k", "v")
	l.Error("test", "k", "v")
}

func TestFireBufferDrain_PanicRecovery(t *testing.T) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, func(count int) {
		panic("drain callback panic")
	})

	assert.NotPanics(t, func() {
		sm.fireBufferDrain(10)
		time.Sleep(100 * time.Millisecond)
	})
	assert.True(t, log.contains("OnBufferDrain callback panicked"))
}

func TestIsValidTransition(t *testing.T) {
	tests := []struct {
		from  types.ConnectionState
		to    types.ConnectionState
		valid bool
	}{
		{types.StateIdle, types.StateConnecting, true},
		{types.StateIdle, types.StateClosed, true},
		{types.StateIdle, types.StateReady, false},
		{types.StateConnecting, types.StateReady, true},
		{types.StateConnecting, types.StateClosed, true},
		{types.StateConnecting, types.StateReconnecting, false},
		{types.StateReady, types.StateReconnecting, true},
		{types.StateReady, types.StateClosed, true},
		{types.StateReady, types.StateConnecting, false},
		{types.StateReconnecting, types.StateReady, true},
		{types.StateReconnecting, types.StateClosed, true},
		{types.StateReconnecting, types.StateConnecting, false},
		{types.StateClosed, types.StateIdle, false},
		{types.StateClosed, types.StateConnecting, false},
	}

	for _, tt := range tests {
		t.Run(tt.from.String()+"→"+tt.to.String(), func(t *testing.T) {
			assert.Equal(t, tt.valid, isValidTransition(tt.from, tt.to))
		})
	}
}
