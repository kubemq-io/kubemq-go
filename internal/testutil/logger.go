package testutil

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
)

// TestLogger captures log output for assertion in tests.
// Implements types.Logger (defined in 05-observability-spec.md, see TYPE-REGISTRY.md).
type TestLogger struct {
	t       *testing.T
	mu      sync.Mutex
	entries []LogEntry
}

// LogEntry represents a single captured log entry.
type LogEntry struct {
	Level   string
	Message string
	KVs     []any
}

var _ types.Logger = (*TestLogger)(nil)

// NewTestLogger creates a new TestLogger bound to the given test.
func NewTestLogger(t *testing.T) *TestLogger {
	return &TestLogger{t: t}
}

func (l *TestLogger) Debug(msg string, keysAndValues ...any) {
	l.record("DEBUG", msg, keysAndValues)
}

func (l *TestLogger) Info(msg string, keysAndValues ...any) {
	l.record("INFO", msg, keysAndValues)
}

func (l *TestLogger) Warn(msg string, keysAndValues ...any) {
	l.record("WARN", msg, keysAndValues)
}

func (l *TestLogger) Error(msg string, keysAndValues ...any) {
	l.record("ERROR", msg, keysAndValues)
}

func (l *TestLogger) record(level, msg string, kvs []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, LogEntry{Level: level, Message: msg, KVs: kvs})
	l.t.Logf("[%s] %s %v", level, msg, kvs)
}

// Entries returns a copy of all recorded log entries.
func (l *TestLogger) Entries() []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	entries := make([]LogEntry, len(l.entries))
	copy(entries, l.entries)
	return entries
}

// ContainsMessage returns true if any entry contains the substring.
func (l *TestLogger) ContainsMessage(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		if strings.Contains(e.Message, substr) {
			return true
		}
	}
	return false
}

// ContainsLevel returns true if any entry exists at the given level.
func (l *TestLogger) ContainsLevel(level string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		if e.Level == level {
			return true
		}
	}
	return false
}

// AssertContains fails the test if no log entry contains the substring.
func (l *TestLogger) AssertContains(substr string) {
	l.t.Helper()
	if !l.ContainsMessage(substr) {
		l.t.Errorf("expected log entry containing %q, got entries: %v", substr, l.formatEntries())
	}
}

// AssertNotContains fails the test if any log entry contains the substring.
func (l *TestLogger) AssertNotContains(substr string) {
	l.t.Helper()
	if l.ContainsMessage(substr) {
		l.t.Errorf("expected no log entry containing %q, but found one in: %v", substr, l.formatEntries())
	}
}

func (l *TestLogger) formatEntries() string {
	var sb strings.Builder
	for _, e := range l.entries {
		fmt.Fprintf(&sb, "\n  [%s] %s", e.Level, e.Message)
	}
	return sb.String()
}
