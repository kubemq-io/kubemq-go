package disconnect

import (
	"context"
	"log/slog"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/burnin/metrics"
)

// ClientRecreator is the interface that the disconnect manager uses
// to force-close and re-establish a client connection.
type ClientRecreator interface {
	Close() error
	Recreate(ctx context.Context) error
}

// Manager periodically forces a client disconnect and reconnect
// to test resilience and recovery behaviour.
type Manager struct {
	interval  time.Duration
	duration  time.Duration
	recreator ClientRecreator
	logger    *slog.Logger
}

// New creates a new disconnect Manager.
// If interval is 0, Run will return immediately (feature disabled).
func New(interval, duration time.Duration, recreator ClientRecreator, logger *slog.Logger) *Manager {
	return &Manager{
		interval:  interval,
		duration:  duration,
		recreator: recreator,
		logger:    logger,
	}
}

// Run periodically forces disconnects until ctx is cancelled.
// It blocks until the context is done. If interval is 0 the function returns immediately.
func (m *Manager) Run(ctx context.Context) {
	if m.interval == 0 {
		return
	}

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.forceDisconnect(ctx)
		}
	}
}

func (m *Manager) forceDisconnect(ctx context.Context) {
	m.logger.Info("forcing disconnect")

	if err := m.recreator.Close(); err != nil {
		m.logger.Error("error closing client during forced disconnect", "error", err)
	}

	// Wait for the configured outage duration before reconnecting.
	select {
	case <-ctx.Done():
		return
	case <-time.After(m.duration):
	}

	if err := m.recreator.Recreate(ctx); err != nil {
		m.logger.Error("error recreating client after forced disconnect", "error", err)
	}

	metrics.ForcedDisconnectsTotal.Inc()
}
