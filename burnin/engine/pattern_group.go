package engine

import (
	"log/slog"

	"github.com/kubemq-io/kubemq-go/v2/burnin/config"
	"github.com/kubemq-io/kubemq-go/v2/burnin/metrics"
	"github.com/kubemq-io/kubemq-go/v2/burnin/worker"
)

// PatternGroup holds N ChannelWorkers for a single pattern and aggregates their stats.
type PatternGroup struct {
	pattern         string
	patternConfig   *config.PatternConfig
	channelWorkers  []worker.Worker
	patternLatAccum *metrics.LatencyAccumulator // shared, thread-safe
	logger          *slog.Logger
}

// NewPatternGroup creates a PatternGroup with N workers for the given pattern.
func NewPatternGroup(pattern string, pc *config.PatternConfig, cfg *config.Config,
	runID string, cp *worker.ClientProvider, logger *slog.Logger) *PatternGroup {

	pg := &PatternGroup{
		pattern:         pattern,
		patternConfig:   pc,
		patternLatAccum: metrics.NewLatencyAccumulator(),
		logger:          logger.With("pattern_group", pattern),
	}

	for i := 1; i <= pc.Channels; i++ {
		channelName := worker.ChannelNameForPattern(runID, pattern, i)
		var w worker.Worker

		switch pattern {
		case worker.PatternEvents:
			w = worker.NewEventsWorker(cfg, cp, logger, channelName, i, pc, runID, pg.patternLatAccum)
		case worker.PatternEventsStore:
			w = worker.NewEventsStoreWorker(cfg, cp, logger, channelName, i, pc, runID, pg.patternLatAccum)
		case worker.PatternQueueStream:
			w = worker.NewQueueStreamWorker(cfg, cp, logger, channelName, i, pc, pg.patternLatAccum)
		case worker.PatternQueueSimple:
			w = worker.NewQueueSimpleWorker(cfg, cp, logger, channelName, i, pc, pg.patternLatAccum)
		case worker.PatternCommands:
			w = worker.NewCommandsWorker(cfg, cp, logger, channelName, i, pc, pg.patternLatAccum)
		case worker.PatternQueries:
			w = worker.NewQueriesWorker(cfg, cp, logger, channelName, i, pc, pg.patternLatAccum)
		}

		if w != nil {
			pg.channelWorkers = append(pg.channelWorkers, w)
		}
	}

	return pg
}

// Pattern returns the pattern name.
func (pg *PatternGroup) Pattern() string { return pg.pattern }

// PatternConfig returns the pattern configuration.
func (pg *PatternGroup) PatternConfig() *config.PatternConfig { return pg.patternConfig }

// ChannelWorkers returns all channel workers.
func (pg *PatternGroup) ChannelWorkers() []worker.Worker { return pg.channelWorkers }

// PatternLatencyAccumulator returns the shared pattern-level latency accumulator.
func (pg *PatternGroup) PatternLatencyAccumulator() *metrics.LatencyAccumulator {
	return pg.patternLatAccum
}

// ChannelCount returns the number of channels.
func (pg *PatternGroup) ChannelCount() int { return len(pg.channelWorkers) }

// --- Aggregation methods ---

func (pg *PatternGroup) TotalSent() uint64 {
	var total uint64
	for _, w := range pg.channelWorkers {
		total += w.SentCount()
	}
	return total
}

func (pg *PatternGroup) TotalReceived() uint64 {
	var total uint64
	for _, w := range pg.channelWorkers {
		total += w.ReceivedCount()
	}
	return total
}

func (pg *PatternGroup) TotalLost() uint64 {
	var total uint64
	for _, w := range pg.channelWorkers {
		total += w.Tracker().TotalLost()
	}
	return total
}

func (pg *PatternGroup) TotalDuplicated() uint64 {
	var total uint64
	for _, w := range pg.channelWorkers {
		total += w.Tracker().TotalDuplicates()
	}
	return total
}

func (pg *PatternGroup) TotalCorrupted() uint64 {
	var total uint64
	for _, w := range pg.channelWorkers {
		total += w.CorruptedCount()
	}
	return total
}

func (pg *PatternGroup) TotalErrors() uint64 {
	var total uint64
	for _, w := range pg.channelWorkers {
		total += w.ErrorCount()
	}
	return total
}

func (pg *PatternGroup) TotalBytesSent() uint64 {
	var total uint64
	for _, w := range pg.channelWorkers {
		total += w.BytesSentCount()
	}
	return total
}

func (pg *PatternGroup) TotalBytesReceived() uint64 {
	var total uint64
	for _, w := range pg.channelWorkers {
		total += w.BytesReceivedCount()
	}
	return total
}

// TotalReconnections returns the max reconnections across channels (connection-level).
func (pg *PatternGroup) TotalReconnections() uint64 {
	var maxReconn uint64
	for _, w := range pg.channelWorkers {
		r := w.ReconnectionCount()
		if r > maxReconn {
			maxReconn = r
		}
	}
	return maxReconn
}

// MaxDowntimeSeconds returns the max downtime across channels (connection-level).
func (pg *PatternGroup) MaxDowntimeSeconds() float64 {
	var maxDt float64
	for _, w := range pg.channelWorkers {
		dt := w.DowntimeSeconds()
		if dt > maxDt {
			maxDt = dt
		}
	}
	return maxDt
}

func (pg *PatternGroup) TotalRPCSuccess() uint64 {
	var total uint64
	for _, w := range pg.channelWorkers {
		total += w.RPCSuccess()
	}
	return total
}

func (pg *PatternGroup) TotalRPCTimeout() uint64 {
	var total uint64
	for _, w := range pg.channelWorkers {
		total += w.RPCTimeout()
	}
	return total
}

func (pg *PatternGroup) TotalRPCError() uint64 {
	var total uint64
	for _, w := range pg.channelWorkers {
		total += w.RPCError()
	}
	return total
}

func (pg *PatternGroup) TotalUnconfirmed() int64 {
	var total int64
	for _, w := range pg.channelWorkers {
		total += w.UnconfirmedCount()
	}
	return total
}

// PeakThroughput returns the max peak rate across all channels.
func (pg *PatternGroup) PeakThroughput() float64 {
	var maxPeak float64
	for _, w := range pg.channelWorkers {
		p := w.PeakRate().Peak()
		if p > maxPeak {
			maxPeak = p
		}
	}
	return maxPeak
}

func (pg *PatternGroup) TotalOutOfOrder() uint64 {
	var total uint64
	for _, w := range pg.channelWorkers {
		total += w.Tracker().TotalOutOfOrder()
	}
	return total
}

// --- Per-channel data for verdict checks ---

// ChannelSent returns the sent count for a specific channel worker by index.
func (pg *PatternGroup) ChannelSent(i int) uint64 {
	if i < 0 || i >= len(pg.channelWorkers) {
		return 0
	}
	return pg.channelWorkers[i].SentCount()
}

// ChannelReceived returns the received count for a specific channel worker by index.
func (pg *PatternGroup) ChannelReceived(i int) uint64 {
	if i < 0 || i >= len(pg.channelWorkers) {
		return 0
	}
	return pg.channelWorkers[i].ReceivedCount()
}

// ChannelLost returns the lost count for a specific channel worker by index.
func (pg *PatternGroup) ChannelLost(i int) uint64 {
	if i < 0 || i >= len(pg.channelWorkers) {
		return 0
	}
	return pg.channelWorkers[i].Tracker().TotalLost()
}

// ChannelDuplicated returns the duplicate count for a specific channel worker by index.
func (pg *PatternGroup) ChannelDuplicated(i int) uint64 {
	if i < 0 || i >= len(pg.channelWorkers) {
		return 0
	}
	return pg.channelWorkers[i].Tracker().TotalDuplicates()
}

// ChannelCorrupted returns the corrupted count for a specific channel worker by index.
func (pg *PatternGroup) ChannelCorrupted(i int) uint64 {
	if i < 0 || i >= len(pg.channelWorkers) {
		return 0
	}
	return pg.channelWorkers[i].CorruptedCount()
}

// ChannelErrors returns the error count for a specific channel worker by index.
func (pg *PatternGroup) ChannelErrors(i int) uint64 {
	if i < 0 || i >= len(pg.channelWorkers) {
		return 0
	}
	return pg.channelWorkers[i].ErrorCount()
}

// --- Lifecycle methods ---

// StartProducersAll starts all producers across all channel workers.
func (pg *PatternGroup) StartProducersAll() {
	for _, w := range pg.channelWorkers {
		w.StartProducers()
	}
}

// StopProducers stops all producers across all channel workers.
func (pg *PatternGroup) StopProducers() {
	for _, w := range pg.channelWorkers {
		w.StopProducers()
	}
}

// StopConsumers stops all consumers across all channel workers.
func (pg *PatternGroup) StopConsumers() {
	for _, w := range pg.channelWorkers {
		w.StopConsumers()
	}
}

// ResetAfterWarmup resets all channel workers and the pattern-level accumulator.
// Queue patterns are skipped: their persistent broker channels retain in-flight
// messages across the reset boundary, and producers keep their sequence counters
// running, so wiping the Tracker mid-sequence causes false duplicates and a
// received-vs-sent mismatch.
func (pg *PatternGroup) ResetAfterWarmup() {
	if pg.pattern == "queue_stream" || pg.pattern == "queue_simple" {
		slog.Info("skipping warmup reset for persistent queue pattern", "pattern", pg.pattern)
		return
	}
	pg.patternLatAccum.Reset()
	for _, w := range pg.channelWorkers {
		w.ResetAfterWarmup()
	}
}
