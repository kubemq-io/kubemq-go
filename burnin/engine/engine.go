package engine

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
	"github.com/kubemq-io/kubemq-go/v2/burnin/config"
	"github.com/kubemq-io/kubemq-go/v2/burnin/disconnect"
	"github.com/kubemq-io/kubemq-go/v2/burnin/metrics"
	"github.com/kubemq-io/kubemq-go/v2/burnin/payload"
	"github.com/kubemq-io/kubemq-go/v2/burnin/report"
	"github.com/kubemq-io/kubemq-go/v2/burnin/server"
	"github.com/kubemq-io/kubemq-go/v2/burnin/worker"
)

// Engine orchestrates all burn-in workers, periodic tasks, and shutdown.
type Engine struct {
	cfg     *config.Config
	logger  *slog.Logger
	cp      *worker.ClientProvider
	workers []worker.Worker
	srv     *server.Server
	started time.Time

	mu          sync.Mutex
	client      *kubemq.Client
	baselineRSS float64
	peakRSS     float64

	// Downtime delta tracking: stores the last-seen cumulative downtime per
	// pattern so we can compute the delta on each tick and add only the new
	// portion to the Prometheus counter.
	lastDowntime map[string]float64

	// Per-pattern status tracking for the /status endpoint.
	patternStatus sync.Map // map[string]string
}

// New creates a new Engine with the given config and logger.
func New(cfg *config.Config, logger *slog.Logger) *Engine {
	return &Engine{
		cfg:          cfg,
		logger:       logger,
		lastDowntime: make(map[string]float64),
	}
}

// Run is the main entry point. It creates the client, starts all workers,
// runs until duration/context cancellation, then shuts down.
func (e *Engine) Run(ctx context.Context) error {
	e.started = time.Now()

	// Create SDK client
	client, err := e.createClient(ctx)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	e.client = client
	e.cp = worker.NewClientProvider(client)

	// Ping broker
	info, err := client.Ping(ctx)
	if err != nil {
		return fmt.Errorf("ping broker: %w", err)
	}
	e.logger.Info("connected to broker", "host", info.Host, "version", info.Version)

	// Clean stale channels from previous runs before creating new ones.
	e.cleanStaleChannels(ctx)

	// Create workers
	e.workers = []worker.Worker{
		worker.NewEventsWorker(e.cfg, e.cp, e.logger),
		worker.NewEventsStoreWorker(e.cfg, e.cp, e.logger),
		worker.NewQueueStreamWorker(e.cfg, e.cp, e.logger),
		worker.NewQueueSimpleWorker(e.cfg, e.cp, e.logger),
		worker.NewCommandsWorker(e.cfg, e.cp, e.logger),
		worker.NewQueriesWorker(e.cfg, e.cp, e.logger),
	}

	// Initialise pattern status entries.
	for _, w := range e.workers {
		e.patternStatus.Store(w.Pattern(), "starting")
	}

	// Start HTTP server
	e.srv = server.New(e.cfg.Metrics.Port, e.summaryFn, e.statusFn)
	e.srv.Start()
	e.logger.Info("HTTP server started", "port", e.cfg.Metrics.Port)

	// Set target rate gauges
	for _, w := range e.workers {
		metrics.TargetRate.WithLabelValues(metrics.SDK(), w.Pattern()).Set(float64(e.getTargetRate(w.Pattern())))
	}

	// Start all workers
	for _, w := range e.workers {
		if err := w.Start(ctx); err != nil {
			return fmt.Errorf("start worker %s: %w", w.Pattern(), err)
		}
		e.logger.Info("worker started", "pattern", w.Pattern())
	}

	// Wait for all consumers to be ready
	for _, w := range e.workers {
		select {
		case <-w.ConsumerReady():
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
			return fmt.Errorf("timeout waiting for consumer ready: %s", w.Pattern())
		}
	}
	e.logger.Info("all consumers ready")

	// Run warmup verification
	if err := e.runWarmup(ctx); err != nil {
		return fmt.Errorf("warmup failed: %w", err)
	}

	// Mark ready and set all patterns to running.
	e.srv.SetReady(true)
	for _, w := range e.workers {
		e.patternStatus.Store(w.Pattern(), "running")
		metrics.SetActiveConnections(w.Pattern(), 1.0)
	}
	e.logger.Info("burn-in test started", "mode", e.cfg.Mode, "duration", e.cfg.Duration)

	// Handle warmup duration exclusion
	if e.cfg.WarmupDuration > 0 {
		metrics.WarmupActive.Set(1)
		go func() {
			select {
			case <-time.After(e.cfg.WarmupDuration):
				metrics.WarmupActive.Set(0)
				// Reset worker accumulators
				for _, w := range e.workers {
					if bw, ok := w.(interface{ ResetAfterWarmup() }); ok {
						bw.ResetAfterWarmup()
					}
				}
				e.logger.Info("warmup period ended, metrics reset")
			case <-ctx.Done():
			}
		}()
	}

	// Start periodic tasks
	go e.periodicReporter(ctx)
	go e.peakRateAdvancer(ctx)
	go e.uptimeTracker(ctx)
	go e.memoryTracker(ctx)
	go e.timestampPurger(ctx)

	// Start forced disconnect manager
	if e.cfg.ForcedDisconnect.Interval > 0 {
		dm := disconnect.New(e.cfg.ForcedDisconnect.Interval, e.cfg.ForcedDisconnect.Duration, e, e.logger)
		go dm.Run(ctx)
	}

	// Wait for duration or context cancellation
	if e.cfg.Duration > 0 {
		maxDur := e.cfg.Duration
		if e.cfg.Thresholds.MaxDuration > 0 && e.cfg.Thresholds.MaxDuration < maxDur {
			maxDur = e.cfg.Thresholds.MaxDuration
		}
		select {
		case <-time.After(maxDur):
			e.logger.Info("test duration reached")
		case <-ctx.Done():
			e.logger.Info("context cancelled")
		}
	} else {
		// Duration 0 = run until stopped, but respect max_duration
		if e.cfg.Thresholds.MaxDuration > 0 {
			select {
			case <-time.After(e.cfg.Thresholds.MaxDuration):
				e.logger.Info("max duration reached")
			case <-ctx.Done():
				e.logger.Info("context cancelled")
			}
		} else {
			<-ctx.Done()
			e.logger.Info("context cancelled")
		}
	}

	return nil
}

// Shutdown performs 2-phase graceful shutdown: stop producers, drain, stop
// consumers, generate report. Returns true if the verdict passed.
func (e *Engine) Shutdown() bool {
	e.logger.Info("shutting down...")
	e.srv.SetReady(false)

	// --- Phase 1: stop producers (stop sending) ---
	for _, w := range e.workers {
		w.StopProducers()
		e.patternStatus.Store(w.Pattern(), "draining")
		e.logger.Info("producer stopped", "pattern", w.Pattern())
	}

	// Wait for drain
	drainTimeout := time.Duration(e.cfg.Shutdown.DrainTimeoutSeconds) * time.Second
	e.logger.Info("waiting for drain", "timeout", drainTimeout)
	time.Sleep(drainTimeout)

	// --- Phase 2: stop consumers (stop receiving) ---
	for _, w := range e.workers {
		w.StopConsumers()
		e.patternStatus.Store(w.Pattern(), "stopped")
		e.logger.Info("consumer stopped", "pattern", w.Pattern())
	}

	// If memory baseline was never set (test shorter than 5 min), snapshot now.
	e.mu.Lock()
	if e.baselineRSS == 0 {
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		e.baselineRSS = float64(memStats.Sys) / 1024 / 1024
	}
	e.mu.Unlock()

	// Generate and print report
	summary := e.buildSummary("completed")
	verdict := report.Generate(summary, e.cfg)
	summary.Verdict = *verdict

	// Write JSON report first (before console output) so the file is
	// available even if the process is interrupted during printing.
	if e.cfg.Output.ReportFile != "" {
		if err := report.WriteJSON(summary, e.cfg.Output.ReportFile); err != nil {
			e.logger.Error("failed to write report file", "error", err)
		} else {
			e.logger.Info("report written", "path", e.cfg.Output.ReportFile)
		}
	}

	report.PrintConsole(summary)

	// Cleanup channels
	if e.cfg.Shutdown.CleanupChannels {
		e.cleanupChannels()
	}

	// Close client
	if e.client != nil {
		_ = e.client.Close()
	}

	// Stop HTTP server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e.srv != nil {
		_ = e.srv.Stop(ctx)
	}

	return verdict.Passed
}

// Close implements disconnect.ClientRecreator.
// Sets active connections to 0 and readiness to false during disconnect.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.srv != nil {
		e.srv.SetReady(false)
	}

	// Mark all patterns as disconnected.
	for _, w := range e.workers {
		metrics.SetActiveConnections(w.Pattern(), 0.0)
		e.patternStatus.Store(w.Pattern(), "disconnected")
	}

	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

// Recreate implements disconnect.ClientRecreator.
// Restores active connections and readiness after reconnect.
func (e *Engine) Recreate(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	client, err := e.createClient(ctx)
	if err != nil {
		return err
	}
	e.client = client
	e.cp.Set(client)

	// Restore active connection gauges and readiness.
	for _, w := range e.workers {
		metrics.SetActiveConnections(w.Pattern(), 1.0)
		e.patternStatus.Store(w.Pattern(), "running")
	}
	if e.srv != nil {
		e.srv.SetReady(true)
	}

	e.logger.Info("client recreated after forced disconnect")
	return nil
}

func (e *Engine) createClient(ctx context.Context) (*kubemq.Client, error) {
	return kubemq.NewClient(ctx,
		kubemq.WithAddress(e.cfg.BrokerHost(), e.cfg.BrokerPort()),
		kubemq.WithClientId(fmt.Sprintf("%s-%s", e.cfg.Broker.ClientIDPrefix, e.cfg.RunID)),
		kubemq.WithReconnectPolicy(kubemq.ReconnectPolicy{
			MaxAttempts:  0, // unlimited
			InitialDelay: e.cfg.Recovery.ReconnectInterval,
			MaxDelay:     e.cfg.Recovery.ReconnectMaxInterval,
			Multiplier:   e.cfg.Recovery.ReconnectMultiplier,
		}),
		kubemq.WithCheckConnection(true),
	)
}

// runWarmup performs real warmup verification for each pattern.
// For events and events_store patterns we subscribe temporarily, send 10
// warmup messages, and verify receipt. For queue and RPC patterns we verify
// the broker path with a Ping.
func (e *Engine) runWarmup(ctx context.Context) error {
	e.logger.Info("running warmup verification...")

	warmupTimeout := 30 * time.Second
	warmupCtx, cancel := context.WithTimeout(ctx, warmupTimeout)
	defer cancel()

	client := e.cp.Get()
	const warmupCount = 10

	for _, w := range e.workers {
		pattern := w.Pattern()

		switch pattern {
		case worker.PatternEvents:
			// Subscribe temporarily, send warmup events, verify receipt.
			if err := e.warmupPubSub(warmupCtx, client, w, warmupCount, false); err != nil {
				return fmt.Errorf("warmup events: %w", err)
			}

		case worker.PatternEventsStore:
			// Same approach for events_store but with store subscription.
			if err := e.warmupPubSub(warmupCtx, client, w, warmupCount, true); err != nil {
				return fmt.Errorf("warmup events_store: %w", err)
			}

		case worker.PatternQueueStream, worker.PatternQueueSimple:
			if err := e.warmupQueue(warmupCtx, client, w, warmupCount); err != nil {
				return fmt.Errorf("warmup %s: %w", pattern, err)
			}
		case worker.PatternCommands:
			if err := e.warmupCommand(warmupCtx, client, w); err != nil {
				return fmt.Errorf("warmup commands: %w", err)
			}
		case worker.PatternQueries:
			if err := e.warmupQuery(warmupCtx, client, w); err != nil {
				return fmt.Errorf("warmup queries: %w", err)
			}
		}
	}

	e.logger.Info("warmup verification passed")
	return nil
}

// warmupPubSub sends warmupCount messages to the worker's channel and verifies
// receipt through a temporary subscription.
func (e *Engine) warmupPubSub(ctx context.Context, client *kubemq.Client, w worker.Worker, count int, isStore bool) error {
	var channelName string
	if bw, ok := w.(interface{ ChannelName() string }); ok {
		channelName = bw.ChannelName()
	} else {
		return fmt.Errorf("worker %s does not expose ChannelName()", w.Pattern())
	}

	received := make(chan struct{}, count)

	if isStore {
		sub, err := client.SubscribeToEventsStore(ctx, channelName, "",
			kubemq.StartFromNewEvents(),
			kubemq.WithOnEventStoreReceive(func(event *kubemq.EventStoreReceive) {
				if event.Tags != nil && event.Tags["warmup"] == "true" {
					select {
					case received <- struct{}{}:
					default:
					}
				}
			}),
			kubemq.WithOnError(func(err error) {
				e.logger.Warn("warmup events_store subscription error", "error", err)
			}),
		)
		if err != nil {
			return fmt.Errorf("warmup subscribe events_store: %w", err)
		}
		defer sub.Cancel()
	} else {
		sub, err := client.SubscribeToEvents(ctx, channelName, "",
			kubemq.WithOnEvent(func(event *kubemq.Event) {
				if event.Tags != nil && event.Tags["warmup"] == "true" {
					select {
					case received <- struct{}{}:
					default:
					}
				}
			}),
			kubemq.WithOnError(func(err error) {
				e.logger.Warn("warmup events subscription error", "error", err)
			}),
		)
		if err != nil {
			return fmt.Errorf("warmup subscribe events: %w", err)
		}
		defer sub.Cancel()
	}

	// Brief pause to let the subscription register at the broker.
	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	// Send warmup messages using the correct API for the pattern.
	if isStore {
		if err := SendWarmupEventStoreMessages(ctx, client, channelName, count); err != nil {
			return err
		}
	} else {
		if err := SendWarmupMessages(ctx, client, channelName, count); err != nil {
			return err
		}
	}

	// Wait for at least one message to verify the round-trip.
	select {
	case <-received:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("warmup: timed out waiting for messages on %s", channelName)
	}
}

func (e *Engine) warmupQueue(ctx context.Context, client *kubemq.Client, w worker.Worker, count int) error {
	var channelName string
	if bw, ok := w.(interface{ ChannelName() string }); ok {
		channelName = bw.ChannelName()
	} else {
		return fmt.Errorf("worker %s does not expose ChannelName()", w.Pattern())
	}

	// Send warmup messages
	for i := 0; i < count; i++ {
		body, crcHex := payload.Encode(metrics.SDK(), "warmup", "warmup", uint64(i+1), 256)
		msg := &kubemq.QueueMessage{
			Channel: channelName,
			Body:    body,
			Tags:    map[string]string{"warmup": "true", "content_hash": crcHex},
		}
		result, err := client.SendQueueMessage(ctx, msg)
		if err != nil {
			return fmt.Errorf("warmup queue send %d: %w", i+1, err)
		}
		if result.IsError {
			return fmt.Errorf("warmup queue send %d: %s", i+1, result.Error)
		}
	}

	// Receive at least one message to verify round-trip
	resp, err := client.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
		Channel:             channelName,
		MaxNumberOfMessages: int32(count),
		WaitTimeSeconds:     5,
	})
	if err != nil {
		return fmt.Errorf("warmup queue receive: %w", err)
	}
	if resp.MessagesReceived == 0 {
		return fmt.Errorf("warmup queue: no messages received on %s", channelName)
	}
	return nil
}

func (e *Engine) warmupCommand(ctx context.Context, client *kubemq.Client, w worker.Worker) error {
	var channelName string
	if bw, ok := w.(interface{ ChannelName() string }); ok {
		channelName = bw.ChannelName()
	} else {
		return fmt.Errorf("worker %s does not expose ChannelName()", w.Pattern())
	}

	// Subscribe a temporary responder that auto-responds
	sub, err := client.SubscribeToCommands(ctx, channelName, "",
		kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
			_ = client.SendResponse(ctx, &kubemq.Response{
				RequestId:  cmd.Id,
				ResponseTo: cmd.ResponseTo,
				ExecutedAt: time.Now(),
			})
		}),
		kubemq.WithOnError(func(err error) {}),
	)
	if err != nil {
		return fmt.Errorf("warmup subscribe commands: %w", err)
	}
	defer sub.Cancel()

	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	body, crcHex := payload.Encode(metrics.SDK(), "warmup", "warmup", 1, 256)
	_, err = client.SendCommand(ctx, &kubemq.Command{
		Channel: channelName,
		Body:    body,
		Timeout: 5 * time.Second,
		Tags:    map[string]string{"warmup": "true", "content_hash": crcHex},
	})
	if err != nil {
		return fmt.Errorf("warmup command send: %w", err)
	}
	return nil
}

func (e *Engine) warmupQuery(ctx context.Context, client *kubemq.Client, w worker.Worker) error {
	var channelName string
	if bw, ok := w.(interface{ ChannelName() string }); ok {
		channelName = bw.ChannelName()
	} else {
		return fmt.Errorf("worker %s does not expose ChannelName()", w.Pattern())
	}

	sub, err := client.SubscribeToQueries(ctx, channelName, "",
		kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
			_ = client.SendResponse(ctx, &kubemq.Response{
				RequestId:  q.Id,
				ResponseTo: q.ResponseTo,
				Body:       q.Body,
				ExecutedAt: time.Now(),
			})
		}),
		kubemq.WithOnError(func(err error) {}),
	)
	if err != nil {
		return fmt.Errorf("warmup subscribe queries: %w", err)
	}
	defer sub.Cancel()

	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	body, crcHex := payload.Encode(metrics.SDK(), "warmup", "warmup", 1, 256)
	_, err = client.SendQuery(ctx, &kubemq.Query{
		Channel: channelName,
		Body:    body,
		Timeout: 5 * time.Second,
		Tags:    map[string]string{"warmup": "true", "content_hash": crcHex},
	})
	if err != nil {
		return fmt.Errorf("warmup query send: %w", err)
	}
	return nil
}

// cleanStaleChannels lists channels matching the go_burnin_* prefix across all
// channel types and deletes any leftovers from previous runs.
// NOTE: We intentionally use the broad "go_burnin_" prefix (not scoped to our
// RunID) so that channels from *any* previous burn-in run are cleaned up before
// we start. This prevents interference from stale channels left behind by
// earlier runs that may not have shut down cleanly.
func (e *Engine) cleanStaleChannels(ctx context.Context) {
	e.logger.Info("cleaning stale channels from all previous burn-in runs")
	client := e.cp.Get()
	if client == nil {
		return
	}

	prefix := "go_burnin_"
	channelTypes := []string{
		kubemq.ChannelTypeEvents,
		kubemq.ChannelTypeEventsStore,
		kubemq.ChannelTypeQueues,
		kubemq.ChannelTypeCommands,
		kubemq.ChannelTypeQueries,
	}

	cleanCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	for _, ct := range channelTypes {
		channels, err := client.ListChannels(cleanCtx, ct, prefix)
		if err != nil {
			e.logger.Warn("failed to list stale channels", "type", ct, "error", err)
			continue
		}
		for _, ch := range channels {
			e.logger.Warn("deleting stale channel from previous run", "name", ch.Name, "type", ct)
			if delErr := client.DeleteChannel(cleanCtx, ch.Name, ct); delErr != nil {
				e.logger.Warn("failed to delete stale channel", "name", ch.Name, "error", delErr)
			} else {
				e.logger.Info("deleted stale channel", "name", ch.Name)
			}
		}
	}
}

func (e *Engine) periodicReporter(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.Metrics.ReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.logStatus()
			e.updateGapDetection()
			e.updateLagMetrics()
			e.updateDowntimeMetrics()
			e.updateActiveConnections()
		}
	}
}

func (e *Engine) peakRateAdvancer(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, w := range e.workers {
				w.PeakRate().Advance()
			}
		}
	}
}

// logStatus prints a periodic status line.
// Uses plain fmt.Printf for text format (matching the spec) and slog for JSON.
func (e *Engine) logStatus() {
	elapsed := time.Since(e.started)
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	rssMB := float64(memStats.Sys) / 1024 / 1024

	if e.cfg.Logging.Format == "json" {
		// Structured JSON logging via slog.
		e.logger.Info("BURN-IN STATUS",
			"uptime", elapsed.Truncate(time.Second).String(),
			"mode", e.cfg.Mode,
			"rss_mb", fmt.Sprintf("%.1f", rssMB),
		)
		for _, w := range e.workers {
			sent := w.SentCount()
			recv := w.ReceivedCount()
			lost := w.Tracker().TotalLost()
			dup := w.Tracker().TotalDuplicates()
			p99 := w.LatencyAccumulator().Percentile(99).Seconds() * 1000

			e.logger.Info("  pattern status",
				"pattern", w.Pattern(),
				"sent", sent,
				"recv", recv,
				"lost", lost,
				"dup", dup,
				"p99_ms", fmt.Sprintf("%.2f", p99),
			)
		}
		return
	}

	// Plain text format matching the spec.
	now := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("[%s] BURN-IN STATUS | uptime=%s mode=%s rss=%.0fMB\n",
		now,
		formatDuration(elapsed),
		e.cfg.Mode,
		rssMB,
	)

	for _, w := range e.workers {
		sent := w.SentCount()
		recv := w.ReceivedCount()
		lost := w.Tracker().TotalLost()
		dup := w.Tracker().TotalDuplicates()
		errCount := w.ErrorCount()
		p99 := w.LatencyAccumulator().Percentile(99).Seconds() * 1000

		rateStr := "0"
		if secs := elapsed.Seconds(); secs > 0 {
			rateStr = fmt.Sprintf("%.0f", float64(sent)/secs)
		}

		if w.Pattern() == worker.PatternCommands || w.Pattern() == worker.PatternQueries {
			fmt.Printf("  %-14s sent=%-8d resp=%-8d tout=%-4d err=%-4d p99=%.1fms  rate=%s/s\n",
				w.Pattern()+":",
				sent, w.RPCSuccess(), w.RPCTimeout(), errCount, p99, rateStr)
		} else {
			fmt.Printf("  %-14s sent=%-8d recv=%-8d lost=%-4d dup=%-4d err=%-4d p99=%.1fms  rate=%s/s\n",
				w.Pattern()+":",
				sent, recv, lost, dup, errCount, p99, rateStr,
			)
		}
	}
}

func (e *Engine) updateGapDetection() {
	for _, w := range e.workers {
		gaps := w.Tracker().DetectGaps()
		for _, lost := range gaps {
			if lost > 0 {
				metrics.AddLost(w.Pattern(), float64(lost))
			}
		}
	}
}

func (e *Engine) updateLagMetrics() {
	for _, w := range e.workers {
		sent := w.SentCount()
		recv := w.ReceivedCount()
		lag := float64(0)
		if sent > recv {
			lag = float64(sent - recv)
		}
		metrics.ConsumerLag.WithLabelValues(metrics.SDK(), w.Pattern()).Set(lag)
		// Update actual rate
		elapsed := time.Since(e.started).Seconds()
		if elapsed > 0 {
			rate := float64(sent) / elapsed
			metrics.ActualRate.WithLabelValues(metrics.SDK(), w.Pattern()).Set(rate)
		}
	}
}

// updateDowntimeMetrics computes the delta of cumulative downtime for each
// worker and adds only the new portion to the Prometheus counter.
func (e *Engine) updateDowntimeMetrics() {
	for _, w := range e.workers {
		current := w.DowntimeSeconds()
		last, ok := e.lastDowntime[w.Pattern()]
		if !ok {
			last = 0
		}
		delta := current - last
		if delta > 0 {
			metrics.AddDowntime(w.Pattern(), delta)
		}
		e.lastDowntime[w.Pattern()] = current
	}
}

// updateActiveConnections sets the active connections gauge for each worker.
// It also sets the consumer group balance ratio to 1.0 for all patterns.
// Per-consumer tracking is not yet available, so 1.0 indicates the metric is
// not applicable (fan-out mode where all consumers receive everything).
func (e *Engine) updateActiveConnections() {
	for _, w := range e.workers {
		metrics.SetActiveConnections(w.Pattern(), 1.0)
		metrics.SetConsumerGroupBalance(w.Pattern(), 1.0)
	}
}

func (e *Engine) uptimeTracker(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.UptimeSeconds.Set(time.Since(e.started).Seconds())
			metrics.ActiveWorkers.Set(float64(runtime.NumGoroutine()))
		}
	}
}

// memoryTracker samples RSS periodically. The baseline is set at the 5-minute
// mark (not on the first tick) to allow the runtime to stabilise. If the test
// ends before 5 minutes the Shutdown method sets the baseline to a final
// snapshot.
func (e *Engine) memoryTracker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	baselineSet := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			rssMB := float64(memStats.Sys) / 1024 / 1024

			e.mu.Lock()

			// Seed initial values on first sample.
			if e.peakRSS == 0 {
				e.peakRSS = rssMB
			}
			if e.baselineRSS == 0 {
				e.baselineRSS = rssMB // initial estimate, updated at 5-min mark
			}
			if rssMB > e.peakRSS {
				e.peakRSS = rssMB
			}

			// Update baseline at the 5-minute mark for a stabilised reading.
			if !baselineSet && time.Since(e.started) >= 5*time.Minute {
				e.baselineRSS = rssMB
				baselineSet = true
			}

			e.mu.Unlock()
		}
	}
}

func (e *Engine) timestampPurger(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, w := range e.workers {
				if bw, ok := w.(interface{ TSStore() *metrics.SendTimestampStore }); ok {
					bw.TSStore().Purge(60 * time.Second)
				}
			}
		}
	}
}

func (e *Engine) cleanupChannels() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := e.cp.Get()
	if client == nil {
		return
	}

	channelTypes := map[string]string{
		"events":       kubemq.ChannelTypeEvents,
		"events_store": kubemq.ChannelTypeEventsStore,
		"queue_stream": kubemq.ChannelTypeQueues,
		"queue_simple": kubemq.ChannelTypeQueues,
		"commands":     kubemq.ChannelTypeCommands,
		"queries":      kubemq.ChannelTypeQueries,
	}

	for _, w := range e.workers {
		ct, ok := channelTypes[w.Pattern()]
		if !ok {
			continue
		}
		if bw, ok := w.(interface{ ChannelName() string }); ok {
			if err := client.DeleteChannel(ctx, bw.ChannelName(), ct); err != nil {
				e.logger.Warn("failed to delete channel", "channel", bw.ChannelName(), "error", err)
			} else {
				e.logger.Info("deleted channel", "channel", bw.ChannelName())
			}
		}
	}
}

func (e *Engine) getTargetRate(pattern string) int {
	switch pattern {
	case worker.PatternEvents:
		return e.cfg.Rates.Events
	case worker.PatternEventsStore:
		return e.cfg.Rates.EventsStore
	case worker.PatternQueueStream:
		return e.cfg.Rates.QueueStream
	case worker.PatternQueueSimple:
		return e.cfg.Rates.QueueSimple
	case worker.PatternCommands:
		return e.cfg.Rates.Commands
	case worker.PatternQueries:
		return e.cfg.Rates.Queries
	}
	return 0
}

func (e *Engine) buildSummary(status string) *report.Summary {
	elapsed := time.Since(e.started)

	e.mu.Lock()
	baselineRSS := e.baselineRSS
	peakRSS := e.peakRSS
	e.mu.Unlock()

	if baselineRSS == 0 {
		baselineRSS = 1 // avoid division by zero
	}

	sdkVersion := e.cfg.Output.SDKVersion
	if sdkVersion == "" {
		sdkVersion = kubemq.Version
	}

	summary := &report.Summary{
		SDK:           metrics.SDK(),
		Version:       sdkVersion,
		Mode:          e.cfg.Mode,
		BrokerAddress: e.cfg.Broker.Address,
		StartedAt:     e.started,
		EndedAt:       time.Now(),
		DurationSecs:  elapsed.Seconds(),
		Status:        status,
		Patterns:      make(map[string]*report.PatternStats),
		Resources: report.ResourceStats{
			PeakRSSMB:          peakRSS,
			BaselineRSSMB:      baselineRSS,
			MemoryGrowthFactor: peakRSS / baselineRSS,
			PeakWorkers:        runtime.NumGoroutine(),
		},
	}

	for _, w := range e.workers {
		patStatus := "running"
		if v, ok := e.patternStatus.Load(w.Pattern()); ok {
			patStatus = v.(string)
		}
		ps := &report.PatternStats{
			Status:          patStatus,
			Sent:            w.SentCount(),
			Received:        w.ReceivedCount(),
			Lost:            w.Tracker().TotalLost(),
			Duplicated:      w.Tracker().TotalDuplicates(),
			Corrupted:       w.CorruptedCount(),
			OutOfOrder:      w.Tracker().TotalOutOfOrder(),
			Errors:          w.ErrorCount(),
			Reconnections:   w.ReconnectionCount(),
			TargetRate:      e.getTargetRate(w.Pattern()),
			DowntimeSeconds: w.DowntimeSeconds(),
			LatencyP50MS:    w.LatencyAccumulator().Percentile(50).Seconds() * 1000,
			LatencyP95MS:    w.LatencyAccumulator().Percentile(95).Seconds() * 1000,
			LatencyP99MS:    w.LatencyAccumulator().Percentile(99).Seconds() * 1000,
			LatencyP999MS:   w.LatencyAccumulator().Percentile(99.9).Seconds() * 1000,
		}

		if ps.Sent > 0 {
			ps.LossPct = float64(ps.Lost) / float64(ps.Sent) * 100.0
			ps.AvgThroughput = float64(ps.Sent) / elapsed.Seconds()
		}

		// RPC-specific fields for commands/queries
		if w.Pattern() == worker.PatternCommands || w.Pattern() == worker.PatternQueries {
			ps.ResponsesSuccess = w.RPCSuccess()
			ps.ResponsesTimeout = w.RPCTimeout()
			ps.ResponsesError = w.RPCError()
			ps.RPCP50MS = w.RPCLatencyAccumulator().Percentile(50).Seconds() * 1000
			ps.RPCP95MS = w.RPCLatencyAccumulator().Percentile(95).Seconds() * 1000
			ps.RPCP99MS = w.RPCLatencyAccumulator().Percentile(99).Seconds() * 1000
			ps.RPCP999MS = w.RPCLatencyAccumulator().Percentile(99.9).Seconds() * 1000
			if elapsed.Seconds() > 0 {
				ps.AvgThroughputRPC = float64(ps.ResponsesSuccess) / elapsed.Seconds()
			}
		}
		ps.PeakThroughput = w.PeakRate().Peak()

		summary.Patterns[w.Pattern()] = ps
	}

	return summary
}

func (e *Engine) summaryFn() any {
	summary := e.buildSummary("running")
	verdict := report.Generate(summary, e.cfg)
	summary.Verdict = *verdict
	return summary
}

// statusFn reads per-pattern status from the sync.Map so the /status endpoint
// reflects actual worker states (starting, running, draining, disconnected,
// stopped).
func (e *Engine) statusFn() any {
	statuses := make(map[string]string)
	e.patternStatus.Range(func(key, value any) bool {
		statuses[key.(string)] = value.(string)
		return true
	})
	return statuses
}

// CleanupOnly deletes all channels matching the burn-in prefix.
func CleanupOnly(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(cfg.BrokerHost(), cfg.BrokerPort()),
		kubemq.WithClientId("burnin-cleanup"),
		kubemq.WithCheckConnection(true),
	)
	if err != nil {
		return fmt.Errorf("create cleanup client: %w", err)
	}
	defer client.Close()

	prefix := "go_burnin_"
	channelTypes := []string{
		kubemq.ChannelTypeEvents,
		kubemq.ChannelTypeEventsStore,
		kubemq.ChannelTypeQueues,
		kubemq.ChannelTypeCommands,
		kubemq.ChannelTypeQueries,
	}

	for _, ct := range channelTypes {
		channels, err := client.ListChannels(ctx, ct, prefix)
		if err != nil {
			logger.Warn("failed to list channels for cleanup", "type", ct, "error", err)
			continue
		}
		for _, ch := range channels {
			if err := client.DeleteChannel(ctx, ch.Name, ct); err != nil {
				logger.Warn("failed to delete channel", "name", ch.Name, "error", err)
			} else {
				logger.Info("deleted channel", "name", ch.Name)
			}
		}
	}

	return nil
}

// SendWarmupMessages sends fire-and-forget event warmup messages.
func SendWarmupMessages(ctx context.Context, client *kubemq.Client, channelName string, count int) error {
	for i := 0; i < count; i++ {
		body, crcHex := payload.Encode(metrics.SDK(), "warmup", "warmup", uint64(i+1), 256)
		event := &kubemq.Event{
			Channel: channelName,
			Body:    body,
			Tags:    map[string]string{"warmup": "true", "content_hash": crcHex},
		}
		if err := client.SendEvent(ctx, event); err != nil {
			return fmt.Errorf("warmup send %d: %w", i+1, err)
		}
	}
	return nil
}

// SendWarmupEventStoreMessages sends persistent event store warmup messages.
func SendWarmupEventStoreMessages(ctx context.Context, client *kubemq.Client, channelName string, count int) error {
	for i := 0; i < count; i++ {
		body, crcHex := payload.Encode(metrics.SDK(), "warmup", "warmup", uint64(i+1), 256)
		event := &kubemq.EventStore{
			Channel: channelName,
			Body:    body,
			Tags:    map[string]string{"warmup": "true", "content_hash": crcHex},
		}
		result, err := client.SendEventStore(ctx, event)
		if err != nil {
			return fmt.Errorf("warmup event store send %d: %w", i+1, err)
		}
		if result.Err != nil {
			return fmt.Errorf("warmup event store send %d: %w", i+1, result.Err)
		}
	}
	return nil
}

// formatDuration returns a compact human-readable duration like "5m30s" or "2h15m".
func formatDuration(d time.Duration) string {
	d = d.Truncate(time.Second)
	if d == 0 {
		return "0s"
	}

	var parts []string
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60

	if h > 0 {
		parts = append(parts, fmt.Sprintf("%dh", h))
	}
	if m > 0 {
		parts = append(parts, fmt.Sprintf("%dm", m))
	}
	if s > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%ds", s))
	}
	return strings.Join(parts, "")
}
