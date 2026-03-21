package engine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
	"github.com/kubemq-io/kubemq-go/v2/burnin/api"
	"github.com/kubemq-io/kubemq-go/v2/burnin/config"
	"github.com/kubemq-io/kubemq-go/v2/burnin/disconnect"
	"github.com/kubemq-io/kubemq-go/v2/burnin/metrics"
	"github.com/kubemq-io/kubemq-go/v2/burnin/payload"
	"github.com/kubemq-io/kubemq-go/v2/burnin/report"
	"github.com/kubemq-io/kubemq-go/v2/burnin/worker"
)

// Run states per spec
const (
	StateIdle     = "idle"
	StateStarting = "starting"
	StateRunning  = "running"
	StateStopping = "stopping"
	StateStopped  = "stopped"
	StateError    = "error"
)

// PatternSnapshot holds a point-in-time capture of all PatternGroup counters,
// taken at the moment producers stop (T2). Used by buildSummary so the final
// report reflects only the measured window, not the drain period.
type PatternSnapshot struct {
	Sent            uint64
	Received        uint64
	Lost            uint64
	Duplicated      uint64
	Corrupted       uint64
	OutOfOrder      uint64
	Errors          uint64
	Reconnections   uint64
	BytesSent       uint64
	BytesReceived   uint64
	RpcSuccess      uint64
	RpcTimeout      uint64
	RpcError        uint64
	Unconfirmed     int64
	PeakThroughput  float64
	DowntimeSeconds float64

	// Latency percentiles (ms)
	LatencyP50MS  float64
	LatencyP95MS  float64
	LatencyP99MS  float64
	LatencyP999MS float64

	// Per-channel data
	PerChannel []ChannelSnapshot

	// Per-worker producer/consumer snapshots
	PerWorkerProducers [][]worker.WorkerStatSnapshot // indexed by channel worker
	PerWorkerConsumers [][]worker.WorkerStatSnapshot
}

// ChannelSnapshot holds per-channel counters at snapshot time.
type ChannelSnapshot struct {
	Sent       uint64
	Received   uint64
	Lost       uint64
	Duplicated uint64
	Corrupted  uint64
	Errors     uint64
}

// Engine orchestrates burn-in runs with API-driven lifecycle.
type Engine struct {
	startupCfg *config.Config
	logger     *slog.Logger
	bootTime   time.Time

	// State machine (mutex-protected)
	mu                 sync.Mutex
	state              string
	runCfg             *config.Config
	runID              string
	runStartedAt       time.Time
	runEndedAt         time.Time
	producersStartedAt time.Time
	producersStoppedAt time.Time
	runError           string
	runCancel          context.CancelFunc
	client             *kubemq.Client
	cp                 *worker.ClientProvider
	patternGroups      map[string]*PatternGroup // v2: pattern -> PatternGroup
	baselineRSS        float64
	peakRSS            float64
	lastDowntime       map[string]float64
	patternStatus      sync.Map
	lastReport         *report.Summary
	runDone            chan struct{}
	warmupActive       atomic.Bool
	baselineFullWindow bool

	// Snapshot of all pattern counters at producer-stop time (T2).
	// When set, buildSummary uses these instead of live values.
	producerStopSnapshot map[string]*PatternSnapshot
}

// New creates a new Engine with the given startup config and logger.
func New(cfg *config.Config, logger *slog.Logger) *Engine {
	return &Engine{
		startupCfg:   cfg,
		logger:       logger,
		bootTime:     time.Now(),
		state:        StateIdle,
		lastDowntime: make(map[string]float64),
	}
}

// State returns the current run state (thread-safe).
func (e *Engine) State() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state
}

// RunID returns the current or last run ID.
func (e *Engine) RunID() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runID
}

// RunStartedAt returns the started_at time as ISO 8601 string, or empty.
func (e *Engine) RunStartedAt() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runStartedAt.IsZero() {
		return ""
	}
	return e.runStartedAt.Format(time.RFC3339)
}

// RunError returns the current run error message.
func (e *Engine) RunError() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runError
}

// StartRun begins a new burn-in run from an API config. Returns run_id and enabled count.
func (e *Engine) StartRun(rc *api.RunConfig) (string, int, error) {
	e.mu.Lock()
	if e.state != StateIdle && e.state != StateStopped && e.state != StateError {
		st := e.state
		e.mu.Unlock()
		return "", 0, fmt.Errorf("cannot start run in state %s", st)
	}

	runCfg, err := rc.ToInternalConfig(e.startupCfg)
	if err != nil {
		e.mu.Unlock()
		return "", 0, fmt.Errorf("config translation: %w", err)
	}

	e.state = StateStarting
	e.runCfg = runCfg
	e.runID = runCfg.RunID
	e.runStartedAt = time.Now()
	e.runEndedAt = time.Time{}
	e.producersStartedAt = time.Time{}
	e.producersStoppedAt = time.Time{}
	e.runError = ""
	e.baselineRSS = 0
	e.peakRSS = 0
	e.baselineFullWindow = false
	e.lastDowntime = make(map[string]float64)
	e.lastReport = nil
	e.patternGroups = nil
	e.producerStopSnapshot = nil
	e.client = nil
	e.cp = nil
	e.runDone = make(chan struct{})

	enabledCount := rc.EnabledPatternCount()
	e.mu.Unlock()

	// Launch the run in a background goroutine
	ctx, cancel := context.WithCancel(context.Background())
	e.mu.Lock()
	e.runCancel = cancel
	e.mu.Unlock()

	go e.runLoop(ctx, runCfg)

	return runCfg.RunID, enabledCount, nil
}

// StartRunFromConfig starts a run using an internal Config directly (for CLI backward compat).
func (e *Engine) StartRunFromConfig(cfg *config.Config) error {
	e.mu.Lock()
	if e.state != StateIdle && e.state != StateStopped && e.state != StateError {
		st := e.state
		e.mu.Unlock()
		return fmt.Errorf("cannot start run in state %s", st)
	}

	e.state = StateStarting
	e.runCfg = cfg
	e.runID = cfg.RunID
	e.runStartedAt = time.Now()
	e.runEndedAt = time.Time{}
	e.producersStartedAt = time.Time{}
	e.producersStoppedAt = time.Time{}
	e.runError = ""
	e.baselineRSS = 0
	e.peakRSS = 0
	e.baselineFullWindow = false
	e.lastDowntime = make(map[string]float64)
	e.lastReport = nil
	e.patternGroups = nil
	e.producerStopSnapshot = nil
	e.client = nil
	e.cp = nil
	e.runDone = make(chan struct{})
	e.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	e.mu.Lock()
	e.runCancel = cancel
	e.mu.Unlock()

	go e.runLoop(ctx, cfg)

	return nil
}

// StopRun initiates graceful stop of the active run.
func (e *Engine) StopRun() error {
	e.mu.Lock()
	if e.state != StateStarting && e.state != StateRunning {
		st := e.state
		e.mu.Unlock()
		return fmt.Errorf("cannot stop run in state %s", st)
	}
	e.state = StateStopping
	cancel := e.runCancel
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	return nil
}

// WaitForRun blocks until the current run completes.
func (e *Engine) WaitForRun() {
	e.mu.Lock()
	done := e.runDone
	e.mu.Unlock()
	if done != nil {
		<-done
	}
}

// allWorkers returns a flattened list of all workers from all pattern groups.
func (e *Engine) allWorkers() []worker.Worker {
	var workers []worker.Worker
	for _, pg := range e.patternGroups {
		workers = append(workers, pg.ChannelWorkers()...)
	}
	return workers
}

// runLoop is the main run execution goroutine.
func (e *Engine) runLoop(ctx context.Context, cfg *config.Config) {
	defer func() {
		e.mu.Lock()
		if e.runDone != nil {
			close(e.runDone)
		}
		e.mu.Unlock()
	}()

	startingTimeout := time.Duration(cfg.StartingTimeoutSeconds) * time.Second
	startingCtx, startingCancel := context.WithTimeout(ctx, startingTimeout)

	err := e.runStartup(startingCtx, ctx, cfg)
	startingCancel()

	if err != nil {
		e.mu.Lock()
		if e.state == StateStopping {
			e.finishRunLocked("stopped", "")
		} else {
			e.finishRunLocked("error", fmt.Sprintf("Starting failed: %s", err.Error()))
		}
		e.mu.Unlock()
		return
	}

	// Transition to running
	e.mu.Lock()
	if e.state == StateStopping {
		e.mu.Unlock()
		e.shutdownWorkers(cfg)
		e.mu.Lock()
		e.finishRunLocked("stopped", "")
		e.mu.Unlock()
		return
	}
	e.state = StateRunning
	e.mu.Unlock()

	// Start ALL producers across ALL pattern groups
	e.producersStartedAt = time.Now()
	for _, pg := range e.patternGroups {
		for _, w := range pg.ChannelWorkers() {
			w.StartProducers()
		}
		e.patternStatus.Store(pg.Pattern(), StateRunning)
		metrics.SetActiveConnections(pg.Pattern(), 1.0)
	}

	e.logger.Info("burn-in test started", "mode", cfg.Mode, "duration", cfg.Duration,
		"total_channels", cfg.TotalChannelCount())

	if cfg.WarmupDuration > 0 {
		metrics.WarmupActive.Set(1)
		e.warmupActive.Store(true)
		go func() {
			select {
			case <-time.After(cfg.WarmupDuration):
				metrics.WarmupActive.Set(0)
				e.warmupActive.Store(false)
				for _, pg := range e.patternGroups {
					pg.ResetAfterWarmup()
				}
				e.logger.Info("warmup period ended, metrics reset")
			case <-ctx.Done():
				e.warmupActive.Store(false)
			}
		}()
	}

	// Start periodic tasks
	go e.periodicReporter(ctx, cfg)
	go e.peakRateAdvancer(ctx)
	go e.uptimeTracker(ctx)
	go e.memoryTracker(ctx)
	go e.timestampPurger(ctx)

	// Forced disconnect
	if cfg.ForcedDisconnect.Interval > 0 {
		dm := disconnect.New(cfg.ForcedDisconnect.Interval, cfg.ForcedDisconnect.Duration, e, e.logger)
		go dm.Run(ctx)
	}

	// Wait for duration or context cancellation
	if cfg.Duration > 0 {
		maxDur := cfg.Duration
		if cfg.Thresholds.MaxDuration > 0 && cfg.Thresholds.MaxDuration < maxDur {
			maxDur = cfg.Thresholds.MaxDuration
		}
		select {
		case <-time.After(maxDur):
			e.logger.Info("test duration reached")
		case <-ctx.Done():
			e.logger.Info("context cancelled")
		}
	} else {
		if cfg.Thresholds.MaxDuration > 0 {
			select {
			case <-time.After(cfg.Thresholds.MaxDuration):
				e.logger.Info("max duration reached")
			case <-ctx.Done():
				e.logger.Info("context cancelled")
			}
		} else {
			<-ctx.Done()
			e.logger.Info("context cancelled")
		}
	}

	// Shutdown workers and generate report
	e.mu.Lock()
	if e.state == StateRunning {
		e.state = StateStopping
	}
	e.mu.Unlock()

	e.shutdownWorkers(cfg)

	e.mu.Lock()
	e.finishRunLocked("stopped", "")
	e.mu.Unlock()
}

// runStartup handles the startup phase: connect, create channels, start consumers, warmup.
func (e *Engine) runStartup(startingCtx context.Context, runCtx context.Context, cfg *config.Config) error {
	client, err := e.createClient(startingCtx, cfg)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	e.mu.Lock()
	e.client = client
	e.cp = worker.NewClientProvider(client)
	e.mu.Unlock()

	info, err := client.Ping(startingCtx)
	if err != nil {
		return fmt.Errorf("ping broker: %w", err)
	}
	e.logger.Info("connected to broker", "host", info.Host, "version", info.Version)

	e.logger.Info("skipping stale channel cleanup at startup (channels auto-create on subscribe/send)")

	// Create PatternGroups (v2)
	patternGroups := make(map[string]*PatternGroup)
	for _, pname := range config.AllPatternNames {
		pc := cfg.GetPatternConfig(pname)
		if pc == nil || !pc.Enabled {
			continue
		}
		if enabled, ok := cfg.PatternEnabled[pname]; ok && !enabled {
			continue
		}
		pg := NewPatternGroup(pname, pc, cfg, cfg.RunID, e.cp, e.logger)
		patternGroups[pname] = pg
		e.patternStatus.Store(pname, StateStarting)
	}

	e.mu.Lock()
	e.patternGroups = patternGroups
	e.mu.Unlock()

	// Set target rate gauges (total target = rate * channels)
	for pname, pg := range patternGroups {
		totalTargetRate := pg.PatternConfig().Rate * pg.ChannelCount()
		metrics.TargetRate.WithLabelValues(metrics.SDK(), pname).Set(float64(totalTargetRate))
	}

	// Start ALL consumers/responders across ALL channels across ALL patterns
	for _, pg := range patternGroups {
		for _, w := range pg.ChannelWorkers() {
			if err := w.Start(runCtx); err != nil {
				return fmt.Errorf("start worker %s channel %s: %w", w.Pattern(), w.ChannelName(), err)
			}
		}
	}

	// Wait for ALL consumer readiness (bounded by startingCtx timeout)
	for _, pg := range patternGroups {
		for _, w := range pg.ChannelWorkers() {
			select {
			case <-w.ConsumerReady():
			case <-startingCtx.Done():
				return startingCtx.Err()
			}
		}
	}
	e.logger.Info("all consumers ready across all channels")

	// Warmup verification: all channels
	if err := e.runWarmupV2(startingCtx, cfg); err != nil {
		return fmt.Errorf("warmup failed: %w", err)
	}

	return nil
}

// shutdownWorkers performs 2-phase graceful shutdown and generates a report.
func (e *Engine) shutdownWorkers(cfg *config.Config) {
	e.logger.Info("shutting down workers...")

	// Snapshot all counters BEFORE stopping producers — this captures the clean
	// measurement window without any shutdown-phase errors from in-flight RPCs.
	e.producerStopSnapshot = e.capturePatternSnapshots()
	e.producersStoppedAt = time.Now()
	e.logger.Info("producer-stop snapshot captured")

	// Phase 1: stop ALL producers across ALL channels across ALL patterns
	for pname, pg := range e.patternGroups {
		pg.StopProducers()
		e.patternStatus.Store(pname, "draining")
		e.logger.Info("producers stopped", "pattern", pname, "channels", pg.ChannelCount())
	}

	// Drain period
	drainTimeout := time.Duration(cfg.Shutdown.DrainTimeoutSeconds) * time.Second
	e.logger.Info("waiting for drain", "timeout", drainTimeout)
	time.Sleep(drainTimeout)

	// Phase 2: stop ALL consumers across ALL channels across ALL patterns
	for pname, pg := range e.patternGroups {
		pg.StopConsumers()
		e.patternStatus.Store(pname, StateStopped)
		e.logger.Info("consumers stopped", "pattern", pname)
	}

	e.mu.Lock()
	if e.baselineRSS == 0 {
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		e.baselineRSS = float64(memStats.Sys) / 1024 / 1024
	}
	e.mu.Unlock()

	summary := e.buildSummary(cfg, StateStopped)
	verdict := report.Generate(summary, cfg)
	summary.Verdict = *verdict
	e.mu.Lock()
	e.lastReport = summary
	e.mu.Unlock()

	if cfg.Output.ReportFile != "" {
		if err := report.WriteJSON(summary, cfg.Output.ReportFile); err != nil {
			e.logger.Error("failed to write report file", "error", err)
		} else {
			e.logger.Info("report written", "path", cfg.Output.ReportFile)
		}
	}

	report.PrintConsole(summary)

	if cfg.Shutdown.CleanupChannels {
		e.cleanupRunChannels(cfg)
	}

	if e.client != nil {
		_ = e.client.Close()
	}
}

// finishRunLocked transitions to the final state. Must be called with e.mu held.
func (e *Engine) finishRunLocked(finalState string, errorMsg string) {
	e.state = finalState
	e.runEndedAt = time.Now()
	e.runError = errorMsg
	if finalState == StateError && e.lastReport == nil {
		summary := &report.Summary{
			SDK:           metrics.SDK(),
			Version:       e.sdkVersion(),
			Mode:          e.runCfg.Mode,
			BrokerAddress: e.runCfg.Broker.Address,
			StartedAt:     e.runStartedAt,
			EndedAt:       time.Now(),
			DurationSecs:  time.Since(e.runStartedAt).Seconds(),
			Status:        StateError,
			Patterns:      make(map[string]*report.PatternStats),
			Resources: report.ResourceStats{
				PeakRSSMB:          e.peakRSS,
				BaselineRSSMB:      e.baselineRSS,
				MemoryGrowthFactor: 1.0,
				PeakWorkers:        runtime.NumGoroutine(),
			},
		}
		allEnabled := true
		for _, enabled := range e.runCfg.PatternEnabled {
			if !enabled {
				allEnabled = false
				break
			}
		}
		summary.AllPatternsEnabled = allEnabled
		summary.Verdict = report.Verdict{
			Result:   "FAILED",
			Passed:   false,
			Warnings: []string{},
			Checks: map[string]*report.CheckResult{
				"startup": {
					Passed:    false,
					Threshold: "success",
					Actual:    errorMsg,
				},
			},
		}
		e.lastReport = summary
	}
}

// GracefulShutdown stops any active run and cleans up (for SIGTERM).
func (e *Engine) GracefulShutdown() bool {
	e.mu.Lock()
	state := e.state
	e.mu.Unlock()

	switch state {
	case StateStarting, StateRunning:
		_ = e.StopRun()
		e.WaitForRun()
	case StateStopping:
		e.WaitForRun()
	}

	e.mu.Lock()
	rpt := e.lastReport
	e.mu.Unlock()

	if rpt != nil {
		return rpt.Verdict.Passed
	}
	return true
}

// --- RunController interface methods for Server ---

func (e *Engine) GetInfo() map[string]any {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return map[string]any{
		"sdk":                 metrics.SDK(),
		"sdk_version":         e.sdkVersion(),
		"burnin_version":      "2.0.0",
		"burnin_spec_version": "2",
		"os":                  runtime.GOOS,
		"arch":                runtime.GOARCH,
		"runtime":             runtime.Version(),
		"cpus":                runtime.NumCPU(),
		"memory_total_mb":     int(memStats.Sys / 1024 / 1024),
		"pid":                 os.Getpid(),
		"uptime_seconds":      time.Since(e.bootTime).Seconds(),
		"started_at":          e.bootTime.Format(time.RFC3339),
		"state":               e.State(),
		"broker_address":      e.startupCfg.Broker.Address,
	}
}

func (e *Engine) GetBrokerStatus(ctx context.Context) map[string]any {
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	client, err := kubemq.NewClient(pingCtx,
		kubemq.WithAddress(e.startupCfg.BrokerHost(), e.startupCfg.BrokerPort()),
		kubemq.WithClientId("burnin-ping"),
		kubemq.WithCheckConnection(true),
	)
	if err != nil {
		return map[string]any{
			"connected": false,
			"address":   e.startupCfg.Broker.Address,
			"error":     err.Error(),
		}
	}
	defer client.Close()

	start := time.Now()
	info, err := client.Ping(pingCtx)
	latencyMs := float64(time.Since(start).Microseconds()) / 1000.0

	if err != nil {
		return map[string]any{
			"connected": false,
			"address":   e.startupCfg.Broker.Address,
			"error":     err.Error(),
		}
	}

	return map[string]any{
		"connected":       true,
		"address":         e.startupCfg.Broker.Address,
		"ping_latency_ms": latencyMs,
		"server_version":  info.Version,
		"last_ping_at":    time.Now().Format(time.RFC3339),
	}
}

func (e *Engine) GetRunFull() map[string]any {
	e.mu.Lock()
	state := e.state
	cfg := e.runCfg
	pgs := e.patternGroups
	e.mu.Unlock()

	if state == StateIdle {
		return map[string]any{"run_id": nil, "state": StateIdle}
	}

	result := map[string]any{
		"run_id":         e.RunID(),
		"state":          state,
		"mode":           cfg.Mode,
		"started_at":     e.RunStartedAt(),
		"broker_address": cfg.Broker.Address,
	}

	if state == StateError {
		result["error"] = e.RunError()
		result["elapsed_seconds"] = time.Since(e.runStartedAt).Seconds()
	}

	if state == StateRunning || state == StateStopping || state == StateStopped || state == StateStarting {
		elapsed := time.Since(e.runStartedAt).Seconds()
		result["elapsed_seconds"] = elapsed
		result["duration"] = cfg.DurationStr
		result["warmup_active"] = e.warmupActive.Load()

		if cfg.Duration > 0 {
			remaining := cfg.Duration.Seconds() - elapsed
			if remaining < 0 {
				remaining = 0
			}
			result["remaining_seconds"] = remaining
		} else {
			result["remaining_seconds"] = nil
		}
	}

	if state == StateStopped {
		e.mu.Lock()
		result["ended_at"] = e.runEndedAt.Format(time.RFC3339)
		e.mu.Unlock()
	}

	// Patterns with v2 channel count
	patterns := make(map[string]any)
	if cfg != nil {
		for _, pname := range config.AllPatternNames {
			pg, exists := pgs[pname]
			if !exists {
				patterns[pname] = map[string]any{"enabled": false}
				continue
			}

			patState := StateRunning
			if v, ok := e.patternStatus.Load(pname); ok {
				patState = v.(string)
			}
			ps := e.buildPatternGroupMetrics(cfg, pg, patState)
			ps["enabled"] = true
			ps["channels"] = pg.ChannelCount()
			patterns[pname] = ps
		}
	}
	result["patterns"] = patterns

	// Resources
	e.mu.Lock()
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	rssMB := float64(memStats.Sys) / 1024 / 1024
	baseline := e.baselineRSS
	if baseline == 0 {
		baseline = rssMB
	}
	result["resources"] = map[string]any{
		"rss_mb":               rssMB,
		"baseline_rss_mb":      baseline,
		"memory_growth_factor": rssMB / baseline,
		"active_workers":       runtime.NumGoroutine(),
	}
	e.mu.Unlock()

	return result
}

func (e *Engine) GetRunStatus() map[string]any {
	e.mu.Lock()
	state := e.state
	cfg := e.runCfg
	pgs := e.patternGroups
	e.mu.Unlock()

	if state == StateIdle {
		return map[string]any{"run_id": nil, "state": StateIdle}
	}

	if state == StateError {
		return map[string]any{
			"run_id": e.RunID(),
			"state":  StateError,
			"error":  e.RunError(),
		}
	}

	result := map[string]any{
		"run_id":     e.RunID(),
		"state":      state,
		"started_at": e.RunStartedAt(),
	}

	elapsed := time.Since(e.runStartedAt).Seconds()
	result["elapsed_seconds"] = elapsed
	if cfg != nil && cfg.Duration > 0 {
		remaining := cfg.Duration.Seconds() - elapsed
		if remaining < 0 {
			remaining = 0
		}
		result["remaining_seconds"] = remaining
	} else {
		result["remaining_seconds"] = nil
	}
	result["warmup_active"] = e.warmupActive.Load()

	// Totals aggregated from pattern groups
	var totalSent, totalRecv, totalLost, totalDup, totalCorrupted, totalOOO, totalErrors, totalReconn uint64
	patternStates := make(map[string]any)
	for pname, pg := range pgs {
		totalSent += pg.TotalSent()
		if config.IsRPCPattern(pname) {
			totalRecv += pg.TotalRPCSuccess()
			totalLost += pg.TotalRPCTimeout() + pg.TotalRPCError()
		} else {
			totalRecv += pg.TotalReceived()
			totalLost += pg.TotalLost()
		}
		totalDup += pg.TotalDuplicated()
		totalCorrupted += pg.TotalCorrupted()
		totalOOO += pg.TotalOutOfOrder()
		totalErrors += pg.TotalErrors()
		totalReconn += pg.TotalReconnections()

		patState := StateRunning
		if v, ok := e.patternStatus.Load(pname); ok {
			patState = v.(string)
		}
		patternStates[pname] = map[string]any{
			"state":    patState,
			"channels": pg.ChannelCount(),
		}
	}

	result["totals"] = map[string]any{
		"sent":          totalSent,
		"received":      totalRecv,
		"lost":          totalLost,
		"duplicated":    totalDup,
		"corrupted":     totalCorrupted,
		"out_of_order":  totalOOO,
		"errors":        totalErrors,
		"reconnections": totalReconn,
	}
	result["pattern_states"] = patternStates

	return result
}

func (e *Engine) GetRunConfig() (map[string]any, bool) {
	e.mu.Lock()
	cfg := e.runCfg
	state := e.state
	e.mu.Unlock()

	if cfg == nil {
		return nil, false
	}

	configMap := map[string]any{
		"version":                  "2",
		"mode":                     cfg.Mode,
		"duration":                 cfg.DurationStr,
		"run_id":                   cfg.RunID,
		"warmup_duration":          cfg.WarmupStr,
		"starting_timeout_seconds": cfg.StartingTimeoutSeconds,
		"broker": map[string]any{
			"address":          cfg.Broker.Address,
			"client_id_prefix": cfg.Broker.ClientIDPrefix,
		},
		"queue": map[string]any{
			"poll_max_messages":         cfg.Queue.PollMaxMessages,
			"poll_wait_timeout_seconds": cfg.Queue.PollWaitTimeoutSeconds,
			"auto_ack":                  cfg.Queue.AutoAck,
			"max_depth":                 cfg.Queue.MaxDepth,
		},
		"rpc": map[string]any{
			"timeout_ms": cfg.RPC.TimeoutMS,
		},
		"message": map[string]any{
			"size_mode":         cfg.Message.SizeMode,
			"size_bytes":        cfg.Message.SizeBytes,
			"size_distribution": cfg.Message.SizeDistribution,
			"reorder_window":    cfg.Message.ReorderWindow,
		},
		"thresholds": map[string]any{
			"max_duplication_pct":      cfg.Thresholds.MaxDuplicationPct,
			"max_error_rate_pct":       cfg.Thresholds.MaxErrorRatePct,
			"max_memory_growth_factor": cfg.Thresholds.MaxMemoryGrowth,
			"max_downtime_pct":         cfg.Thresholds.MaxDowntimePct,
			"min_throughput_pct":       cfg.Thresholds.MinThroughputPct,
			"max_duration":             cfg.Thresholds.MaxDurationStr,
		},
		"forced_disconnect": map[string]any{
			"interval": cfg.ForcedDisconnect.IntervalStr,
			"duration": cfg.ForcedDisconnect.DurationStr,
		},
		"warmup": map[string]any{
			"max_parallel_channels":  cfg.Warmup.MaxParallelChannels,
			"timeout_per_channel_ms": cfg.Warmup.TimeoutPerChannelMs,
		},
		"shutdown": map[string]any{
			"drain_timeout_seconds": cfg.Shutdown.DrainTimeoutSeconds,
			"cleanup_channels":      cfg.Shutdown.CleanupChannels,
		},
		"metrics": map[string]any{
			"report_interval": cfg.Metrics.ReportStr,
		},
	}

	// Build v2 patterns block
	patterns := make(map[string]any)
	for _, pname := range config.AllPatternNames {
		pc := cfg.GetPatternConfig(pname)
		if pc == nil {
			patterns[pname] = map[string]any{"enabled": false}
			continue
		}
		pMap := map[string]any{
			"enabled":  pc.Enabled,
			"channels": pc.Channels,
			"rate":     pc.Rate,
		}
		if pc.Enabled {
			if config.IsRPCPattern(pname) {
				pMap["senders_per_channel"] = pc.SendersPerChannel
				pMap["responders_per_channel"] = pc.RespondersPerChannel
			} else {
				pMap["producers_per_channel"] = pc.ProducersPerChannel
				pMap["consumers_per_channel"] = pc.ConsumersPerChannel
				if config.IsPubSubPattern(pname) {
					pMap["consumer_group"] = pc.ConsumerGroup
				}
			}
		}
		patterns[pname] = pMap
	}
	configMap["patterns"] = patterns

	return map[string]any{
		"run_id": cfg.RunID,
		"state":  state,
		"config": configMap,
	}, true
}

func (e *Engine) GetRunReport() (map[string]any, bool) {
	e.mu.Lock()
	rpt := e.lastReport
	e.mu.Unlock()

	if rpt == nil {
		return nil, false
	}

	result := map[string]any{
		"run_id":               rpt.RunID,
		"sdk":                  rpt.SDK,
		"sdk_version":          rpt.Version,
		"mode":                 rpt.Mode,
		"broker_address":       rpt.BrokerAddress,
		"started_at":           rpt.StartedAt.Format(time.RFC3339),
		"ended_at":             rpt.EndedAt.Format(time.RFC3339),
		"duration_seconds":     rpt.DurationSecs,
		"all_patterns_enabled": rpt.AllPatternsEnabled,
		"patterns":             rpt.Patterns,
		"resources": map[string]any{
			"peak_rss_mb":          rpt.Resources.PeakRSSMB,
			"baseline_rss_mb":      rpt.Resources.BaselineRSSMB,
			"memory_growth_factor": rpt.Resources.MemoryGrowthFactor,
			"peak_workers":         rpt.Resources.PeakWorkers,
		},
		"verdict": map[string]any{
			"result":   rpt.Verdict.Result,
			"warnings": rpt.Verdict.Warnings,
			"checks":   rpt.Verdict.Checks,
		},
	}

	return result, true
}

func (e *Engine) CleanupChannels(ctx context.Context) map[string]any {
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(e.startupCfg.BrokerHost(), e.startupCfg.BrokerPort()),
		kubemq.WithClientId("burnin-cleanup"),
		kubemq.WithCheckConnection(true),
	)
	if err != nil {
		return map[string]any{
			"deleted_channels": []string{},
			"failed_channels":  []string{},
			"message":          fmt.Sprintf("Could not connect to broker: %s", err.Error()),
		}
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

	var deleted, failed []string
	for _, ct := range channelTypes {
		channels, err := client.ListChannels(ctx, ct, prefix)
		if err != nil {
			continue
		}
		for _, ch := range channels {
			if err := client.DeleteChannel(ctx, ch.Name, ct); err != nil {
				failed = append(failed, ch.Name)
			} else {
				deleted = append(deleted, ch.Name)
			}
		}
	}

	if deleted == nil {
		deleted = []string{}
	}
	if failed == nil {
		failed = []string{}
	}

	enabledPatterns := 0
	for _, pc := range e.startupCfg.Patterns {
		if pc.Enabled {
			enabledPatterns++
		}
	}

	return map[string]any{
		"deleted_channels": deleted,
		"failed_channels":  failed,
		"message":          fmt.Sprintf("cleaned %d channels across %d patterns", len(deleted), enabledPatterns),
	}
}

// --- Helper methods ---

func (e *Engine) sdkVersion() string {
	v := e.startupCfg.Output.SDKVersion
	if v == "" {
		v = kubemq.Version
	}
	return v
}

func (e *Engine) buildPatternGroupMetrics(cfg *config.Config, pg *PatternGroup, patState string) map[string]any {
	sent := pg.TotalSent()
	recv := pg.TotalReceived()
	lost := pg.TotalLost()
	dup := pg.TotalDuplicated()
	corrupted := pg.TotalCorrupted()
	ooo := pg.TotalOutOfOrder()
	errs := pg.TotalErrors()
	reconn := pg.TotalReconnections()

	lossPct := 0.0
	if sent > 0 {
		lossPct = float64(lost) / float64(sent) * 100.0
	}

	// target_rate = rate * channels (total)
	totalTargetRate := pg.PatternConfig().Rate * pg.ChannelCount()

	// Latency from pattern-level accumulator
	pla := pg.PatternLatencyAccumulator()

	result := map[string]any{
		"state":          patState,
		"sent":           sent,
		"received":       recv,
		"lost":           lost,
		"duplicated":     dup,
		"corrupted":      corrupted,
		"out_of_order":   ooo,
		"errors":         errs,
		"reconnections":  reconn,
		"loss_pct":       lossPct,
		"target_rate":    totalTargetRate,
		"bytes_sent":     pg.TotalBytesSent(),
		"bytes_received": pg.TotalBytesReceived(),
		"latency": map[string]any{
			"p50_ms":  pla.Percentile(50).Seconds() * 1000,
			"p95_ms":  pla.Percentile(95).Seconds() * 1000,
			"p99_ms":  pla.Percentile(99).Seconds() * 1000,
			"p999_ms": pla.Percentile(99.9).Seconds() * 1000,
		},
	}

	isRPC := config.IsRPCPattern(pg.Pattern())
	if isRPC {
		result["responses_success"] = pg.TotalRPCSuccess()
		result["responses_timeout"] = pg.TotalRPCTimeout()
		result["responses_error"] = pg.TotalRPCError()
		delete(result, "received")
		delete(result, "lost")
		delete(result, "duplicated")
		delete(result, "corrupted")
		delete(result, "out_of_order")
		delete(result, "loss_pct")
	}

	if pg.Pattern() == worker.PatternEventsStore {
		result["unconfirmed"] = pg.TotalUnconfirmed()
	}

	return result
}

// capturePatternSnapshots reads all live counters from every PatternGroup
// and returns a map of pattern-name -> PatternSnapshot.
func (e *Engine) capturePatternSnapshots() map[string]*PatternSnapshot {
	snapshots := make(map[string]*PatternSnapshot, len(e.patternGroups))
	for pname, pg := range e.patternGroups {
		pla := pg.PatternLatencyAccumulator()
		snap := &PatternSnapshot{
			Sent:            pg.TotalSent(),
			Received:        pg.TotalReceived(),
			Lost:            pg.TotalLost(),
			Duplicated:      pg.TotalDuplicated(),
			Corrupted:       pg.TotalCorrupted(),
			OutOfOrder:      pg.TotalOutOfOrder(),
			Errors:          pg.TotalErrors(),
			Reconnections:   pg.TotalReconnections(),
			BytesSent:       pg.TotalBytesSent(),
			BytesReceived:   pg.TotalBytesReceived(),
			RpcSuccess:      pg.TotalRPCSuccess(),
			RpcTimeout:      pg.TotalRPCTimeout(),
			RpcError:        pg.TotalRPCError(),
			Unconfirmed:     pg.TotalUnconfirmed(),
			PeakThroughput:  pg.PeakThroughput(),
			DowntimeSeconds: pg.MaxDowntimeSeconds(),
			LatencyP50MS:    pla.Percentile(50).Seconds() * 1000,
			LatencyP95MS:    pla.Percentile(95).Seconds() * 1000,
			LatencyP99MS:    pla.Percentile(99).Seconds() * 1000,
			LatencyP999MS:   pla.Percentile(99.9).Seconds() * 1000,
		}

		// Per-channel snapshots
		for i := 0; i < pg.ChannelCount(); i++ {
			snap.PerChannel = append(snap.PerChannel, ChannelSnapshot{
				Sent:       pg.ChannelSent(i),
				Received:   pg.ChannelReceived(i),
				Lost:       pg.ChannelLost(i),
				Duplicated: pg.ChannelDuplicated(i),
				Corrupted:  pg.ChannelCorrupted(i),
				Errors:     pg.ChannelErrors(i),
			})
		}

		// Per-worker producer/consumer snapshots
		for _, w := range pg.ChannelWorkers() {
			snap.PerWorkerProducers = append(snap.PerWorkerProducers, w.ProducerStatSnapshots())
			snap.PerWorkerConsumers = append(snap.PerWorkerConsumers, w.ConsumerStatSnapshots())
		}

		snapshots[pname] = snap
	}
	return snapshots
}

func (e *Engine) buildSummary(cfg *config.Config, status string) *report.Summary {
	endTime := e.producersStoppedAt
	if endTime.IsZero() {
		endTime = time.Now()
	}
	// Use producersStartedAt for throughput calculation (excludes warmup time)
	startTime := e.producersStartedAt
	if startTime.IsZero() {
		startTime = e.runStartedAt
	}
	elapsed := endTime.Sub(startTime)

	e.mu.Lock()
	baselineRSS := e.baselineRSS
	peakRSS := e.peakRSS
	e.mu.Unlock()

	if baselineRSS == 0 {
		baselineRSS = 1
	}

	summary := &report.Summary{
		RunID:         cfg.RunID,
		SDK:           metrics.SDK(),
		Version:       e.sdkVersion(),
		Mode:          cfg.Mode,
		BrokerAddress: cfg.Broker.Address,
		StartedAt:     e.runStartedAt,
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

	allEnabled := true
	for _, enabled := range cfg.PatternEnabled {
		if !enabled {
			allEnabled = false
			break
		}
	}
	summary.AllPatternsEnabled = allEnabled
	summary.BaselineFullWindow = e.baselineFullWindow

	elapsedSecs := elapsed.Seconds()

	for pname, pg := range e.patternGroups {
		patStatus := StateRunning
		if v, ok := e.patternStatus.Load(pname); ok {
			patStatus = v.(string)
		}

		pc := pg.PatternConfig()
		// target_rate = rate * channels
		totalTargetRate := pc.Rate * pg.ChannelCount()

		// Use snapshot values if available (taken at producer-stop time T2),
		// otherwise fall back to live values (e.g. early error before producers started).
		snap, hasSnap := e.producerStopSnapshot[pname]

		var (
			sent, received, lost, duplicated, corrupted, outOfOrder uint64
			errors, reconnections, bytesSent, bytesReceived         uint64
			rpcSuccess, rpcTimeout, rpcError                        uint64
			unconfirmed                                             int64
			peakThroughput, downtimeSeconds                         float64
			latP50, latP95, latP99, latP999                         float64
		)

		if hasSnap {
			sent = snap.Sent
			received = snap.Received
			lost = snap.Lost
			duplicated = snap.Duplicated
			corrupted = snap.Corrupted
			outOfOrder = snap.OutOfOrder
			errors = snap.Errors
			reconnections = snap.Reconnections
			bytesSent = snap.BytesSent
			bytesReceived = snap.BytesReceived
			rpcSuccess = snap.RpcSuccess
			rpcTimeout = snap.RpcTimeout
			rpcError = snap.RpcError
			unconfirmed = snap.Unconfirmed
			peakThroughput = snap.PeakThroughput
			downtimeSeconds = snap.DowntimeSeconds
			latP50 = snap.LatencyP50MS
			latP95 = snap.LatencyP95MS
			latP99 = snap.LatencyP99MS
			latP999 = snap.LatencyP999MS
		} else {
			pla := pg.PatternLatencyAccumulator()
			sent = pg.TotalSent()
			received = pg.TotalReceived()
			lost = pg.TotalLost()
			duplicated = pg.TotalDuplicated()
			corrupted = pg.TotalCorrupted()
			outOfOrder = pg.TotalOutOfOrder()
			errors = pg.TotalErrors()
			reconnections = pg.TotalReconnections()
			bytesSent = pg.TotalBytesSent()
			bytesReceived = pg.TotalBytesReceived()
			rpcSuccess = pg.TotalRPCSuccess()
			rpcTimeout = pg.TotalRPCTimeout()
			rpcError = pg.TotalRPCError()
			unconfirmed = pg.TotalUnconfirmed()
			peakThroughput = pg.PeakThroughput()
			downtimeSeconds = pg.MaxDowntimeSeconds()
			latP50 = pla.Percentile(50).Seconds() * 1000
			latP95 = pla.Percentile(95).Seconds() * 1000
			latP99 = pla.Percentile(99).Seconds() * 1000
			latP999 = pla.Percentile(99.9).Seconds() * 1000
		}

		ps := &report.PatternStats{
			Enabled:         true,
			Status:          patStatus,
			Sent:            sent,
			Received:        received,
			Lost:            lost,
			Duplicated:      duplicated,
			Corrupted:       corrupted,
			OutOfOrder:      outOfOrder,
			Errors:          errors,
			Reconnections:   reconnections,
			TargetRate:      totalTargetRate,
			DowntimeSeconds: downtimeSeconds,
			BytesSent:       bytesSent,
			BytesReceived:   bytesReceived,
			LatencyP50MS:    latP50,
			LatencyP95MS:    latP95,
			LatencyP99MS:    latP99,
			LatencyP999MS:   latP999,
			Channels:        pg.ChannelCount(),
			PeakThroughput:  peakThroughput,
		}

		if ps.Sent > 0 {
			ps.LossPct = float64(ps.Lost) / float64(ps.Sent) * 100.0
			if elapsedSecs > 0 {
				ps.AvgThroughput = float64(ps.Sent) / elapsedSecs
			}
		}

		isRPC := config.IsRPCPattern(pname)
		if isRPC {
			ps.ResponsesSuccess = rpcSuccess
			ps.ResponsesTimeout = rpcTimeout
			ps.ResponsesError = rpcError
			ps.SendersPerChannel = pc.SendersPerChannel
			ps.RespondersPerChannel = pc.RespondersPerChannel
			if elapsedSecs > 0 {
				ps.AvgThroughputRPC = float64(ps.ResponsesSuccess) / elapsedSecs
			}
			// RPC latency from snapshot or live
			ps.RPCP50MS = latP50
			ps.RPCP95MS = latP95
			ps.RPCP99MS = latP99
			ps.RPCP999MS = latP999
		} else {
			ps.ProducersPerChannel = pc.ProducersPerChannel
			ps.ConsumersPerChannel = pc.ConsumersPerChannel
		}

		// Pub/sub metadata
		if config.IsPubSubPattern(pname) {
			ps.ConsumerGroup = pc.ConsumerGroup
			ps.NumConsumers = pc.ConsumersPerChannel
		}

		if pname == worker.PatternEventsStore {
			ps.Unconfirmed = unconfirmed
		}

		// Populate per-channel stats for fail-on-any-channel verdict checks
		if hasSnap && len(snap.PerChannel) > 0 {
			for i, cs := range snap.PerChannel {
				ps.PerChannel = append(ps.PerChannel, report.ChannelStats{
					Index:      i + 1, // 1-based
					Sent:       cs.Sent,
					Received:   cs.Received,
					Lost:       cs.Lost,
					Duplicated: cs.Duplicated,
					Corrupted:  cs.Corrupted,
					Errors:     cs.Errors,
				})
			}
		} else {
			for i := 0; i < pg.ChannelCount(); i++ {
				ps.PerChannel = append(ps.PerChannel, report.ChannelStats{
					Index:      i + 1, // 1-based
					Sent:       pg.ChannelSent(i),
					Received:   pg.ChannelReceived(i),
					Lost:       pg.ChannelLost(i),
					Duplicated: pg.ChannelDuplicated(i),
					Corrupted:  pg.ChannelCorrupted(i),
					Errors:     pg.ChannelErrors(i),
				})
			}
		}

		// Per-worker producer/consumer stats from snapshot or live
		if hasSnap && len(snap.PerWorkerProducers) > 0 {
			for wIdx := range snap.PerWorkerProducers {
				if isRPC {
					for _, ws := range snap.PerWorkerProducers[wIdx] {
						avgRate := 0.0
						if elapsedSecs > 0 && ws.Sent > 0 {
							avgRate = float64(ws.Sent) / elapsedSecs
						}
						ps.Senders = append(ps.Senders, &report.SenderStat{
							ID: ws.ID, Sent: ws.Sent,
							ResponsesSuccess: ws.RPCSuccess, ResponsesTimeout: ws.RPCTimeout,
							ResponsesError: ws.RPCError, Errors: ws.Errors, AvgRate: avgRate,
							LatencyP50MS: ws.LatencyP50MS, LatencyP95MS: ws.LatencyP95MS,
							LatencyP99MS: ws.LatencyP99MS, LatencyP999MS: ws.LatencyP999MS,
						})
					}
					for _, ws := range snap.PerWorkerConsumers[wIdx] {
						ps.Responders = append(ps.Responders, &report.ResponderStat{
							ID: ws.ID, Responded: ws.Responded, Errors: ws.Errors,
						})
					}
				} else {
					for _, ws := range snap.PerWorkerProducers[wIdx] {
						avgRate := 0.0
						if elapsedSecs > 0 && ws.Sent > 0 {
							avgRate = float64(ws.Sent) / elapsedSecs
						}
						ps.Producers = append(ps.Producers, &report.ProducerStat{
							ID: ws.ID, Sent: ws.Sent, Errors: ws.Errors,
							AvgRate: avgRate, BytesSent: ws.BytesSent,
							LatencyP50MS: ws.LatencyP50MS, LatencyP95MS: ws.LatencyP95MS,
							LatencyP99MS: ws.LatencyP99MS, LatencyP999MS: ws.LatencyP999MS,
						})
					}
					for _, ws := range snap.PerWorkerConsumers[wIdx] {
						ps.Consumers = append(ps.Consumers, &report.ConsumerStat{
							ID: ws.ID, Received: ws.Received, Errors: ws.Errors,
							Duplicated: ws.Duplicated, Corrupted: ws.Corrupted,
							BytesReceived: ws.BytesReceived,
							LatencyP50MS:  ws.LatencyP50MS, LatencyP95MS: ws.LatencyP95MS,
							LatencyP99MS: ws.LatencyP99MS, LatencyP999MS: ws.LatencyP999MS,
						})
					}
				}
			}
		} else {
			for _, w := range pg.ChannelWorkers() {
				if isRPC {
					for _, ws := range w.ProducerStatSnapshots() {
						avgRate := 0.0
						if elapsedSecs > 0 && ws.Sent > 0 {
							avgRate = float64(ws.Sent) / elapsedSecs
						}
						ps.Senders = append(ps.Senders, &report.SenderStat{
							ID: ws.ID, Sent: ws.Sent,
							ResponsesSuccess: ws.RPCSuccess, ResponsesTimeout: ws.RPCTimeout,
							ResponsesError: ws.RPCError, Errors: ws.Errors, AvgRate: avgRate,
							LatencyP50MS: ws.LatencyP50MS, LatencyP95MS: ws.LatencyP95MS,
							LatencyP99MS: ws.LatencyP99MS, LatencyP999MS: ws.LatencyP999MS,
						})
					}
					for _, ws := range w.ConsumerStatSnapshots() {
						ps.Responders = append(ps.Responders, &report.ResponderStat{
							ID: ws.ID, Responded: ws.Responded, Errors: ws.Errors,
						})
					}
				} else {
					for _, ws := range w.ProducerStatSnapshots() {
						avgRate := 0.0
						if elapsedSecs > 0 && ws.Sent > 0 {
							avgRate = float64(ws.Sent) / elapsedSecs
						}
						ps.Producers = append(ps.Producers, &report.ProducerStat{
							ID: ws.ID, Sent: ws.Sent, Errors: ws.Errors,
							AvgRate: avgRate, BytesSent: ws.BytesSent,
							LatencyP50MS: ws.LatencyP50MS, LatencyP95MS: ws.LatencyP95MS,
							LatencyP99MS: ws.LatencyP99MS, LatencyP999MS: ws.LatencyP999MS,
						})
					}
					for _, ws := range w.ConsumerStatSnapshots() {
						ps.Consumers = append(ps.Consumers, &report.ConsumerStat{
							ID: ws.ID, Received: ws.Received, Errors: ws.Errors,
							Duplicated: ws.Duplicated, Corrupted: ws.Corrupted,
							BytesReceived: ws.BytesReceived,
							LatencyP50MS:  ws.LatencyP50MS, LatencyP95MS: ws.LatencyP95MS,
							LatencyP99MS: ws.LatencyP99MS, LatencyP999MS: ws.LatencyP999MS,
						})
					}
				}
			}
		}

		summary.Patterns[pname] = ps
	}

	return summary
}

func (e *Engine) createClient(ctx context.Context, cfg *config.Config) (*kubemq.Client, error) {
	// Calculate concurrent callbacks needed: sum of all responder/consumer
	// goroutines across all patterns. Default to a generous minimum so
	// responder callbacks are never serialised on a single slot.
	maxCallbacks := 64 // generous default
	for _, pc := range cfg.Patterns {
		if pc == nil || !pc.Enabled {
			continue
		}
		maxCallbacks += pc.Channels * max(pc.RespondersPerChannel, pc.ConsumersPerChannel)
	}

	return kubemq.NewClient(ctx,
		kubemq.WithAddress(cfg.BrokerHost(), cfg.BrokerPort()),
		kubemq.WithClientId(fmt.Sprintf("%s-%s", cfg.Broker.ClientIDPrefix, cfg.RunID)),
		kubemq.WithReconnectPolicy(kubemq.ReconnectPolicy{
			MaxAttempts:  0,
			InitialDelay: cfg.Recovery.ReconnectInterval,
			MaxDelay:     cfg.Recovery.ReconnectMaxInterval,
			Multiplier:   cfg.Recovery.ReconnectMultiplier,
		}),
		kubemq.WithCheckConnection(true),
		kubemq.WithMaxConcurrentCallbacks(maxCallbacks),
	)
}

// --- Warmup v2: parallel warmup across all channels ---

func (e *Engine) runWarmupV2(ctx context.Context, cfg *config.Config) error {
	e.logger.Info("running warmup verification across all channels...")

	maxParallel := cfg.Warmup.MaxParallelChannels
	if maxParallel <= 0 {
		maxParallel = 10
	}
	timeoutPerChannel := time.Duration(cfg.Warmup.TimeoutPerChannelMs) * time.Millisecond
	if timeoutPerChannel <= 0 {
		timeoutPerChannel = 5 * time.Second
	}

	e.mu.Lock()
	client := e.cp.Get()
	e.mu.Unlock()

	sem := make(chan struct{}, maxParallel)
	var mu sync.Mutex
	var firstErr error

	var wg sync.WaitGroup

	for _, pg := range e.patternGroups {
		for _, w := range pg.ChannelWorkers() {
			mu.Lock()
			if firstErr != nil {
				mu.Unlock()
				break
			}
			mu.Unlock()

			wg.Add(1)
			go func(wk worker.Worker, pattern string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				mu.Lock()
				if firstErr != nil {
					mu.Unlock()
					return
				}
				mu.Unlock()

				var err error
				for retry := 0; retry < 3; retry++ {
					channelCtx, cancel := context.WithTimeout(ctx, timeoutPerChannel)
					err = e.warmupSingleChannel(channelCtx, client, wk, pattern)
					cancel()
					if err == nil {
						break
					}
					e.logger.Warn("warmup retry", "channel", wk.ChannelName(), "attempt", retry+1, "error", err)
				}
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("warmup failed for channel %s after 3 retries: %w", wk.ChannelName(), err)
					}
					mu.Unlock()
				}
			}(w, pg.Pattern())
		}
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	// Reset all channel workers after warmup
	for _, pg := range e.patternGroups {
		pg.ResetAfterWarmup()
	}

	e.logger.Info("warmup verification passed for all channels", "total_channels", cfg.TotalChannelCount())
	return nil
}

func (e *Engine) warmupSingleChannel(ctx context.Context, client *kubemq.Client, w worker.Worker, pattern string) error {
	channelName := w.ChannelName()

	switch pattern {
	case worker.PatternEvents:
		return e.warmupPubSub(ctx, client, channelName, 3, false)
	case worker.PatternEventsStore:
		return e.warmupPubSub(ctx, client, channelName, 3, true)
	case worker.PatternQueueStream, worker.PatternQueueSimple:
		// Skip warmup sends for queue patterns — the broker auto-creates the channel
		// on first real send/poll. Sending warmup messages to queues leaves unacked
		// messages that cause false duplicates when the real run starts.
		return nil
	case worker.PatternCommands:
		return e.warmupCommand(ctx, client, channelName)
	case worker.PatternQueries:
		return e.warmupQuery(ctx, client, channelName)
	}
	return nil
}

func (e *Engine) warmupPubSub(ctx context.Context, client *kubemq.Client, channelName string, count int, isStore bool) error {
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
			kubemq.WithOnError(func(err error) {}),
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
			kubemq.WithOnError(func(err error) {}),
		)
		if err != nil {
			return fmt.Errorf("warmup subscribe events: %w", err)
		}
		defer sub.Cancel()
	}

	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	if isStore {
		if err := SendWarmupEventStoreMessages(ctx, client, channelName, count); err != nil {
			return err
		}
	} else {
		if err := SendWarmupMessages(ctx, client, channelName, count); err != nil {
			return err
		}
	}

	select {
	case <-received:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("warmup: timed out waiting for messages on %s", channelName)
	}
}

func (e *Engine) warmupQueue(ctx context.Context, client *kubemq.Client, channelName string, count int) error {
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
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (e *Engine) warmupCommand(ctx context.Context, client *kubemq.Client, channelName string) error {
	body, crcHex := payload.Encode(metrics.SDK(), "warmup", "warmup", 1, 256)
	_, err := client.SendCommand(ctx, &kubemq.Command{
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

func (e *Engine) warmupQuery(ctx context.Context, client *kubemq.Client, channelName string) error {
	body, crcHex := payload.Encode(metrics.SDK(), "warmup", "warmup", 1, 256)
	_, err := client.SendQuery(ctx, &kubemq.Query{
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

// --- Channel management ---

func (e *Engine) cleanStaleChannels(ctx context.Context) {
	e.logger.Info("cleaning stale channels from all previous burn-in runs")
	e.mu.Lock()
	client := e.cp.Get()
	e.mu.Unlock()
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

	count := 0
	for _, ct := range channelTypes {
		channels, err := client.ListChannels(cleanCtx, ct, prefix)
		if err != nil {
			e.logger.Warn("failed to list stale channels", "type", ct, "error", err)
			continue
		}
		for _, ch := range channels {
			if delErr := client.DeleteChannel(cleanCtx, ch.Name, ct); delErr != nil {
				e.logger.Warn("failed to delete stale channel", "name", ch.Name, "error", delErr)
			} else {
				count++
			}
		}
	}
	if count > 0 {
		e.logger.Info("cleaned stale channels from prior runs", "count", count)
	}
}

func (e *Engine) cleanupRunChannels(cfg *config.Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	e.mu.Lock()
	client := e.cp.Get()
	e.mu.Unlock()
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

	count := 0
	for pname, pg := range e.patternGroups {
		ct, ok := channelTypes[pname]
		if !ok {
			continue
		}
		for _, w := range pg.ChannelWorkers() {
			if err := client.DeleteChannel(ctx, w.ChannelName(), ct); err != nil {
				e.logger.Warn("failed to delete channel", "channel", w.ChannelName(), "error", err)
			} else {
				count++
			}
		}
	}
	e.logger.Info("cleaned channels for run", "run_id", cfg.RunID, "count", count)
}

// --- Periodic tasks ---

func (e *Engine) periodicReporter(ctx context.Context, cfg *config.Config) {
	ticker := time.NewTicker(cfg.Metrics.ReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.logStatus(cfg)
			e.updateGapDetection()
			e.updateLagMetrics(cfg)
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
			for _, pg := range e.patternGroups {
				for _, w := range pg.ChannelWorkers() {
					w.PeakRate().Advance()
					w.AdvanceRateWindows()
				}
			}
		}
	}
}

func (e *Engine) logStatus(cfg *config.Config) {
	elapsed := time.Since(e.runStartedAt)
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	rssMB := float64(memStats.Sys) / 1024 / 1024

	now := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("[%s] BURN-IN STATUS | uptime=%s mode=%s rss=%.0fMB\n",
		now,
		formatDuration(elapsed),
		cfg.Mode,
		rssMB,
	)

	for pname, pg := range e.patternGroups {
		sent := pg.TotalSent()
		recv := pg.TotalReceived()
		lost := pg.TotalLost()
		dup := pg.TotalDuplicated()
		errCount := pg.TotalErrors()
		p99 := pg.PatternLatencyAccumulator().Percentile(99).Seconds() * 1000
		channels := pg.ChannelCount()

		rateStr := "0"
		if secs := elapsed.Seconds(); secs > 0 {
			rateStr = fmt.Sprintf("%.0f", float64(sent)/secs)
		}

		if config.IsRPCPattern(pname) {
			fmt.Printf("  %-14s (%dch) sent=%-8d resp=%-8d tout=%-4d err=%-4d p99=%.1fms  rate=%s/s\n",
				pname+":",
				channels,
				sent, pg.TotalRPCSuccess(), pg.TotalRPCTimeout(), errCount, p99, rateStr)
		} else {
			fmt.Printf("  %-14s (%dch) sent=%-8d recv=%-8d lost=%-4d dup=%-4d err=%-4d p99=%.1fms  rate=%s/s\n",
				pname+":",
				channels,
				sent, recv, lost, dup, errCount, p99, rateStr,
			)
		}
	}
}

func (e *Engine) updateGapDetection() {
	for _, pg := range e.patternGroups {
		for _, w := range pg.ChannelWorkers() {
			gaps := w.Tracker().DetectGaps()
			for _, lost := range gaps {
				if lost > 0 {
					metrics.AddLost(w.Pattern(), float64(lost))
				}
			}
		}
	}
}

func (e *Engine) updateLagMetrics(cfg *config.Config) {
	for pname, pg := range e.patternGroups {
		sent := pg.TotalSent()
		recv := pg.TotalReceived()
		lag := float64(0)
		if sent > recv {
			lag = float64(sent - recv)
		}
		metrics.ConsumerLag.WithLabelValues(metrics.SDK(), pname).Set(lag)
		elapsed := time.Since(e.runStartedAt).Seconds()
		if elapsed > 0 {
			rate := float64(sent) / elapsed
			metrics.ActualRate.WithLabelValues(metrics.SDK(), pname).Set(rate)
		}
	}
}

func (e *Engine) updateDowntimeMetrics() {
	for pname, pg := range e.patternGroups {
		current := pg.MaxDowntimeSeconds()
		last, ok := e.lastDowntime[pname]
		if !ok {
			last = 0
		}
		delta := current - last
		if delta > 0 {
			metrics.AddDowntime(pname, delta)
		}
		e.lastDowntime[pname] = current
	}
}

func (e *Engine) updateActiveConnections() {
	for pname := range e.patternGroups {
		metrics.SetActiveConnections(pname, 1.0)
		metrics.SetConsumerGroupBalance(pname, 1.0)
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
			metrics.UptimeSeconds.Set(time.Since(e.bootTime).Seconds())
			metrics.ActiveWorkers.Set(float64(runtime.NumGoroutine()))
		}
	}
}

func (e *Engine) memoryTracker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	captured1min := false
	captured5min := false

	var initMem runtime.MemStats
	runtime.ReadMemStats(&initMem)
	e.mu.Lock()
	e.baselineRSS = float64(initMem.Sys) / 1024 / 1024
	e.peakRSS = e.baselineRSS
	e.baselineFullWindow = false
	e.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			rssMB := float64(memStats.Sys) / 1024 / 1024

			e.mu.Lock()
			if rssMB > e.peakRSS {
				e.peakRSS = rssMB
			}
			elapsed := time.Since(e.runStartedAt)
			if !captured1min && elapsed >= 1*time.Minute {
				e.baselineRSS = rssMB
				captured1min = true
			}
			if !captured5min && elapsed >= 5*time.Minute {
				e.baselineRSS = rssMB
				captured5min = true
				e.baselineFullWindow = true
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
			for _, pg := range e.patternGroups {
				for _, w := range pg.ChannelWorkers() {
					if bw, ok := w.(interface {
						TSStore() *metrics.SendTimestampStore
					}); ok {
						bw.TSStore().Purge(60 * time.Second)
					}
				}
			}
		}
	}
}

// Close implements disconnect.ClientRecreator.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for pname := range e.patternGroups {
		metrics.SetActiveConnections(pname, 0.0)
		e.patternStatus.Store(pname, "recovering")
	}

	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

// Recreate implements disconnect.ClientRecreator.
func (e *Engine) Recreate(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	cfg := e.runCfg
	if cfg == nil {
		return fmt.Errorf("no run config available")
	}

	client, err := e.createClient(ctx, cfg)
	if err != nil {
		return err
	}
	e.client = client
	e.cp.Set(client)

	for pname := range e.patternGroups {
		metrics.SetActiveConnections(pname, 1.0)
		e.patternStatus.Store(pname, StateRunning)
	}

	e.logger.Info("client recreated after forced disconnect")
	return nil
}

// CleanupOnly deletes all channels matching the burn-in prefix (CLI mode).
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
		time.Sleep(100 * time.Millisecond)
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
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

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
