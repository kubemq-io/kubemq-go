package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
	"github.com/kubemq-io/kubemq-go/v2/burnin/config"
	"github.com/kubemq-io/kubemq-go/v2/burnin/metrics"
	"github.com/kubemq-io/kubemq-go/v2/burnin/payload"
	"github.com/kubemq-io/kubemq-go/v2/burnin/tracker"
	"golang.org/x/time/rate"
)

// Pattern constants
const (
	PatternEvents      = "events"
	PatternEventsStore = "events_store"
	PatternQueueStream = "queue_stream"
	PatternQueueSimple = "queue_simple"
	PatternCommands    = "commands"
	PatternQueries     = "queries"
)

// AllPatterns returns all pattern names.
func AllPatterns() []string {
	return []string{PatternEvents, PatternEventsStore, PatternQueueStream, PatternQueueSimple, PatternCommands, PatternQueries}
}

// Worker is the interface all pattern workers implement.
type Worker interface {
	Pattern() string
	ChannelType() string
	Start(ctx context.Context) error
	Stop()
	StopProducers()
	StopConsumers()
	ConsumerReady() <-chan struct{}
	Tracker() *tracker.Tracker
	LatencyAccumulator() *metrics.LatencyAccumulator
	RPCLatencyAccumulator() *metrics.LatencyAccumulator
	PeakRate() *metrics.PeakRateTracker
	SentCount() uint64
	ReceivedCount() uint64
	ErrorCount() uint64
	ReconnectionCount() uint64
	CorruptedCount() uint64
	RPCSuccess() uint64
	RPCTimeout() uint64
	RPCError() uint64
	DowntimeSeconds() float64
}

// ClientProvider provides access to the shared SDK client behind a RWMutex.
type ClientProvider struct {
	mu     sync.RWMutex
	client *kubemq.Client
}

func NewClientProvider(c *kubemq.Client) *ClientProvider {
	return &ClientProvider{client: c}
}

func (cp *ClientProvider) Get() *kubemq.Client {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	return cp.client
}

func (cp *ClientProvider) Set(c *kubemq.Client) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.client = c
}

// BaseWorker provides common functionality for all workers.
type BaseWorker struct {
	pattern        string
	cfg            *config.Config
	cp             *ClientProvider
	logger         *slog.Logger
	trk            *tracker.Tracker
	latAccum       *metrics.LatencyAccumulator
	tsStore        *metrics.SendTimestampStore
	consumerReady  chan struct{}
	sent           atomic.Uint64
	received       atomic.Uint64
	corrupted      atomic.Uint64
	errors         atomic.Uint64
	reconnections  atomic.Uint64
	limiter        *rate.Limiter
	sizeDist       *payload.SizeDistribution
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	channelName    string
	producerCtx    context.Context
	producerCancel context.CancelFunc
	consumerCtx    context.Context
	consumerCancel context.CancelFunc
	downtimeStart  time.Time
	downtimeMu     sync.Mutex
	downtimeTotal        time.Duration
	channelType          string
	recentlyReconnected  atomic.Bool
	reconnDupCooldown    atomic.Uint64 // counts non-dup messages since last reconnect
	rpcSuccess           atomic.Uint64
	rpcTimeout           atomic.Uint64
	rpcError             atomic.Uint64
	rpcLatAccum          *metrics.LatencyAccumulator
	peakRate             *metrics.PeakRateTracker
}

func NewBaseWorker(pattern string, cfg *config.Config, cp *ClientProvider, logger *slog.Logger) *BaseWorker {
	bw := &BaseWorker{
		pattern:       pattern,
		cfg:           cfg,
		cp:            cp,
		logger:        logger.With("pattern", pattern),
		trk:           tracker.New(cfg.Message.ReorderWindow),
		latAccum:      metrics.NewLatencyAccumulator(),
		tsStore:       metrics.NewSendTimestampStore(),
		consumerReady: make(chan struct{}),
		channelName:   fmt.Sprintf("go_burnin_%s_%s_001", cfg.RunID, pattern),
		rpcLatAccum:   metrics.NewLatencyAccumulator(),
		peakRate:      metrics.NewPeakRateTracker(),
	}

	// Setup rate limiter
	targetRate := bw.getTargetRate()
	if cfg.Mode == "benchmark" || targetRate == 0 {
		bw.limiter = rate.NewLimiter(rate.Inf, 0)
	} else {
		bw.limiter = rate.NewLimiter(rate.Limit(targetRate), targetRate)
	}

	// Setup size distribution
	if cfg.Message.SizeMode == "distribution" {
		var err error
		bw.sizeDist, err = payload.ParseDistribution(cfg.Message.SizeDistribution)
		if err != nil {
			logger.Error("failed to parse size distribution, falling back to fixed", "error", err)
		}
	}

	return bw
}

func (bw *BaseWorker) Pattern() string                            { return bw.pattern }
func (bw *BaseWorker) ChannelType() string                        { return bw.channelType }
func (bw *BaseWorker) Tracker() *tracker.Tracker                  { return bw.trk }
func (bw *BaseWorker) LatencyAccumulator() *metrics.LatencyAccumulator { return bw.latAccum }
func (bw *BaseWorker) ConsumerReady() <-chan struct{}              { return bw.consumerReady }
func (bw *BaseWorker) SentCount() uint64                          { return bw.sent.Load() }
func (bw *BaseWorker) ReceivedCount() uint64                      { return bw.received.Load() }
func (bw *BaseWorker) ErrorCount() uint64                         { return bw.errors.Load() }
func (bw *BaseWorker) ReconnectionCount() uint64                  { return bw.reconnections.Load() }
func (bw *BaseWorker) CorruptedCount() uint64                     { return bw.corrupted.Load() }
func (bw *BaseWorker) IncError()        { bw.errors.Add(1) }
func (bw *BaseWorker) IncReconnection() { bw.reconnections.Add(1); bw.recentlyReconnected.Store(true); bw.reconnDupCooldown.Store(0) }
func (bw *BaseWorker) RPCLatencyAccumulator() *metrics.LatencyAccumulator { return bw.rpcLatAccum }
func (bw *BaseWorker) PeakRate() *metrics.PeakRateTracker                { return bw.peakRate }
func (bw *BaseWorker) RPCSuccess() uint64                                { return bw.rpcSuccess.Load() }
func (bw *BaseWorker) RPCTimeout() uint64                                { return bw.rpcTimeout.Load() }
func (bw *BaseWorker) RPCError() uint64                                  { return bw.rpcError.Load() }
func (bw *BaseWorker) IncRPCSuccess()                                    { bw.rpcSuccess.Add(1) }
func (bw *BaseWorker) IncRPCTimeout()                                    { bw.rpcTimeout.Add(1) }
func (bw *BaseWorker) IncRPCError()                                      { bw.rpcError.Add(1) }

// RecordError increments both the Prometheus counter and the in-process atomic.
func (bw *BaseWorker) RecordError(errorType string) {
	metrics.IncError(bw.pattern, errorType)
	bw.errors.Add(1)
}
func (bw *BaseWorker) ChannelName() string                        { return bw.channelName }
func (bw *BaseWorker) Client() *kubemq.Client                     { return bw.cp.Get() }
func (bw *BaseWorker) Logger() *slog.Logger                       { return bw.logger }
func (bw *BaseWorker) Config() *config.Config                     { return bw.cfg }
func (bw *BaseWorker) TSStore() *metrics.SendTimestampStore        { return bw.tsStore }

func (bw *BaseWorker) DowntimeSeconds() float64 {
	bw.downtimeMu.Lock()
	defer bw.downtimeMu.Unlock()
	return bw.downtimeTotal.Seconds()
}

func (bw *BaseWorker) StartDowntime() {
	bw.downtimeMu.Lock()
	defer bw.downtimeMu.Unlock()
	if bw.downtimeStart.IsZero() {
		bw.downtimeStart = time.Now()
	}
}

func (bw *BaseWorker) StopDowntime() {
	bw.downtimeMu.Lock()
	defer bw.downtimeMu.Unlock()
	if !bw.downtimeStart.IsZero() {
		bw.downtimeTotal += time.Since(bw.downtimeStart)
		bw.downtimeStart = time.Time{}
	}
}

func (bw *BaseWorker) StopProducers() {
	if bw.producerCancel != nil {
		bw.producerCancel()
	}
}

func (bw *BaseWorker) StopConsumers() {
	if bw.cancel != nil {
		bw.cancel()
	}
}

// BackpressureCheck returns true if producers should pause because the consumer
// lag (sent - received) exceeds the configured MaxDepth threshold.
func (bw *BaseWorker) BackpressureCheck() bool {
	if bw.cfg.Queue.MaxDepth <= 0 {
		return false
	}
	sent := bw.sent.Load()
	recv := bw.received.Load()
	if sent <= recv {
		return false
	}
	return int(sent-recv) > bw.cfg.Queue.MaxDepth
}

// Stop cancels all goroutines (producers + consumers) and waits.
func (bw *BaseWorker) Stop() {
	bw.StopProducers()
	bw.StopConsumers()
	bw.wg.Wait()
}

func (bw *BaseWorker) getTargetRate() int {
	switch bw.pattern {
	case PatternEvents:
		return bw.cfg.Rates.Events
	case PatternEventsStore:
		return bw.cfg.Rates.EventsStore
	case PatternQueueStream:
		return bw.cfg.Rates.QueueStream
	case PatternQueueSimple:
		return bw.cfg.Rates.QueueSimple
	case PatternCommands:
		return bw.cfg.Rates.Commands
	case PatternQueries:
		return bw.cfg.Rates.Queries
	}
	return 100
}

func (bw *BaseWorker) GetTargetRate() int {
	return bw.getTargetRate()
}

func (bw *BaseWorker) WaitForRate(ctx context.Context) error {
	return bw.limiter.Wait(ctx)
}

func (bw *BaseWorker) MessageSize() int {
	if bw.sizeDist != nil {
		return bw.sizeDist.SelectSize()
	}
	return bw.cfg.Message.SizeBytes
}

func (bw *BaseWorker) MarkConsumerReady() {
	select {
	case <-bw.consumerReady:
		// Already closed
	default:
		close(bw.consumerReady)
	}
}

func (bw *BaseWorker) RecordSend(producerID string, seq uint64) {
	bw.sent.Add(1)
	metrics.IncSent(bw.pattern, producerID)
	bw.tsStore.Store(producerID, seq, time.Now())
	bw.peakRate.Record()
}

func (bw *BaseWorker) RecordReceive(consumerID string, body []byte, crcTag string, producerID string, seq uint64) {
	// Skip warmup messages — they are not part of the actual test
	if producerID == "warmup" {
		return
	}
	bw.received.Add(1)
	metrics.IncReceived(bw.pattern, consumerID)
	metrics.AddBytesReceived(bw.pattern, float64(len(body)))

	// CRC check
	if !payload.VerifyCRC(body, crcTag) {
		bw.corrupted.Add(1)
		metrics.IncCorrupted(bw.pattern)
		bw.logger.Error("CRC mismatch", "producer", producerID, "seq", seq)
		return
	}

	// Sequence tracking
	isDup, isOOO := bw.trk.Record(producerID, seq)
	if isDup {
		metrics.IncDuplicated(bw.pattern)
		if bw.recentlyReconnected.Load() {
			metrics.IncReconnectionDuplicate(bw.pattern)
		}
	} else {
		// Cool down reconnection flag after 100 non-duplicate messages
		if bw.recentlyReconnected.Load() {
			if bw.reconnDupCooldown.Add(1) >= 100 {
				bw.recentlyReconnected.Store(false)
			}
		}
	}
	if isOOO {
		metrics.IncOutOfOrder(bw.pattern)
	}

	// Latency
	if sendTime, ok := bw.tsStore.LoadAndDelete(producerID, seq); ok {
		latency := time.Since(sendTime)
		metrics.ObserveLatency(bw.pattern, latency)
		bw.latAccum.Record(latency)
	}
}

// CreateChannel creates the channel for this worker.
func (bw *BaseWorker) CreateChannel(ctx context.Context, channelType string) error {
	return bw.cp.Get().CreateChannel(ctx, bw.channelName, channelType)
}

// DeleteChannel deletes the channel for this worker.
func (bw *BaseWorker) DeleteChannel(ctx context.Context, channelType string) error {
	return bw.cp.Get().DeleteChannel(ctx, bw.channelName, channelType)
}

// ResetAfterWarmup clears metrics accumulators.
func (bw *BaseWorker) ResetAfterWarmup() {
	bw.trk.Reset()
	bw.latAccum.Reset()
	bw.sent.Store(0)
	bw.received.Store(0)
	bw.corrupted.Store(0)
	bw.errors.Store(0)
	bw.reconnections.Store(0)
	bw.rpcSuccess.Store(0)
	bw.rpcTimeout.Store(0)
	bw.rpcError.Store(0)
	bw.rpcLatAccum.Reset()
}
