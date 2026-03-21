package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
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

// WorkerStat tracks per-producer or per-consumer metrics.
type WorkerStat struct {
	id         string
	sent       atomic.Uint64
	received   atomic.Uint64
	errors     atomic.Uint64
	duplicated atomic.Uint64
	corrupted  atomic.Uint64
	bytesSent  atomic.Uint64
	bytesRecv  atomic.Uint64
	rpcSuccess atomic.Uint64
	rpcTimeout atomic.Uint64
	rpcError   atomic.Uint64
	responded  atomic.Uint64
	latAccum   *metrics.LatencyAccumulator
	rateWindow *metrics.SlidingRateWindow
}

// WorkerStatSnapshot is a point-in-time snapshot of a WorkerStat.
type WorkerStatSnapshot struct {
	ID            string
	Sent          uint64
	Received      uint64
	Errors        uint64
	Duplicated    uint64
	Corrupted     uint64
	ActualRate    float64
	BytesSent     uint64
	BytesReceived uint64
	RPCSuccess    uint64
	RPCTimeout    uint64
	RPCError      uint64
	Responded     uint64
	LatencyP50MS  float64
	LatencyP95MS  float64
	LatencyP99MS  float64
	LatencyP999MS float64
}

// Worker is the interface all pattern workers implement.
type Worker interface {
	Pattern() string
	ChannelType() string
	ChannelName() string
	ChannelIndex() int
	Start(ctx context.Context) error
	StartProducers()
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
	BytesSentCount() uint64
	BytesReceivedCount() uint64
	UnconfirmedCount() int64
	RateWindow() *metrics.SlidingRateWindow
	AdvanceRateWindows()
	ProducerStatSnapshots() []WorkerStatSnapshot
	ConsumerStatSnapshots() []WorkerStatSnapshot
	ResetAfterWarmup()
	PatternLatencyAccumulator() *metrics.LatencyAccumulator
	DuplicatedCount() uint64
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
	pattern             string
	cfg                 *config.Config
	cp                  *ClientProvider
	logger              *slog.Logger
	trk                 *tracker.Tracker
	latAccum            *metrics.LatencyAccumulator
	patternLatAccum     *metrics.LatencyAccumulator // shared pattern-level accumulator
	tsStore             *metrics.SendTimestampStore
	consumerReady       chan struct{}
	sent                atomic.Uint64
	received            atomic.Uint64
	corrupted           atomic.Uint64
	errors              atomic.Uint64
	reconnections       atomic.Uint64
	limiter             *rate.Limiter
	sizeDist            *payload.SizeDistribution
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	channelName         string
	channelIdx          int // 1-based channel index
	targetRate          int // per-channel target rate
	producerCtx         context.Context
	producerCancel      context.CancelFunc
	consumerCtx         context.Context
	consumerCancel      context.CancelFunc
	downtimeStart       time.Time
	downtimeMu          sync.Mutex
	downtimeTotal       time.Duration
	channelType         string
	recentlyReconnected atomic.Bool
	reconnDupCooldown   atomic.Uint64 // counts non-dup messages since last reconnect
	rpcSuccess          atomic.Uint64
	rpcTimeout          atomic.Uint64
	rpcError            atomic.Uint64
	rpcLatAccum         *metrics.LatencyAccumulator
	peakRate            *metrics.PeakRateTracker
	bytesSent           atomic.Uint64
	bytesReceived       atomic.Uint64
	unconfirmed         atomic.Int64
	rateWindow          *metrics.SlidingRateWindow
	producerStats       sync.Map // id -> *WorkerStat
	consumerStats       sync.Map // id -> *WorkerStat
}

// NewBaseWorker creates a BaseWorker with explicit channel name, rate, channel index, and pattern-level accumulator.
func NewBaseWorker(pattern string, cfg *config.Config, cp *ClientProvider, logger *slog.Logger,
	channelName string, channelIndex int, targetRate int, patternLatAccum *metrics.LatencyAccumulator) *BaseWorker {

	bw := &BaseWorker{
		pattern:         pattern,
		cfg:             cfg,
		cp:              cp,
		logger:          logger.With("pattern", pattern, "channel", channelName),
		trk:             tracker.New(cfg.Message.ReorderWindow),
		latAccum:        metrics.NewLatencyAccumulator(),
		patternLatAccum: patternLatAccum,
		tsStore:         metrics.NewSendTimestampStore(),
		consumerReady:   make(chan struct{}),
		channelName:     channelName,
		channelIdx:      channelIndex,
		targetRate:      targetRate,
		rpcLatAccum:     metrics.NewLatencyAccumulator(),
		peakRate:        metrics.NewPeakRateTracker(),
		rateWindow:      metrics.NewSlidingRateWindow(),
	}

	// Setup rate limiter
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

func (bw *BaseWorker) Pattern() string                                 { return bw.pattern }
func (bw *BaseWorker) ChannelType() string                             { return bw.channelType }
func (bw *BaseWorker) ChannelName() string                             { return bw.channelName }
func (bw *BaseWorker) ChannelIndex() int                               { return bw.channelIdx }
func (bw *BaseWorker) Tracker() *tracker.Tracker                       { return bw.trk }
func (bw *BaseWorker) LatencyAccumulator() *metrics.LatencyAccumulator { return bw.latAccum }
func (bw *BaseWorker) PatternLatencyAccumulator() *metrics.LatencyAccumulator {
	return bw.patternLatAccum
}
func (bw *BaseWorker) ConsumerReady() <-chan struct{} { return bw.consumerReady }
func (bw *BaseWorker) SentCount() uint64              { return bw.sent.Load() }
func (bw *BaseWorker) ReceivedCount() uint64          { return bw.received.Load() }
func (bw *BaseWorker) ErrorCount() uint64             { return bw.errors.Load() }
func (bw *BaseWorker) ReconnectionCount() uint64      { return bw.reconnections.Load() }
func (bw *BaseWorker) CorruptedCount() uint64         { return bw.corrupted.Load() }
func (bw *BaseWorker) DuplicatedCount() uint64        { return bw.trk.TotalDuplicates() }
func (bw *BaseWorker) IncError()                      { bw.errors.Add(1) }
func (bw *BaseWorker) IncReconnection() {
	bw.reconnections.Add(1)
	bw.recentlyReconnected.Store(true)
	bw.reconnDupCooldown.Store(0)
}
func (bw *BaseWorker) RPCLatencyAccumulator() *metrics.LatencyAccumulator { return bw.rpcLatAccum }
func (bw *BaseWorker) PeakRate() *metrics.PeakRateTracker                 { return bw.peakRate }
func (bw *BaseWorker) RPCSuccess() uint64                                 { return bw.rpcSuccess.Load() }
func (bw *BaseWorker) RPCTimeout() uint64                                 { return bw.rpcTimeout.Load() }
func (bw *BaseWorker) RPCError() uint64                                   { return bw.rpcError.Load() }
func (bw *BaseWorker) IncRPCSuccess()                                     { bw.rpcSuccess.Add(1) }
func (bw *BaseWorker) IncRPCTimeout()                                     { bw.rpcTimeout.Add(1) }
func (bw *BaseWorker) IncRPCError()                                       { bw.rpcError.Add(1) }
func (bw *BaseWorker) BytesSentCount() uint64                             { return bw.bytesSent.Load() }
func (bw *BaseWorker) BytesReceivedCount() uint64                         { return bw.bytesReceived.Load() }
func (bw *BaseWorker) UnconfirmedCount() int64                            { return bw.unconfirmed.Load() }
func (bw *BaseWorker) RateWindow() *metrics.SlidingRateWindow             { return bw.rateWindow }

func (bw *BaseWorker) getOrCreateProducerStat(id string) *WorkerStat {
	if v, ok := bw.producerStats.Load(id); ok {
		return v.(*WorkerStat)
	}
	ws := &WorkerStat{
		id:         id,
		latAccum:   metrics.NewLatencyAccumulator(),
		rateWindow: metrics.NewSlidingRateWindow(),
	}
	actual, _ := bw.producerStats.LoadOrStore(id, ws)
	return actual.(*WorkerStat)
}

func (bw *BaseWorker) getOrCreateConsumerStat(id string) *WorkerStat {
	if v, ok := bw.consumerStats.Load(id); ok {
		return v.(*WorkerStat)
	}
	ws := &WorkerStat{
		id:         id,
		latAccum:   metrics.NewLatencyAccumulator(),
		rateWindow: metrics.NewSlidingRateWindow(),
	}
	actual, _ := bw.consumerStats.LoadOrStore(id, ws)
	return actual.(*WorkerStat)
}

// RecordBytesSent tracks bytes sent at both per-run and per-worker level.
func (bw *BaseWorker) RecordBytesSent(producerID string, size int) {
	bw.bytesSent.Add(uint64(size))
	metrics.AddBytesSent(bw.pattern, float64(size))
	ws := bw.getOrCreateProducerStat(producerID)
	ws.bytesSent.Add(uint64(size))
}

// AdvanceRateWindows advances all sliding rate windows (pattern + per-worker).
func (bw *BaseWorker) AdvanceRateWindows() {
	bw.rateWindow.Advance()
	bw.producerStats.Range(func(_, value any) bool {
		value.(*WorkerStat).rateWindow.Advance()
		return true
	})
	bw.consumerStats.Range(func(_, value any) bool {
		value.(*WorkerStat).rateWindow.Advance()
		return true
	})
}

// ProducerStatSnapshots returns a sorted snapshot of all producer stats.
func (bw *BaseWorker) ProducerStatSnapshots() []WorkerStatSnapshot {
	var result []WorkerStatSnapshot
	bw.producerStats.Range(func(_, value any) bool {
		ws := value.(*WorkerStat)
		result = append(result, WorkerStatSnapshot{
			ID:            ws.id,
			Sent:          ws.sent.Load(),
			Errors:        ws.errors.Load(),
			ActualRate:    ws.rateWindow.Rate(),
			BytesSent:     ws.bytesSent.Load(),
			RPCSuccess:    ws.rpcSuccess.Load(),
			RPCTimeout:    ws.rpcTimeout.Load(),
			RPCError:      ws.rpcError.Load(),
			LatencyP50MS:  ws.latAccum.Percentile(50).Seconds() * 1000,
			LatencyP95MS:  ws.latAccum.Percentile(95).Seconds() * 1000,
			LatencyP99MS:  ws.latAccum.Percentile(99).Seconds() * 1000,
			LatencyP999MS: ws.latAccum.Percentile(99.9).Seconds() * 1000,
		})
		return true
	})
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// ConsumerStatSnapshots returns a sorted snapshot of all consumer stats.
func (bw *BaseWorker) ConsumerStatSnapshots() []WorkerStatSnapshot {
	var result []WorkerStatSnapshot
	bw.consumerStats.Range(func(_, value any) bool {
		ws := value.(*WorkerStat)
		result = append(result, WorkerStatSnapshot{
			ID:            ws.id,
			Received:      ws.received.Load(),
			Errors:        ws.errors.Load(),
			Duplicated:    ws.duplicated.Load(),
			Corrupted:     ws.corrupted.Load(),
			BytesReceived: ws.bytesRecv.Load(),
			Responded:     ws.responded.Load(),
			LatencyP50MS:  ws.latAccum.Percentile(50).Seconds() * 1000,
			LatencyP95MS:  ws.latAccum.Percentile(95).Seconds() * 1000,
			LatencyP99MS:  ws.latAccum.Percentile(99).Seconds() * 1000,
			LatencyP999MS: ws.latAccum.Percentile(99.9).Seconds() * 1000,
		})
		return true
	})
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// RecordError increments both the Prometheus counter and the in-process atomic.
func (bw *BaseWorker) RecordError(errorType string) {
	metrics.IncError(bw.pattern, errorType)
	bw.errors.Add(1)
}
func (bw *BaseWorker) Client() *kubemq.Client               { return bw.cp.Get() }
func (bw *BaseWorker) Logger() *slog.Logger                 { return bw.logger }
func (bw *BaseWorker) Config() *config.Config               { return bw.cfg }
func (bw *BaseWorker) TSStore() *metrics.SendTimestampStore { return bw.tsStore }

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

func (bw *BaseWorker) GetTargetRate() int {
	return bw.targetRate
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
	bw.rateWindow.Record()
	ws := bw.getOrCreateProducerStat(producerID)
	ws.sent.Add(1)
	ws.rateWindow.Record()
}

func (bw *BaseWorker) RecordReceive(consumerID string, body []byte, crcTag string, producerID string, seq uint64) {
	if producerID == "warmup" {
		return
	}
	bodyLen := uint64(len(body))
	metrics.AddBytesReceived(bw.pattern, float64(bodyLen))
	bw.bytesReceived.Add(bodyLen)

	ws := bw.getOrCreateConsumerStat(consumerID)
	ws.bytesRecv.Add(bodyLen)

	if !payload.VerifyCRC(body, crcTag) {
		bw.corrupted.Add(1)
		ws.corrupted.Add(1)
		metrics.IncCorrupted(bw.pattern)
		bw.logger.Error("CRC mismatch", "producer", producerID, "seq", seq)
		return
	}

	isDup, isOOO := bw.trk.Record(producerID, seq)
	if isDup {
		metrics.IncDuplicated(bw.pattern)
		ws.duplicated.Add(1)
		if bw.recentlyReconnected.Load() {
			metrics.IncReconnectionDuplicate(bw.pattern)
		}
	} else {
		bw.received.Add(1)
		ws.received.Add(1)
		metrics.IncReceived(bw.pattern, consumerID)
		if bw.recentlyReconnected.Load() {
			if bw.reconnDupCooldown.Add(1) >= 100 {
				bw.recentlyReconnected.Store(false)
			}
		}
	}
	if isOOO {
		metrics.IncOutOfOrder(bw.pattern)
	}

	if sendTime, ok := bw.tsStore.LoadAndDelete(producerID, seq); ok {
		latency := time.Since(sendTime)
		metrics.ObserveLatency(bw.pattern, latency)
		bw.latAccum.Record(latency)
		// Dual-write to pattern-level accumulator
		if bw.patternLatAccum != nil {
			bw.patternLatAccum.Record(latency)
		}
		ws.latAccum.Record(latency)
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
	bw.bytesSent.Store(0)
	bw.bytesReceived.Store(0)
	bw.unconfirmed.Store(0)
	bw.rateWindow.Reset()
	bw.peakRate = metrics.NewPeakRateTracker()
	bw.tsStore = metrics.NewSendTimestampStore()
	bw.producerStats.Range(func(_, value any) bool {
		ws := value.(*WorkerStat)
		ws.sent.Store(0)
		ws.received.Store(0)
		ws.errors.Store(0)
		ws.duplicated.Store(0)
		ws.corrupted.Store(0)
		ws.bytesSent.Store(0)
		ws.bytesRecv.Store(0)
		ws.rpcSuccess.Store(0)
		ws.rpcTimeout.Store(0)
		ws.rpcError.Store(0)
		ws.responded.Store(0)
		ws.latAccum.Reset()
		ws.rateWindow.Reset()
		return true
	})
	bw.consumerStats.Range(func(_, value any) bool {
		ws := value.(*WorkerStat)
		ws.sent.Store(0)
		ws.received.Store(0)
		ws.errors.Store(0)
		ws.duplicated.Store(0)
		ws.corrupted.Store(0)
		ws.bytesSent.Store(0)
		ws.bytesRecv.Store(0)
		ws.rpcSuccess.Store(0)
		ws.rpcTimeout.Store(0)
		ws.rpcError.Store(0)
		ws.responded.Store(0)
		ws.latAccum.Reset()
		ws.rateWindow.Reset()
		return true
	})
}

// ChannelNameForPattern generates a v2 channel name.
func ChannelNameForPattern(runID, pattern string, channelIndex int) string {
	return fmt.Sprintf("go_burnin_%s_%s_%04d", runID, pattern, channelIndex)
}

// ConsumerGroupName generates a v2 consumer group name.
func ConsumerGroupName(runID, pattern string, channelIndex int) string {
	return fmt.Sprintf("go_burnin_%s_%s_%04d_group", runID, pattern, channelIndex)
}

// ProducerID generates a v2 producer ID: p-{pattern}-{4-digit-channel}-{3-digit-worker}
func ProducerID(pattern string, channelIndex, workerIndex int) string {
	return fmt.Sprintf("p-%s-%04d-%03d", pattern, channelIndex, workerIndex)
}

// ConsumerID generates a v2 consumer ID: c-{pattern}-{4-digit-channel}-{3-digit-worker}
func ConsumerID(pattern string, channelIndex, workerIndex int) string {
	return fmt.Sprintf("c-%s-%04d-%03d", pattern, channelIndex, workerIndex)
}

// SenderID generates a v2 sender ID: s-{pattern}-{4-digit-channel}-{3-digit-worker}
func SenderID(pattern string, channelIndex, workerIndex int) string {
	return fmt.Sprintf("s-%s-%04d-%03d", pattern, channelIndex, workerIndex)
}

// ResponderID generates a v2 responder ID: r-{pattern}-{4-digit-channel}-{3-digit-worker}
func ResponderID(pattern string, channelIndex, workerIndex int) string {
	return fmt.Sprintf("r-%s-%04d-%03d", pattern, channelIndex, workerIndex)
}
