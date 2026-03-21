package metrics

import (
	"sort"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const sdkLabel = "go"

var (
	// Counters
	MessagesSentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_sent_total",
		Help: "Total messages sent",
	}, []string{"sdk", "pattern", "producer_id"})

	MessagesReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_received_total",
		Help: "Total messages received",
	}, []string{"sdk", "pattern", "consumer_id"})

	MessagesLostTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_lost_total",
		Help: "Confirmed lost messages",
	}, []string{"sdk", "pattern"})

	MessagesDuplicatedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_duplicated_total",
		Help: "Messages detected as duplicated",
	}, []string{"sdk", "pattern"})

	MessagesCorruptedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_corrupted_total",
		Help: "Messages with CRC32 hash mismatch",
	}, []string{"sdk", "pattern"})

	MessagesOutOfOrderTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_out_of_order_total",
		Help: "Messages received out of sequence order",
	}, []string{"sdk", "pattern"})

	MessagesUnconfirmedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_unconfirmed_total",
		Help: "Messages sent but not confirmed by server",
	}, []string{"sdk", "pattern"})

	ReconnectionDuplicatesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_reconnection_duplicates_total",
		Help: "Duplicates that occurred immediately after reconnection",
	}, []string{"sdk", "pattern"})

	ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_errors_total",
		Help: "Errors by type",
	}, []string{"sdk", "pattern", "error_type"})

	ReconnectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_reconnections_total",
		Help: "Number of reconnection events",
	}, []string{"sdk", "pattern"})

	BytesSentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_bytes_sent_total",
		Help: "Total bytes sent",
	}, []string{"sdk", "pattern"})

	BytesReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_bytes_received_total",
		Help: "Total bytes received",
	}, []string{"sdk", "pattern"})

	RPCResponsesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_rpc_responses_total",
		Help: "RPC responses by status",
	}, []string{"sdk", "pattern", "status"})

	DowntimeSecondsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_downtime_seconds_total",
		Help: "Cumulative time spent in reconnecting state",
	}, []string{"sdk", "pattern"})

	ForcedDisconnectsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "burnin_forced_disconnects_total",
		Help:        "Number of forced disconnect events",
		ConstLabels: prometheus.Labels{"sdk": sdkLabel},
	})

	// Histograms
	latencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	rpcBuckets     = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

	MessageLatencySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "burnin_message_latency_seconds",
		Help:    "End-to-end message latency",
		Buckets: latencyBuckets,
	}, []string{"sdk", "pattern"})

	SendDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "burnin_send_duration_seconds",
		Help:    "Time from SDK Send() call to call returning",
		Buckets: latencyBuckets,
	}, []string{"sdk", "pattern"})

	RPCDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "burnin_rpc_duration_seconds",
		Help:    "RPC round-trip duration",
		Buckets: rpcBuckets,
	}, []string{"sdk", "pattern"})

	// Gauges
	ActiveConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_active_connections",
		Help: "Currently active connections",
	}, []string{"sdk", "pattern"})

	UptimeSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name:        "burnin_uptime_seconds",
		Help:        "Burn-in app uptime",
		ConstLabels: prometheus.Labels{"sdk": sdkLabel},
	})

	TargetRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_target_rate",
		Help: "Configured target rate (msgs/sec)",
	}, []string{"sdk", "pattern"})

	ActualRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_actual_rate",
		Help: "Current achieved rate (msgs/sec)",
	}, []string{"sdk", "pattern"})

	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_consumer_lag_messages",
		Help: "Sent minus received (producer-consumer lag)",
	}, []string{"sdk", "pattern"})

	ConsumerGroupBalance = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_consumer_group_balance_ratio",
		Help: "min_consumer_count / max_consumer_count",
	}, []string{"sdk", "pattern"})

	WarmupActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name:        "burnin_warmup_active",
		Help:        "1 during warmup period, 0 after",
		ConstLabels: prometheus.Labels{"sdk": sdkLabel},
	})

	ActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name:        "burnin_active_workers",
		Help:        "Active goroutines",
		ConstLabels: prometheus.Labels{"sdk": sdkLabel},
	})
)

// SDK returns the canonical SDK label.
func SDK() string { return sdkLabel }

// InitMetrics pre-initializes all Prometheus metrics to 0 with well-known label
// values. This prevents absent() alerts in dashboards on boot.
func InitMetrics() {
	patterns := []string{"events", "events_store", "queue_stream", "queue_simple", "commands", "queries"}
	for _, p := range patterns {
		MessagesSentTotal.WithLabelValues(sdkLabel, p, "p-"+p+"-000").Add(0)
		MessagesReceivedTotal.WithLabelValues(sdkLabel, p, "c-"+p+"-000").Add(0)
		MessagesLostTotal.WithLabelValues(sdkLabel, p).Add(0)
		MessagesDuplicatedTotal.WithLabelValues(sdkLabel, p).Add(0)
		MessagesCorruptedTotal.WithLabelValues(sdkLabel, p).Add(0)
		MessagesOutOfOrderTotal.WithLabelValues(sdkLabel, p).Add(0)
		MessagesUnconfirmedTotal.WithLabelValues(sdkLabel, p).Add(0)
		ReconnectionDuplicatesTotal.WithLabelValues(sdkLabel, p).Add(0)
		ErrorsTotal.WithLabelValues(sdkLabel, p, "send_failure").Add(0)
		ReconnectionsTotal.WithLabelValues(sdkLabel, p).Add(0)
		BytesSentTotal.WithLabelValues(sdkLabel, p).Add(0)
		BytesReceivedTotal.WithLabelValues(sdkLabel, p).Add(0)
		DowntimeSecondsTotal.WithLabelValues(sdkLabel, p).Add(0)
		ActiveConnections.WithLabelValues(sdkLabel, p).Set(0)
		TargetRate.WithLabelValues(sdkLabel, p).Set(0)
		ActualRate.WithLabelValues(sdkLabel, p).Set(0)
		ConsumerLag.WithLabelValues(sdkLabel, p).Set(0)
		ConsumerGroupBalance.WithLabelValues(sdkLabel, p).Set(0)
	}
	for _, p := range patterns[:4] {
		RPCResponsesTotal.WithLabelValues(sdkLabel, p, "success").Add(0)
	}
	for _, p := range patterns[4:] {
		RPCResponsesTotal.WithLabelValues(sdkLabel, p, "success").Add(0)
		RPCResponsesTotal.WithLabelValues(sdkLabel, p, "timeout").Add(0)
		RPCResponsesTotal.WithLabelValues(sdkLabel, p, "error").Add(0)
	}
	UptimeSeconds.Set(0)
	WarmupActive.Set(0)
	ActiveWorkers.Set(0)
}

// IncSent increments the sent counter for a pattern/producer.
func IncSent(pattern, producerID string) {
	MessagesSentTotal.WithLabelValues(sdkLabel, pattern, producerID).Inc()
}

// IncReceived increments the received counter for a pattern/consumer.
func IncReceived(pattern, consumerID string) {
	MessagesReceivedTotal.WithLabelValues(sdkLabel, pattern, consumerID).Inc()
}

// AddLost adds confirmed lost messages for a pattern.
func AddLost(pattern string, count float64) {
	MessagesLostTotal.WithLabelValues(sdkLabel, pattern).Add(count)
}

// IncDuplicated increments the duplicated counter.
func IncDuplicated(pattern string) {
	MessagesDuplicatedTotal.WithLabelValues(sdkLabel, pattern).Inc()
}

// IncCorrupted increments the corrupted counter.
func IncCorrupted(pattern string) {
	MessagesCorruptedTotal.WithLabelValues(sdkLabel, pattern).Inc()
}

// IncOutOfOrder increments the out-of-order counter.
func IncOutOfOrder(pattern string) {
	MessagesOutOfOrderTotal.WithLabelValues(sdkLabel, pattern).Inc()
}

// IncUnconfirmed increments the unconfirmed counter.
func IncUnconfirmed(pattern string) {
	MessagesUnconfirmedTotal.WithLabelValues(sdkLabel, pattern).Inc()
}

// IncError increments the error counter.
func IncError(pattern, errorType string) {
	ErrorsTotal.WithLabelValues(sdkLabel, pattern, errorType).Inc()
}

// IncReconnection increments the reconnection counter.
func IncReconnection(pattern string) {
	ReconnectionsTotal.WithLabelValues(sdkLabel, pattern).Inc()
}

// AddBytesSent adds bytes to the sent counter.
func AddBytesSent(pattern string, bytes float64) {
	BytesSentTotal.WithLabelValues(sdkLabel, pattern).Add(bytes)
}

// AddBytesReceived adds bytes to the received counter.
func AddBytesReceived(pattern string, bytes float64) {
	BytesReceivedTotal.WithLabelValues(sdkLabel, pattern).Add(bytes)
}

// ObserveLatency records a message latency.
func ObserveLatency(pattern string, d time.Duration) {
	MessageLatencySeconds.WithLabelValues(sdkLabel, pattern).Observe(d.Seconds())
}

// ObserveSendDuration records a send duration.
func ObserveSendDuration(pattern string, d time.Duration) {
	SendDurationSeconds.WithLabelValues(sdkLabel, pattern).Observe(d.Seconds())
}

// ObserveRPCDuration records an RPC duration.
func ObserveRPCDuration(pattern string, d time.Duration) {
	RPCDurationSeconds.WithLabelValues(sdkLabel, pattern).Observe(d.Seconds())
}

// IncRPCResponse increments the RPC response counter.
func IncRPCResponse(pattern, status string) {
	RPCResponsesTotal.WithLabelValues(sdkLabel, pattern, status).Inc()
}

// SetActiveConnections sets the active connection gauge for a pattern.
func SetActiveConnections(pattern string, count float64) {
	ActiveConnections.WithLabelValues(sdkLabel, pattern).Set(count)
}

// SetConsumerGroupBalance sets the consumer group balance ratio for a pattern.
func SetConsumerGroupBalance(pattern string, ratio float64) {
	ConsumerGroupBalance.WithLabelValues(sdkLabel, pattern).Set(ratio)
}

// AddDowntime adds downtime seconds to the cumulative downtime counter for a pattern.
func AddDowntime(pattern string, seconds float64) {
	DowntimeSecondsTotal.WithLabelValues(sdkLabel, pattern).Add(seconds)
}

// IncReconnectionDuplicate increments the reconnection-duplicate counter for a pattern.
func IncReconnectionDuplicate(pattern string) {
	ReconnectionDuplicatesTotal.WithLabelValues(sdkLabel, pattern).Inc()
}

// LatencyAccumulator tracks latency values in-memory for percentile computation at verdict time.
// Uses HdrHistogram for memory-efficient, high-precision percentile calculation.
type LatencyAccumulator struct {
	mu   sync.Mutex
	hist *hdrhistogram.Histogram
}

// NewLatencyAccumulator creates a new accumulator.
// Records latencies from 1 microsecond to 60 seconds with 3 significant digits.
func NewLatencyAccumulator() *LatencyAccumulator {
	return &LatencyAccumulator{
		hist: hdrhistogram.New(1, 60_000_000, 3), // 1us to 60s in microseconds
	}
}

// Record adds a latency observation.
func (a *LatencyAccumulator) Record(d time.Duration) {
	a.mu.Lock()
	_ = a.hist.RecordValue(d.Microseconds())
	a.mu.Unlock()
}

// Percentile returns the value at the given percentile (0-100).
func (a *LatencyAccumulator) Percentile(p float64) time.Duration {
	a.mu.Lock()
	v := a.hist.ValueAtQuantile(p)
	a.mu.Unlock()
	return time.Duration(v) * time.Microsecond
}

// Reset clears the accumulator (used after warmup).
func (a *LatencyAccumulator) Reset() {
	a.mu.Lock()
	a.hist.Reset()
	a.mu.Unlock()
}

// Count returns the total number of recorded values.
func (a *LatencyAccumulator) Count() int64 {
	a.mu.Lock()
	c := a.hist.TotalCount()
	a.mu.Unlock()
	return c
}

// SendTimestampStore tracks monotonic send timestamps for latency computation.
// Uses sync.Map for concurrent producer/consumer access.
type SendTimestampStore struct {
	store sync.Map // key: "producerID:seq" -> value: time.Time (monotonic)
}

func NewSendTimestampStore() *SendTimestampStore {
	return &SendTimestampStore{}
}

type tsKey struct {
	ProducerID string
	Seq        uint64
}

func (s *SendTimestampStore) Store(producerID string, seq uint64, t time.Time) {
	s.store.Store(tsKey{producerID, seq}, t)
}

func (s *SendTimestampStore) LoadAndDelete(producerID string, seq uint64) (time.Time, bool) {
	v, ok := s.store.LoadAndDelete(tsKey{producerID, seq})
	if !ok {
		return time.Time{}, false
	}
	return v.(time.Time), true
}

// Purge removes entries older than maxAge.
func (s *SendTimestampStore) Purge(maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge)
	s.store.Range(func(key, value any) bool {
		if t, ok := value.(time.Time); ok && t.Before(cutoff) {
			s.store.Delete(key)
		}
		return true
	})
}

// RateTracker tracks message rate over a sliding window.
type RateTracker struct {
	mu      sync.Mutex
	times   []time.Time
	maxSize int
}

func NewRateTracker(windowSize int) *RateTracker {
	return &RateTracker{
		times:   make([]time.Time, 0, windowSize),
		maxSize: windowSize,
	}
}

func (r *RateTracker) Record() {
	r.mu.Lock()
	now := time.Now()
	r.times = append(r.times, now)
	// Trim to window
	if len(r.times) > r.maxSize {
		r.times = r.times[len(r.times)-r.maxSize:]
	}
	r.mu.Unlock()
}

func (r *RateTracker) Rate(window time.Duration) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.times) < 2 {
		return 0
	}

	cutoff := time.Now().Add(-window)
	// Find the first index within window
	idx := sort.Search(len(r.times), func(i int) bool {
		return r.times[i].After(cutoff)
	})

	count := len(r.times) - idx
	if count < 2 {
		return 0
	}

	elapsed := r.times[len(r.times)-1].Sub(r.times[idx])
	if elapsed <= 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}

// PeakRateTracker tracks peak throughput over a 10-second sliding window.
// Call Advance() every second to rotate buckets, and Record() for each message.
type PeakRateTracker struct {
	mu       sync.Mutex
	buckets  []int64   // per-second counts (10 buckets)
	idx      int       // current bucket index
	peak     float64   // peak 10-second average seen so far
	lastTick time.Time // last time Advance was called
}

const peakWindowSize = 10

// NewPeakRateTracker creates a new PeakRateTracker with a 10-second sliding window.
func NewPeakRateTracker() *PeakRateTracker {
	return &PeakRateTracker{
		buckets:  make([]int64, peakWindowSize),
		lastTick: time.Now(),
	}
}

// Record increments the current bucket count (call for each message).
func (p *PeakRateTracker) Record() {
	p.mu.Lock()
	p.buckets[p.idx]++
	p.mu.Unlock()
}

// Advance rotates to the next bucket and updates the peak if the current
// 10-second average exceeds the previous peak. Call this once per second.
func (p *PeakRateTracker) Advance() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Compute current 10-second average
	var total int64
	for _, b := range p.buckets {
		total += b
	}
	avg := float64(total) / float64(peakWindowSize)
	if avg > p.peak {
		p.peak = avg
	}

	// Move to next bucket and clear it
	p.idx = (p.idx + 1) % peakWindowSize
	p.buckets[p.idx] = 0
	p.lastTick = time.Now()
}

// Peak returns the peak 10-second average throughput (msgs/sec).
func (p *PeakRateTracker) Peak() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

// SlidingRateWindow tracks message rate over a 30-second sliding window.
// Call Advance() once per second to rotate buckets, and Record() per message.
const slidingWindowSize = 30

type SlidingRateWindow struct {
	mu      sync.Mutex
	buckets [slidingWindowSize]int64
	idx     int
	total   int64
	ticks   int
}

func NewSlidingRateWindow() *SlidingRateWindow {
	return &SlidingRateWindow{}
}

func (w *SlidingRateWindow) Record() {
	w.mu.Lock()
	w.buckets[w.idx]++
	w.total++
	w.mu.Unlock()
}

func (w *SlidingRateWindow) Advance() {
	w.mu.Lock()
	w.idx = (w.idx + 1) % slidingWindowSize
	w.total -= w.buckets[w.idx]
	w.buckets[w.idx] = 0
	w.ticks++
	w.mu.Unlock()
}

// Rate returns the average messages/sec over the sliding window.
// Uses min(ticks, 30) as the divisor to avoid undercount during ramp-up.
func (w *SlidingRateWindow) Rate() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	window := w.ticks
	if window > slidingWindowSize {
		window = slidingWindowSize
	}
	if window == 0 {
		return 0
	}
	return float64(w.total) / float64(window)
}

func (w *SlidingRateWindow) Reset() {
	w.mu.Lock()
	for i := range w.buckets {
		w.buckets[i] = 0
	}
	w.total = 0
	w.idx = 0
	w.ticks = 0
	w.mu.Unlock()
}
