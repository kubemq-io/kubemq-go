package report

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/burnin/config"
)

// Summary is the top-level report structure written as JSON and returned by the /run/report endpoint.
type Summary struct {
	RunID              string                   `json:"run_id"`
	SDK                string                   `json:"sdk"`
	Version            string                   `json:"sdk_version"`
	Mode               string                   `json:"mode"`
	BrokerAddress      string                   `json:"broker_address"`
	StartedAt          time.Time                `json:"started_at"`
	EndedAt            time.Time                `json:"ended_at"`
	DurationSecs       float64                  `json:"duration_seconds"`
	AllPatternsEnabled bool                     `json:"all_patterns_enabled"`
	Status             string                   `json:"status,omitempty"`
	Patterns           map[string]*PatternStats `json:"patterns"`
	Resources          ResourceStats            `json:"resources"`
	Verdict            Verdict                  `json:"verdict"`
	BaselineFullWindow bool                     `json:"-"`
}

// PatternStats holds per-pattern statistics.
type PatternStats struct {
	Enabled          bool    `json:"enabled"`
	Status           string  `json:"status,omitempty"`
	Sent             uint64  `json:"sent"`
	Received         uint64  `json:"received"`
	Lost             uint64  `json:"lost"`
	Duplicated       uint64  `json:"duplicated"`
	Corrupted        uint64  `json:"corrupted"`
	OutOfOrder       uint64  `json:"out_of_order"`
	LossPct          float64 `json:"loss_pct"`
	Errors           uint64  `json:"errors"`
	Reconnections    uint64  `json:"reconnections"`
	DowntimeSeconds  float64 `json:"downtime_seconds,omitempty"`
	LatencyP50MS     float64 `json:"latency_p50_ms,omitempty"`
	LatencyP95MS     float64 `json:"latency_p95_ms,omitempty"`
	LatencyP99MS     float64 `json:"latency_p99_ms,omitempty"`
	LatencyP999MS    float64 `json:"latency_p999_ms,omitempty"`
	AvgThroughput    float64 `json:"avg_rate,omitempty"`
	PeakThroughput   float64 `json:"peak_rate,omitempty"`
	TargetRate       int     `json:"target_rate"`
	BytesSent        uint64  `json:"bytes_sent,omitempty"`
	BytesReceived    uint64  `json:"bytes_received,omitempty"`
	ResponsesSuccess uint64  `json:"responses_success,omitempty"`
	ResponsesTimeout uint64  `json:"responses_timeout,omitempty"`
	ResponsesError   uint64  `json:"responses_error,omitempty"`
	RPCP50MS         float64 `json:"rpc_p50_ms,omitempty"`
	RPCP95MS         float64 `json:"rpc_p95_ms,omitempty"`
	RPCP99MS         float64 `json:"rpc_p99_ms,omitempty"`
	RPCP999MS        float64 `json:"rpc_p999_ms,omitempty"`
	AvgThroughputRPC float64 `json:"avg_throughput_rpc_sec,omitempty"`
	Unconfirmed      int64   `json:"unconfirmed,omitempty"`

	// v2 multi-channel fields
	Channels             int  `json:"channels"`
	ProducersPerChannel  int  `json:"producers_per_channel,omitempty"`
	ConsumersPerChannel  int  `json:"consumers_per_channel,omitempty"`
	SendersPerChannel    int  `json:"senders_per_channel,omitempty"`
	RespondersPerChannel int  `json:"responders_per_channel,omitempty"`
	ConsumerGroup        bool `json:"consumer_group,omitempty"`
	NumConsumers         int  `json:"num_consumers,omitempty"`

	Producers  []*ProducerStat  `json:"producers,omitempty"`
	Consumers  []*ConsumerStat  `json:"consumers,omitempty"`
	Senders    []*SenderStat    `json:"senders,omitempty"`
	Responders []*ResponderStat `json:"responders,omitempty"`

	// Per-channel data for verdict checks (not serialized to JSON report)
	PerChannel []ChannelStats `json:"-"`
}

// ChannelStats holds per-channel statistics used for fail-on-any-channel verdict checks.
type ChannelStats struct {
	Index      int // 1-based channel index
	Sent       uint64
	Received   uint64
	Lost       uint64
	Duplicated uint64
	Corrupted  uint64
	Errors     uint64
}

// ProducerStat holds per-producer metrics for report.
type ProducerStat struct {
	ID            string  `json:"id"`
	Sent          uint64  `json:"sent"`
	Errors        uint64  `json:"errors"`
	AvgRate       float64 `json:"avg_rate"`
	BytesSent     uint64  `json:"bytes_sent,omitempty"`
	LatencyP50MS  float64 `json:"latency_p50_ms,omitempty"`
	LatencyP95MS  float64 `json:"latency_p95_ms,omitempty"`
	LatencyP99MS  float64 `json:"latency_p99_ms,omitempty"`
	LatencyP999MS float64 `json:"latency_p999_ms,omitempty"`
}

// ConsumerStat holds per-consumer metrics for report.
type ConsumerStat struct {
	ID            string  `json:"id"`
	Received      uint64  `json:"received"`
	Errors        uint64  `json:"errors"`
	Duplicated    uint64  `json:"duplicated,omitempty"`
	Corrupted     uint64  `json:"corrupted,omitempty"`
	BytesReceived uint64  `json:"bytes_received,omitempty"`
	LatencyP50MS  float64 `json:"latency_p50_ms,omitempty"`
	LatencyP95MS  float64 `json:"latency_p95_ms,omitempty"`
	LatencyP99MS  float64 `json:"latency_p99_ms,omitempty"`
	LatencyP999MS float64 `json:"latency_p999_ms,omitempty"`
}

// SenderStat holds per-sender RPC metrics for report.
type SenderStat struct {
	ID               string  `json:"id"`
	Sent             uint64  `json:"sent"`
	ResponsesSuccess uint64  `json:"responses_success"`
	ResponsesTimeout uint64  `json:"responses_timeout"`
	ResponsesError   uint64  `json:"responses_error"`
	Errors           uint64  `json:"errors"`
	AvgRate          float64 `json:"avg_rate"`
	LatencyP50MS     float64 `json:"latency_p50_ms,omitempty"`
	LatencyP95MS     float64 `json:"latency_p95_ms,omitempty"`
	LatencyP99MS     float64 `json:"latency_p99_ms,omitempty"`
	LatencyP999MS    float64 `json:"latency_p999_ms,omitempty"`
}

// ResponderStat holds per-responder RPC metrics for report.
type ResponderStat struct {
	ID        string `json:"id"`
	Responded uint64 `json:"responded"`
	Errors    uint64 `json:"errors"`
}

// ResourceStats holds resource-usage metrics.
type ResourceStats struct {
	PeakRSSMB          float64 `json:"peak_rss_mb"`
	BaselineRSSMB      float64 `json:"baseline_rss_mb"`
	MemoryGrowthFactor float64 `json:"memory_growth_factor"`
	PeakWorkers        int     `json:"peak_workers"`
}

// Verdict is the pass/fail result of all checks.
type Verdict struct {
	Result   string                  `json:"result"` // PASSED, PASSED_WITH_WARNINGS, FAILED
	Passed   bool                    `json:"passed"` // convenience: true if result != FAILED
	Warnings []string                `json:"warnings"`
	Checks   map[string]*CheckResult `json:"checks"`
}

// CheckResult is the outcome of a single check.
type CheckResult struct {
	Passed    bool   `json:"passed"`
	Threshold string `json:"threshold"`
	Actual    string `json:"actual"`
	Advisory  bool   `json:"advisory,omitempty"`
}

// pubsubQueuePatterns are patterns that have message_loss and duplication checks.
var pubsubQueuePatterns = map[string]bool{
	"events": true, "events_store": true,
	"queue_stream": true, "queue_simple": true,
}

// rpcCheckPatterns are the RPC patterns.
var rpcCheckPatterns = map[string]bool{
	"commands": true, "queries": true,
}

// defaultLossThresholds per spec.
var defaultLossThresholds = map[string]float64{
	"events": 5.0, "events_store": 0.0,
	"queue_stream": 0.0, "queue_simple": 0.0,
}

// Generate runs all verdict checks and returns the Verdict.
// v2: per-channel fail-on-any-channel checks for loss, broadcast, duplication, corruption.
func Generate(summary *Summary, cfg *config.Config) *Verdict {
	v := &Verdict{
		Checks:   make(map[string]*CheckResult),
		Warnings: []string{},
	}

	getLossThreshold := func(pattern string) float64 {
		if cfg.PerPatternThresholds != nil {
			if ppt, ok := cfg.PerPatternThresholds[pattern]; ok && ppt.HasLossPct {
				return ppt.MaxLossPct
			}
		}
		if pattern == "events" {
			return cfg.Thresholds.MaxEventsLossPct
		}
		if d, ok := defaultLossThresholds[pattern]; ok {
			return d
		}
		return cfg.Thresholds.MaxLossPct
	}

	getP99Threshold := func(pattern string) float64 {
		if cfg.PerPatternThresholds != nil {
			if ppt, ok := cfg.PerPatternThresholds[pattern]; ok && ppt.HasP99 {
				return ppt.MaxP99LatencyMS
			}
		}
		return cfg.Thresholds.MaxP99LatencyMS
	}

	getP999Threshold := func(pattern string) float64 {
		if cfg.PerPatternThresholds != nil {
			if ppt, ok := cfg.PerPatternThresholds[pattern]; ok && ppt.HasP999 {
				return ppt.MaxP999LatencyMS
			}
		}
		return cfg.Thresholds.MaxP999LatencyMS
	}

	// Per-pattern message_loss checks (pub/sub + queue only)
	// v2: fail-on-any-channel -- check each channel independently, report worst-case
	for name, ps := range summary.Patterns {
		if !pubsubQueuePatterns[name] || !ps.Enabled {
			continue
		}
		threshold := getLossThreshold(name)

		if len(ps.PerChannel) > 1 {
			// Multi-channel: check per-channel, fail if ANY exceeds threshold
			worstPct := 0.0
			worstIdx := 0
			worstSent := uint64(0)
			worstLost := uint64(0)
			allPassed := true

			for _, ch := range ps.PerChannel {
				chLossPct := 0.0
				if ch.Sent > 0 {
					chLossPct = float64(ch.Lost) / float64(ch.Sent) * 100.0
				}
				if chLossPct > worstPct {
					worstPct = chLossPct
					worstIdx = ch.Index
					worstSent = ch.Sent
					worstLost = ch.Lost
				}
				if chLossPct > threshold {
					allPassed = false
				}
			}

			actual := fmt.Sprintf("%.5f%%", worstPct)
			if !allPassed {
				actual = fmt.Sprintf("ch_%04d: %.1f%% (%d/%d)", worstIdx, worstPct, worstLost, worstSent)
			}
			v.Checks[fmt.Sprintf("message_loss:%s", name)] = &CheckResult{
				Threshold: fmt.Sprintf("%.1f%%", threshold),
				Actual:    actual,
				Passed:    allPassed,
			}
		} else {
			// Single channel or no per-channel data: aggregate check
			v.Checks[fmt.Sprintf("message_loss:%s", name)] = &CheckResult{
				Threshold: fmt.Sprintf("%.1f%%", threshold),
				Actual:    fmt.Sprintf("%.5f%%", ps.LossPct),
				Passed:    ps.LossPct <= threshold,
			}
		}
	}

	// Per-pattern duplication / broadcast checks
	// v2: fail-on-any-channel for broadcast, duplication, and consumer-group duplication
	for name, ps := range summary.Patterns {
		if !pubsubQueuePatterns[name] || !ps.Enabled {
			continue
		}
		isEventPattern := name == "events" || name == "events_store"

		if isEventPattern && !ps.ConsumerGroup && ps.NumConsumers > 1 {
			// Broadcast mode: per-channel arithmetic check
			if len(ps.PerChannel) > 1 {
				allPassed := true
				worstIdx := 0
				worstRecv := uint64(0)
				worstExpected := uint64(0)

				for _, ch := range ps.PerChannel {
					expected := ch.Sent * uint64(ps.NumConsumers)
					if ch.Received != expected {
						allPassed = false
						if worstExpected == 0 || (expected > 0 && ch.Received < worstRecv) {
							worstIdx = ch.Index
							worstRecv = ch.Received
							worstExpected = expected
						}
					}
				}

				actual := fmt.Sprintf("%d", ps.Received)
				if !allPassed {
					actual = fmt.Sprintf("ch_%04d: %d/%d", worstIdx, worstRecv, worstExpected)
				}
				v.Checks[fmt.Sprintf("broadcast:%s", name)] = &CheckResult{
					Threshold: fmt.Sprintf("sent\u00d7%d", ps.NumConsumers),
					Actual:    actual,
					Passed:    allPassed,
				}
			} else {
				// Single channel: aggregate
				expectedTotal := ps.Sent * uint64(ps.NumConsumers)
				broadcastOk := ps.Received == expectedTotal
				v.Checks[fmt.Sprintf("broadcast:%s", name)] = &CheckResult{
					Threshold: fmt.Sprintf("%d\u00d7%d", ps.Sent, ps.NumConsumers),
					Actual:    fmt.Sprintf("%d", ps.Received),
					Passed:    broadcastOk,
				}
			}
		} else if isEventPattern && ps.ConsumerGroup {
			// Consumer group mode: strict 0% duplication, per-channel
			if len(ps.PerChannel) > 1 {
				allPassed := true
				worstPct := 0.0
				worstIdx := 0

				for _, ch := range ps.PerChannel {
					chDupPct := 0.0
					if ch.Sent > 0 {
						chDupPct = float64(ch.Duplicated) / float64(ch.Sent) * 100.0
					}
					if chDupPct > worstPct {
						worstPct = chDupPct
						worstIdx = ch.Index
					}
					if ch.Duplicated > 0 {
						allPassed = false
					}
				}

				actual := "0.00000%"
				if !allPassed {
					actual = fmt.Sprintf("ch_%04d: %.5f%%", worstIdx, worstPct)
				}
				v.Checks[fmt.Sprintf("duplication:%s", name)] = &CheckResult{
					Threshold: "0.0%",
					Actual:    actual,
					Passed:    allPassed,
				}
			} else {
				dupPct := 0.0
				if ps.Sent > 0 {
					dupPct = float64(ps.Duplicated) / float64(ps.Sent) * 100.0
				}
				v.Checks[fmt.Sprintf("duplication:%s", name)] = &CheckResult{
					Threshold: "0.0%",
					Actual:    fmt.Sprintf("%.5f%%", dupPct),
					Passed:    dupPct == 0,
				}
			}
		} else {
			// Queue patterns or single-consumer events: threshold-based duplication
			if len(ps.PerChannel) > 1 {
				allPassed := true
				worstPct := 0.0
				worstIdx := 0

				for _, ch := range ps.PerChannel {
					chDupPct := 0.0
					if ch.Sent > 0 {
						chDupPct = float64(ch.Duplicated) / float64(ch.Sent) * 100.0
					}
					if chDupPct > worstPct {
						worstPct = chDupPct
						worstIdx = ch.Index
					}
					if chDupPct > cfg.Thresholds.MaxDuplicationPct {
						allPassed = false
					}
				}

				actual := fmt.Sprintf("%.5f%%", worstPct)
				if !allPassed {
					actual = fmt.Sprintf("ch_%04d: %.5f%%", worstIdx, worstPct)
				}
				v.Checks[fmt.Sprintf("duplication:%s", name)] = &CheckResult{
					Threshold: fmt.Sprintf("%.1f%%", cfg.Thresholds.MaxDuplicationPct),
					Actual:    actual,
					Passed:    allPassed,
				}
			} else {
				dupPct := 0.0
				if ps.Sent > 0 {
					dupPct = float64(ps.Duplicated) / float64(ps.Sent) * 100.0
				}
				v.Checks[fmt.Sprintf("duplication:%s", name)] = &CheckResult{
					Threshold: fmt.Sprintf("%.1f%%", cfg.Thresholds.MaxDuplicationPct),
					Actual:    fmt.Sprintf("%.5f%%", dupPct),
					Passed:    dupPct <= cfg.Thresholds.MaxDuplicationPct,
				}
			}
		}
	}

	// Corruption check -- global, but also per-channel: fail if ANY channel has corruption
	{
		var totalCorrupted uint64
		anyChannelCorrupted := false
		worstCorruptedIdx := 0
		worstCorruptedCount := uint64(0)
		worstCorruptedPattern := ""

		for name, ps := range summary.Patterns {
			if !ps.Enabled {
				continue
			}
			totalCorrupted += ps.Corrupted

			for _, ch := range ps.PerChannel {
				if ch.Corrupted > 0 {
					anyChannelCorrupted = true
					if ch.Corrupted > worstCorruptedCount {
						worstCorruptedCount = ch.Corrupted
						worstCorruptedIdx = ch.Index
						worstCorruptedPattern = name
					}
				}
			}
		}

		actual := fmt.Sprintf("%d", totalCorrupted)
		if anyChannelCorrupted {
			actual = fmt.Sprintf("%d (worst: %s ch_%04d: %d)", totalCorrupted, worstCorruptedPattern, worstCorruptedIdx, worstCorruptedCount)
		}
		v.Checks["corruption"] = &CheckResult{
			Threshold: "0",
			Actual:    actual,
			Passed:    totalCorrupted == 0,
		}
	}

	// Per-pattern p99_latency checks (from pattern-level accumulator)
	for name, ps := range summary.Patterns {
		if !ps.Enabled {
			continue
		}
		threshold := getP99Threshold(name)
		latency := ps.LatencyP99MS
		if rpcCheckPatterns[name] && ps.RPCP99MS > 0 {
			latency = ps.RPCP99MS
		}
		v.Checks[fmt.Sprintf("p99_latency:%s", name)] = &CheckResult{
			Threshold: fmt.Sprintf("%.0fms", threshold),
			Actual:    fmt.Sprintf("%.1fms", latency),
			Passed:    latency <= threshold,
		}
	}

	// Per-pattern p999_latency checks
	for name, ps := range summary.Patterns {
		if !ps.Enabled {
			continue
		}
		threshold := getP999Threshold(name)
		latency := ps.LatencyP999MS
		if rpcCheckPatterns[name] && ps.RPCP999MS > 0 {
			latency = ps.RPCP999MS
		}
		v.Checks[fmt.Sprintf("p999_latency:%s", name)] = &CheckResult{
			Threshold: fmt.Sprintf("%.0fms", threshold),
			Actual:    fmt.Sprintf("%.1fms", latency),
			Passed:    latency <= threshold,
		}
	}

	// Throughput check -- soak only
	// v2: throughput_pct = aggregate_avg_throughput / (rate * channels) * 100
	if cfg.Mode == "soak" {
		minPct := -1.0
		for _, ps := range summary.Patterns {
			if !ps.Enabled || ps.TargetRate == 0 {
				continue
			}
			pct := ps.AvgThroughput / float64(ps.TargetRate) * 100.0
			if minPct < 0 || pct < minPct {
				minPct = pct
			}
		}
		if minPct < 0 {
			minPct = 100.0
		}
		v.Checks["throughput"] = &CheckResult{
			Threshold: fmt.Sprintf("%.0f%%", cfg.Thresholds.MinThroughputPct),
			Actual:    fmt.Sprintf("%.0f%%", minPct),
			Passed:    minPct >= cfg.Thresholds.MinThroughputPct,
		}
	}

	// Per-pattern error_rate checks -- aggregated across all channels
	for name, ps := range summary.Patterns {
		if !ps.Enabled {
			continue
		}
		errorPct := 0.0
		if rpcCheckPatterns[name] {
			total := ps.Sent + ps.ResponsesSuccess
			if total > 0 {
				errorPct = float64(ps.Errors) / float64(total) * 100.0
			}
		} else {
			total := ps.Sent + ps.Received
			if total > 0 {
				errorPct = float64(ps.Errors) / float64(total) * 100.0
			}
		}
		v.Checks[fmt.Sprintf("error_rate:%s", name)] = &CheckResult{
			Threshold: fmt.Sprintf("%.1f%%", cfg.Thresholds.MaxErrorRatePct),
			Actual:    fmt.Sprintf("%.3f%%", errorPct),
			Passed:    errorPct <= cfg.Thresholds.MaxErrorRatePct,
		}
	}

	// Memory stability
	{
		growthFactor := summary.Resources.MemoryGrowthFactor
		v.Checks["memory_stability"] = &CheckResult{
			Threshold: fmt.Sprintf("%.1fx", cfg.Thresholds.MaxMemoryGrowth),
			Actual:    fmt.Sprintf("%.2fx", growthFactor),
			Passed:    growthFactor <= cfg.Thresholds.MaxMemoryGrowth,
			Advisory:  !summary.BaselineFullWindow,
		}
	}

	// Memory trend -- advisory
	{
		growthFactor := summary.Resources.MemoryGrowthFactor
		warnThreshold := 1.0 + (cfg.Thresholds.MaxMemoryGrowth-1.0)*0.5
		v.Checks["memory_trend"] = &CheckResult{
			Threshold: fmt.Sprintf("%.1fx", warnThreshold),
			Actual:    fmt.Sprintf("%.2fx", growthFactor),
			Passed:    growthFactor <= warnThreshold,
			Advisory:  true,
		}
	}

	// Downtime -- max across patterns (not sum), since channels share a connection
	{
		maxDowntimePct := 0.0
		if summary.DurationSecs > 0 {
			for _, ps := range summary.Patterns {
				if !ps.Enabled {
					continue
				}
				pct := ps.DowntimeSeconds / summary.DurationSecs * 100.0
				if pct > maxDowntimePct {
					maxDowntimePct = pct
				}
			}
		}
		v.Checks["downtime"] = &CheckResult{
			Threshold: fmt.Sprintf("%.1f%%", cfg.Thresholds.MaxDowntimePct),
			Actual:    fmt.Sprintf("%.4f%%", maxDowntimePct),
			Passed:    maxDowntimePct <= cfg.Thresholds.MaxDowntimePct,
		}
	}

	// Warnings
	if !summary.AllPatternsEnabled {
		v.Warnings = append(v.Warnings, "Not all patterns enabled -- not valid for production certification")
	}

	// Determine overall result
	hardFailed := false
	advisoryFailed := false
	for _, cr := range v.Checks {
		if !cr.Passed {
			if cr.Advisory {
				advisoryFailed = true
			} else {
				hardFailed = true
			}
		}
	}

	if advisoryFailed {
		for checkName, cr := range v.Checks {
			if cr.Advisory && !cr.Passed {
				v.Warnings = append(v.Warnings, fmt.Sprintf("%s: %s (advisory threshold: %s)", checkName, cr.Actual, cr.Threshold))
			}
		}
	}

	if hardFailed {
		v.Result = "FAILED"
		v.Passed = false
	} else if advisoryFailed {
		v.Result = "PASSED_WITH_WARNINGS"
		v.Passed = true
	} else {
		v.Result = "PASSED"
		v.Passed = true
	}

	return v
}

// PrintConsole prints a formatted console report.
func PrintConsole(summary *Summary) {
	sep := strings.Repeat("=", 80)
	fmt.Println(sep)
	fmt.Printf("  KUBEMQ BURN-IN TEST REPORT -- Go SDK %s\n", summary.Version)
	fmt.Println(sep)
	fmt.Printf("  SDK:      %s\n", summary.SDK)
	fmt.Printf("  Version:  %s\n", summary.Version)
	fmt.Printf("  Mode:     %s\n", summary.Mode)
	fmt.Printf("  Broker:   %s\n", summary.BrokerAddress)
	fmt.Printf("  Started:  %s\n", summary.StartedAt.Format(time.RFC3339))
	fmt.Printf("  Ended:    %s\n", summary.EndedAt.Format(time.RFC3339))
	fmt.Printf("  Duration: %.1f seconds\n", summary.DurationSecs)
	fmt.Printf("  Status:   %s\n", summary.Status)
	fmt.Println()

	// Per-pattern stats (tabular format) with channel count
	fmt.Println("  PATTERN STATISTICS")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("  %-20s %10s %10s %6s %5s %5s %8s %8s\n",
		"PATTERN", "SENT", "RECV", "LOST", "DUP", "ERR", "P99(ms)", "P999(ms)")
	fmt.Println(strings.Repeat("-", 80))

	var totalSent, totalRecv, totalLost, totalDup, totalErr uint64
	for name, ps := range summary.Patterns {
		label := name
		if ps.Channels > 1 {
			label = fmt.Sprintf("%s (%dch)", name, ps.Channels)
		}
		fmt.Printf("  %-20s %10d %10d %6d %5d %5d %8.1f %8.1f\n",
			label, ps.Sent, ps.Received, ps.Lost, ps.Duplicated, ps.Errors,
			ps.LatencyP99MS, ps.LatencyP999MS)
		totalSent += ps.Sent
		totalRecv += ps.Received
		totalLost += ps.Lost
		totalDup += ps.Duplicated
		totalErr += ps.Errors
	}
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("  %-20s %10d %10d %6d %5d %5d\n",
		"TOTALS", totalSent, totalRecv, totalLost, totalDup, totalErr)
	fmt.Println()

	// Detailed per-pattern stats
	for name, ps := range summary.Patterns {
		chLabel := ""
		if ps.Channels > 1 {
			chLabel = fmt.Sprintf(" (%d channels)", ps.Channels)
		}
		fmt.Printf("  [%s]%s (status: %s)\n", name, chLabel, ps.Status)
		fmt.Printf("    Sent: %d  Received: %d  Lost: %d (%.4f%%)\n",
			ps.Sent, ps.Received, ps.Lost, ps.LossPct)
		fmt.Printf("    Duplicated: %d  Corrupted: %d  OutOfOrder: %d\n",
			ps.Duplicated, ps.Corrupted, ps.OutOfOrder)
		fmt.Printf("    Errors: %d  Reconnections: %d  Downtime: %.1fs\n",
			ps.Errors, ps.Reconnections, ps.DowntimeSeconds)
		fmt.Printf("    Latency P50: %.2fms  P95: %.2fms  P99: %.2fms  P99.9: %.2fms\n",
			ps.LatencyP50MS, ps.LatencyP95MS, ps.LatencyP99MS, ps.LatencyP999MS)
		fmt.Printf("    Throughput: %.1f msgs/sec (target: %d)\n",
			ps.AvgThroughput, ps.TargetRate)
		if ps.ResponsesSuccess > 0 || ps.ResponsesTimeout > 0 || ps.ResponsesError > 0 {
			fmt.Printf("    RPC: success=%d timeout=%d error=%d\n",
				ps.ResponsesSuccess, ps.ResponsesTimeout, ps.ResponsesError)
			fmt.Printf("    RPC Latency P50: %.2fms  P95: %.2fms  P99: %.2fms  P99.9: %.2fms\n",
				ps.RPCP50MS, ps.RPCP95MS, ps.RPCP99MS, ps.RPCP999MS)
			fmt.Printf("    RPC Throughput: %.1f rpc/sec\n", ps.AvgThroughputRPC)
		}
	}
	fmt.Println()

	// Resource stats
	fmt.Println("  RESOURCES")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("    Peak RSS: %.1f MB  Baseline RSS: %.1f MB  Growth: %.2fx\n",
		summary.Resources.PeakRSSMB, summary.Resources.BaselineRSSMB,
		summary.Resources.MemoryGrowthFactor)
	fmt.Printf("    Peak Workers: %d\n", summary.Resources.PeakWorkers)
	fmt.Println()

	// Verdict
	fmt.Println("  VERDICT")
	fmt.Println(strings.Repeat("-", 80))
	for name, cr := range summary.Verdict.Checks {
		status := "PASS"
		prefix := "+"
		if !cr.Passed {
			if cr.Advisory {
				status = "WARN"
				prefix = "~"
			} else {
				status = "FAIL"
				prefix = "!"
			}
		}
		fmt.Printf("    %s [%s] %-25s threshold: %-20s actual: %s\n",
			prefix, status, name, cr.Threshold, cr.Actual)
	}
	fmt.Println()

	fmt.Printf("  OVERALL: %s\n", summary.Verdict.Result)
	fmt.Println(sep)
}

// WriteJSON writes the summary as JSON to the given file path.
func WriteJSON(summary *Summary, path string) error {
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal summary: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write report file %s: %w", path, err)
	}
	return nil
}
