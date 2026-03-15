package report

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/burnin/config"
)

// Summary is the top-level report structure written as JSON and returned by the /summary endpoint.
type Summary struct {
	SDK           string                   `json:"sdk"`
	Version       string                   `json:"version"`
	Mode          string                   `json:"mode"`
	BrokerAddress string                   `json:"broker_address"`
	StartedAt     time.Time                `json:"started_at"`
	EndedAt       time.Time                `json:"ended_at"`
	DurationSecs  float64                  `json:"duration_seconds"`
	Status        string                   `json:"status"`
	Patterns      map[string]*PatternStats `json:"patterns"`
	Resources     ResourceStats            `json:"resources"`
	Verdict       Verdict                  `json:"verdict"`
}

// PatternStats holds per-pattern statistics.
type PatternStats struct {
	Status           string  `json:"status"`
	Sent             uint64  `json:"sent"`
	Received         uint64  `json:"received"`
	Lost             uint64  `json:"lost"`
	Duplicated       uint64  `json:"duplicated"`
	Corrupted        uint64  `json:"corrupted"`
	OutOfOrder       uint64  `json:"out_of_order"`
	LossPct          float64 `json:"loss_pct"`
	Errors           uint64  `json:"errors"`
	Reconnections    uint64  `json:"reconnections"`
	DowntimeSeconds  float64 `json:"downtime_seconds"`
	LatencyP50MS     float64 `json:"latency_p50_ms"`
	LatencyP95MS     float64 `json:"latency_p95_ms"`
	LatencyP99MS     float64 `json:"latency_p99_ms"`
	LatencyP999MS    float64 `json:"latency_p999_ms"`
	AvgThroughput    float64 `json:"avg_throughput_msgs_sec"`
	PeakThroughput   float64 `json:"peak_throughput_msgs_sec,omitempty"`
	TargetRate       int     `json:"target_rate"`
	ResponsesSuccess uint64  `json:"responses_success,omitempty"`
	ResponsesTimeout uint64  `json:"responses_timeout,omitempty"`
	ResponsesError   uint64  `json:"responses_error,omitempty"`
	RPCP50MS         float64 `json:"rpc_p50_ms,omitempty"`
	RPCP95MS         float64 `json:"rpc_p95_ms,omitempty"`
	RPCP99MS         float64 `json:"rpc_p99_ms,omitempty"`
	RPCP999MS        float64 `json:"rpc_p999_ms,omitempty"`
	AvgThroughputRPC float64 `json:"avg_throughput_rpc_sec,omitempty"`
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
	Result string                  `json:"result"` // PASSED, PASSED_WITH_WARNINGS, FAILED
	Passed bool                    `json:"passed"` // convenience: true if result != FAILED
	Checks map[string]*CheckResult `json:"checks"`
}

// CheckResult is the outcome of a single check.
type CheckResult struct {
	Passed    bool   `json:"passed"`
	Threshold string `json:"threshold"`
	Actual    string `json:"actual"`
	Advisory  bool   `json:"advisory,omitempty"`
}

// persistentPatterns are the patterns that must have zero (or near-zero) loss.
var persistentPatterns = map[string]bool{
	"events_store": true,
	"queue_stream": true,
	"queue_simple": true,
}

// Generate runs all 10 verdict checks and returns the Verdict.
func Generate(summary *Summary, cfg *config.Config) *Verdict {
	v := &Verdict{
		Checks: make(map[string]*CheckResult),
	}

	// 1. message_loss_persistent: max loss% across events_store, queue_stream, queue_simple
	{
		maxLoss := 0.0
		for name, ps := range summary.Patterns {
			if persistentPatterns[name] && ps.LossPct > maxLoss {
				maxLoss = ps.LossPct
			}
		}
		cr := &CheckResult{
			Threshold: fmt.Sprintf("≤ %.2f%%", cfg.Thresholds.MaxLossPct),
			Actual:    fmt.Sprintf("%.4f%%", maxLoss),
			Passed:    maxLoss <= cfg.Thresholds.MaxLossPct,
		}
		v.Checks["message_loss_persistent"] = cr
	}

	// 2. message_loss_events: events loss%
	{
		eventsLoss := 0.0
		if ps, ok := summary.Patterns["events"]; ok {
			eventsLoss = ps.LossPct
		}
		cr := &CheckResult{
			Threshold: fmt.Sprintf("≤ %.2f%%", cfg.Thresholds.MaxEventsLossPct),
			Actual:    fmt.Sprintf("%.4f%%", eventsLoss),
			Passed:    eventsLoss <= cfg.Thresholds.MaxEventsLossPct,
		}
		v.Checks["message_loss_events"] = cr
	}

	// 3. duplication: max dup% across all patterns
	{
		maxDup := 0.0
		for _, ps := range summary.Patterns {
			if ps.Sent == 0 {
				continue
			}
			dupPct := float64(ps.Duplicated) / float64(ps.Sent) * 100.0
			if dupPct > maxDup {
				maxDup = dupPct
			}
		}
		cr := &CheckResult{
			Threshold: fmt.Sprintf("≤ %.2f%%", cfg.Thresholds.MaxDuplicationPct),
			Actual:    fmt.Sprintf("%.4f%%", maxDup),
			Passed:    maxDup <= cfg.Thresholds.MaxDuplicationPct,
		}
		v.Checks["duplication"] = cr
	}

	// 4. corruption: sum corrupted across all patterns == 0
	{
		var totalCorrupted uint64
		for _, ps := range summary.Patterns {
			totalCorrupted += ps.Corrupted
		}
		cr := &CheckResult{
			Threshold: "0",
			Actual:    fmt.Sprintf("%d", totalCorrupted),
			Passed:    totalCorrupted == 0,
		}
		v.Checks["corruption"] = cr
	}

	// 5. p99_latency: max P99 latency across all patterns
	{
		maxP99 := 0.0
		for _, ps := range summary.Patterns {
			if ps.LatencyP99MS > maxP99 {
				maxP99 = ps.LatencyP99MS
			}
		}
		cr := &CheckResult{
			Threshold: fmt.Sprintf("≤ %.1f ms", cfg.Thresholds.MaxP99LatencyMS),
			Actual:    fmt.Sprintf("%.2f ms", maxP99),
			Passed:    maxP99 <= cfg.Thresholds.MaxP99LatencyMS,
		}
		v.Checks["p99_latency"] = cr
	}

	// 6. p999_latency: max P999 latency across all patterns
	{
		maxP999 := 0.0
		for _, ps := range summary.Patterns {
			if ps.LatencyP999MS > maxP999 {
				maxP999 = ps.LatencyP999MS
			}
		}
		cr := &CheckResult{
			Threshold: fmt.Sprintf("≤ %.1f ms", cfg.Thresholds.MaxP999LatencyMS),
			Actual:    fmt.Sprintf("%.2f ms", maxP999),
			Passed:    maxP999 <= cfg.Thresholds.MaxP999LatencyMS,
		}
		v.Checks["p999_latency"] = cr
	}

	// 7. throughput: min throughput% >= MinThroughputPct (soak mode only)
	{
		if cfg.Mode == "soak" {
			minPct := -1.0
			for _, ps := range summary.Patterns {
				if ps.TargetRate == 0 {
					continue
				}
				pct := ps.AvgThroughput / float64(ps.TargetRate) * 100.0
				if minPct < 0 || pct < minPct {
					minPct = pct
				}
			}
			if minPct < 0 {
				minPct = 100.0 // no patterns with target rate
			}
			cr := &CheckResult{
				Threshold: fmt.Sprintf("≥ %.1f%%", cfg.Thresholds.MinThroughputPct),
				Actual:    fmt.Sprintf("%.2f%%", minPct),
				Passed:    minPct >= cfg.Thresholds.MinThroughputPct,
			}
			v.Checks["throughput"] = cr
		} else {
			// Benchmark mode: skip this check (auto-pass)
			v.Checks["throughput"] = &CheckResult{
				Passed:    true,
				Threshold: "skipped (benchmark mode)",
				Actual:    "N/A",
			}
		}
	}

	// 8. error_rate: error rate% across all patterns
	{
		var totalErrors uint64
		var totalSent uint64
		var totalReceived uint64
		for _, ps := range summary.Patterns {
			totalErrors += ps.Errors
			totalSent += ps.Sent
			totalReceived += ps.Received
		}
		errorPct := 0.0
		if totalSent+totalReceived > 0 {
			errorPct = float64(totalErrors) / float64(totalSent+totalReceived) * 100.0
		}
		cr := &CheckResult{
			Threshold: fmt.Sprintf("≤ %.2f%%", cfg.Thresholds.MaxErrorRatePct),
			Actual:    fmt.Sprintf("%.4f%%", errorPct),
			Passed:    errorPct <= cfg.Thresholds.MaxErrorRatePct,
		}
		v.Checks["error_rate"] = cr
	}

	// 9. memory_stability: peakRSS / baselineRSS <= MaxMemoryGrowth
	{
		growthFactor := summary.Resources.MemoryGrowthFactor
		cr := &CheckResult{
			Threshold: fmt.Sprintf("≤ %.1fx", cfg.Thresholds.MaxMemoryGrowth),
			Actual:    fmt.Sprintf("%.2fx", growthFactor),
			Passed:    growthFactor <= cfg.Thresholds.MaxMemoryGrowth,
		}
		v.Checks["memory_stability"] = cr
	}

	// 10. downtime: max downtime% across all patterns
	{
		maxDowntimePct := 0.0
		if summary.DurationSecs > 0 {
			for _, ps := range summary.Patterns {
				downtimeSecs := ps.DowntimeSeconds
				pct := downtimeSecs / summary.DurationSecs * 100.0
				if pct > maxDowntimePct {
					maxDowntimePct = pct
				}
			}
		}
		cr := &CheckResult{
			Threshold: fmt.Sprintf("≤ %.1f%%", cfg.Thresholds.MaxDowntimePct),
			Actual:    fmt.Sprintf("%.4f%%", maxDowntimePct),
			Passed:    maxDowntimePct <= cfg.Thresholds.MaxDowntimePct,
		}
		v.Checks["downtime"] = cr
	}

	// Advisory check: memory_trend — warn if growth > 50% of threshold
	{
		growthFactor := summary.Resources.MemoryGrowthFactor
		warnThreshold := 1.0 + (cfg.Thresholds.MaxMemoryGrowth-1.0)*0.5
		cr := &CheckResult{
			Threshold: fmt.Sprintf("> %.1fx", warnThreshold),
			Actual:    fmt.Sprintf("%.2fx", growthFactor),
			Passed:    growthFactor <= warnThreshold,
			Advisory:  true,
		}
		v.Checks["memory_trend"] = cr
	}

	// Determine overall result from all checks
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

	// Per-pattern stats (tabular format)
	fmt.Println("  PATTERN STATISTICS")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("  %-16s %10s %10s %6s %5s %5s %8s %8s\n",
		"PATTERN", "SENT", "RECV", "LOST", "DUP", "ERR", "P99(ms)", "P999(ms)")
	fmt.Println(strings.Repeat("-", 80))

	var totalSent, totalRecv, totalLost, totalDup, totalErr uint64
	for name, ps := range summary.Patterns {
		fmt.Printf("  %-16s %10d %10d %6d %5d %5d %8.1f %8.1f\n",
			name, ps.Sent, ps.Received, ps.Lost, ps.Duplicated, ps.Errors,
			ps.LatencyP99MS, ps.LatencyP999MS)
		totalSent += ps.Sent
		totalRecv += ps.Received
		totalLost += ps.Lost
		totalDup += ps.Duplicated
		totalErr += ps.Errors
	}
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("  %-16s %10d %10d %6d %5d %5d\n",
		"TOTALS", totalSent, totalRecv, totalLost, totalDup, totalErr)
	fmt.Println()

	// Detailed per-pattern stats
	for name, ps := range summary.Patterns {
		fmt.Printf("  [%s] (status: %s)\n", name, ps.Status)
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
