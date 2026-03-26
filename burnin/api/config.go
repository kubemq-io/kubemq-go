package api

import (
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/burnin/config"
)

var durationRegex = regexp.MustCompile(`^\d+[smhd]$`)

// RunConfig is the API request body for POST /run/start (v2 spec).
type RunConfig struct {
	Broker                 *BrokerOverride           `json:"broker,omitempty"`
	Mode                   string                    `json:"mode"`
	Duration               string                    `json:"duration"`
	RunID                  string                    `json:"run_id"`
	WarmupDuration         string                    `json:"warmup_duration"`
	StartingTimeoutSeconds int                       `json:"starting_timeout_seconds"`
	Patterns               map[string]*PatternConfig `json:"patterns"`
	Queue                  *QueueConfig              `json:"queue"`
	RPC                    *RPCConfig                `json:"rpc"`
	Message                *MessageConfig            `json:"message"`
	Thresholds             *GlobalThresholds         `json:"thresholds"`
	ForcedDisconnect       *ForcedDisconnectConfig   `json:"forced_disconnect"`
	Shutdown               *ShutdownConfig           `json:"shutdown"`
	Metrics                *MetricsConfig            `json:"metrics"`
	Warmup                 *WarmupConfig             `json:"warmup"`
}

// BrokerOverride allows overriding the broker address per-run via the API.
type BrokerOverride struct {
	Address string `json:"address,omitempty"`
}

// PatternConfig is the v2 per-pattern config from the API.
type PatternConfig struct {
	Enabled              *bool              `json:"enabled"`
	Channels             int                `json:"channels"`
	ProducersPerChannel  int                `json:"producers_per_channel"`
	ConsumersPerChannel  int                `json:"consumers_per_channel"`
	ConsumerGroup        *bool              `json:"consumer_group"`
	SendersPerChannel    int                `json:"senders_per_channel"`
	RespondersPerChannel int                `json:"responders_per_channel"`
	Rate                 int                `json:"rate"`
	Thresholds           *PatternThresholds `json:"thresholds"`
}

type PatternThresholds struct {
	MaxLossPct       *float64 `json:"max_loss_pct"`
	MaxP99LatencyMS  *float64 `json:"max_p99_latency_ms"`
	MaxP999LatencyMS *float64 `json:"max_p999_latency_ms"`
}

type QueueConfig struct {
	PollMaxMessages        int  `json:"poll_max_messages"`
	PollWaitTimeoutSeconds int  `json:"poll_wait_timeout_seconds"`
	AutoAck                bool `json:"auto_ack"`
	MaxDepth               int  `json:"max_depth"`
}

type RPCConfig struct {
	TimeoutMS int `json:"timeout_ms"`
}

type MessageConfig struct {
	SizeMode         string `json:"size_mode"`
	SizeBytes        int    `json:"size_bytes"`
	SizeDistribution string `json:"size_distribution"`
	ReorderWindow    int    `json:"reorder_window"`
}

type GlobalThresholds struct {
	MaxDuplicationPct     float64 `json:"max_duplication_pct"`
	MaxErrorRatePct       float64 `json:"max_error_rate_pct"`
	MaxMemoryGrowthFactor float64 `json:"max_memory_growth_factor"`
	MaxDowntimePct        float64 `json:"max_downtime_pct"`
	MinThroughputPct      float64 `json:"min_throughput_pct"`
	MaxDuration           string  `json:"max_duration"`
}

type ForcedDisconnectConfig struct {
	Interval string `json:"interval"`
	Duration string `json:"duration"`
}

type ShutdownConfig struct {
	DrainTimeoutSeconds int  `json:"drain_timeout_seconds"`
	CleanupChannels     bool `json:"cleanup_channels"`
}

type MetricsConfig struct {
	ReportInterval string `json:"report_interval"`
}

type WarmupConfig struct {
	MaxParallelChannels int    `json:"max_parallel_channels"`
	TimeoutPerChannelMs int    `json:"timeout_per_channel_ms"`
	WarmupDuration      string `json:"warmup_duration"`
}

var allPatternNames = []string{"events", "events_store", "queue_stream", "queue_simple", "commands", "queries"}
var rpcPatterns = map[string]bool{"commands": true, "queries": true}

// ParseRunConfig parses a JSON body into a RunConfig.
func ParseRunConfig(body []byte) (*RunConfig, error) {
	var rc RunConfig
	if err := json.Unmarshal(body, &rc); err != nil {
		return nil, err
	}
	return &rc, nil
}

// DetectV1Format checks the raw JSON body for v1 config fields.
// Returns error strings for each v1 field detected.
func DetectV1Format(body []byte) []string {
	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil
	}
	return config.DetectV1Config(raw)
}

// Validate checks all fields per v2 spec. Returns all errors collected.
func (rc *RunConfig) Validate() []string {
	var errs []string

	mode := rc.Mode
	if mode == "" {
		mode = "soak"
	}
	if rc.Mode != "" && rc.Mode != "soak" && rc.Mode != "benchmark" {
		errs = append(errs, fmt.Sprintf("mode: must be 'soak' or 'benchmark', got '%s'", rc.Mode))
	}

	if rc.Duration != "" && rc.Duration != "0" {
		if !durationRegex.MatchString(rc.Duration) {
			errs = append(errs, fmt.Sprintf("duration: must match \\d+[smhd] or '0', got '%s'", rc.Duration))
		}
	}

	if rc.StartingTimeoutSeconds < 0 {
		errs = append(errs, "starting_timeout_seconds: must be > 0")
	}

	enabledCount := 0
	for _, pname := range allPatternNames {
		pc := rc.getPattern(pname)
		enabled := true
		if pc != nil && pc.Enabled != nil {
			enabled = *pc.Enabled
		}
		if !enabled {
			continue
		}
		enabledCount++

		if pc == nil {
			continue
		}

		if pc.Rate < 0 {
			errs = append(errs, fmt.Sprintf("patterns.%s.rate: must be >= 0, got %d", pname, pc.Rate))
		}

		// Validate channels
		if pc.Channels != 0 && (pc.Channels < 1 || pc.Channels > 1000) {
			errs = append(errs, fmt.Sprintf("%s.channels: must be 1-1000, got %d", pname, pc.Channels))
		}

		if rpcPatterns[pname] {
			if pc.SendersPerChannel != 0 && pc.SendersPerChannel < 1 {
				errs = append(errs, fmt.Sprintf("%s.senders_per_channel: must be >= 1, got %d", pname, pc.SendersPerChannel))
			}
			if pc.RespondersPerChannel != 0 && pc.RespondersPerChannel < 1 {
				errs = append(errs, fmt.Sprintf("%s.responders_per_channel: must be >= 1, got %d", pname, pc.RespondersPerChannel))
			}
		} else {
			if pc.ProducersPerChannel != 0 && pc.ProducersPerChannel < 1 {
				errs = append(errs, fmt.Sprintf("%s.producers_per_channel: must be >= 1, got %d", pname, pc.ProducersPerChannel))
			}
			if pc.ConsumersPerChannel != 0 && pc.ConsumersPerChannel < 1 {
				errs = append(errs, fmt.Sprintf("%s.consumers_per_channel: must be >= 1, got %d", pname, pc.ConsumersPerChannel))
			}
		}

		if pc.Thresholds != nil {
			if pc.Thresholds.MaxLossPct != nil {
				v := *pc.Thresholds.MaxLossPct
				if v < 0 || v > 100 {
					errs = append(errs, fmt.Sprintf("patterns.%s.thresholds.max_loss_pct: must be 0-100, got %.1f", pname, v))
				}
			}
		}
	}

	if enabledCount == 0 {
		errs = append(errs, "no patterns enabled")
	}

	if rc.Message != nil {
		if rc.Message.SizeMode != "" && rc.Message.SizeMode != "fixed" && rc.Message.SizeMode != "distribution" {
			errs = append(errs, fmt.Sprintf("message.size_mode: must be 'fixed' or 'distribution', got '%s'", rc.Message.SizeMode))
		}
		if rc.Message.SizeBytes > 0 && rc.Message.SizeBytes < 64 {
			errs = append(errs, fmt.Sprintf("message.size_bytes: must be >= 64, got %d", rc.Message.SizeBytes))
		}
		if rc.Message.ReorderWindow > 0 && rc.Message.ReorderWindow < 100 {
			errs = append(errs, fmt.Sprintf("message.reorder_window: must be >= 100, got %d", rc.Message.ReorderWindow))
		}
	}

	if rc.Thresholds != nil {
		if rc.Thresholds.MaxDuplicationPct < 0 || rc.Thresholds.MaxDuplicationPct > 100 {
			errs = append(errs, fmt.Sprintf("thresholds.max_duplication_pct: must be 0-100, got %.1f", rc.Thresholds.MaxDuplicationPct))
		}
		if rc.Thresholds.MaxErrorRatePct < 0 || rc.Thresholds.MaxErrorRatePct > 100 {
			errs = append(errs, fmt.Sprintf("thresholds.max_error_rate_pct: must be 0-100, got %.1f", rc.Thresholds.MaxErrorRatePct))
		}
	}

	if rc.Shutdown != nil {
		if rc.Shutdown.DrainTimeoutSeconds < 0 {
			errs = append(errs, "shutdown.drain_timeout_seconds: must be > 0")
		}
	}

	return errs
}

func (rc *RunConfig) getPattern(name string) *PatternConfig {
	if rc.Patterns == nil {
		return nil
	}
	return rc.Patterns[name]
}

// ToInternalConfig translates the API v2 config to internal Config format.
func (rc *RunConfig) ToInternalConfig(startupCfg *config.Config) (*config.Config, error) {
	c := config.DefaultConfig()

	c.Broker.Address = startupCfg.Broker.Address
	c.Broker.ClientIDPrefix = startupCfg.Broker.ClientIDPrefix
	if rc.Broker != nil && rc.Broker.Address != "" {
		c.Broker.Address = rc.Broker.Address
	}
	c.Recovery = startupCfg.Recovery
	c.Logging = startupCfg.Logging
	c.Metrics.Port = startupCfg.Metrics.Port
	c.Output = startupCfg.Output
	c.CORS = startupCfg.CORS

	if rc.Mode != "" {
		c.Mode = rc.Mode
	}
	if rc.Duration != "" {
		c.DurationStr = rc.Duration
	}
	if rc.RunID != "" {
		c.RunID = rc.RunID
	} else {
		c.RunID = randomRunID()
	}
	if rc.WarmupDuration != "" {
		c.WarmupStr = rc.WarmupDuration
	}
	if rc.StartingTimeoutSeconds > 0 {
		c.StartingTimeoutSeconds = rc.StartingTimeoutSeconds
	}

	// Apply v2 patterns
	c.PatternEnabled = make(map[string]bool)
	c.PerPatternThresholds = make(map[string]*config.PerPatternThreshold)

	for _, pname := range allPatternNames {
		pc := rc.getPattern(pname)
		enabled := true
		if pc != nil && pc.Enabled != nil {
			enabled = *pc.Enabled
		}
		c.PatternEnabled[pname] = enabled

		// Get default pattern config
		defaultPC := config.DefaultPatternConfig(pname)

		// Get or create internal pattern config
		ipc := c.Patterns[pname]
		if ipc == nil {
			ipc = defaultPC
			c.Patterns[pname] = ipc
		}

		ipc.Enabled = enabled

		if pc == nil || !enabled {
			if !enabled {
				ipc.Enabled = false
			}
			continue
		}

		// Apply API overrides
		if pc.Channels > 0 {
			ipc.Channels = pc.Channels
		}
		if pc.Rate > 0 {
			ipc.Rate = pc.Rate
		}

		if rpcPatterns[pname] {
			if pc.SendersPerChannel > 0 {
				ipc.SendersPerChannel = pc.SendersPerChannel
			}
			if pc.RespondersPerChannel > 0 {
				ipc.RespondersPerChannel = pc.RespondersPerChannel
			}
		} else {
			if pc.ProducersPerChannel > 0 {
				ipc.ProducersPerChannel = pc.ProducersPerChannel
			}
			if pc.ConsumersPerChannel > 0 {
				ipc.ConsumersPerChannel = pc.ConsumersPerChannel
			}
		}

		if pc.ConsumerGroup != nil {
			ipc.ConsumerGroup = *pc.ConsumerGroup
		}

		if pc.Thresholds != nil {
			ppt := &config.PerPatternThreshold{}
			if pc.Thresholds.MaxLossPct != nil {
				ppt.MaxLossPct = *pc.Thresholds.MaxLossPct
				ppt.HasLossPct = true
			}
			if pc.Thresholds.MaxP99LatencyMS != nil {
				ppt.MaxP99LatencyMS = *pc.Thresholds.MaxP99LatencyMS
				ppt.HasP99 = true
			}
			if pc.Thresholds.MaxP999LatencyMS != nil {
				ppt.MaxP999LatencyMS = *pc.Thresholds.MaxP999LatencyMS
				ppt.HasP999 = true
			}
			c.PerPatternThresholds[pname] = ppt

			ipc.Thresholds = &config.ThresholdsConfig{}
			if pc.Thresholds.MaxLossPct != nil {
				v := *pc.Thresholds.MaxLossPct
				ipc.Thresholds.MaxLossPct = &v
			}
			if pc.Thresholds.MaxP99LatencyMS != nil {
				v := *pc.Thresholds.MaxP99LatencyMS
				ipc.Thresholds.MaxP99LatencyMS = &v
			}
			if pc.Thresholds.MaxP999LatencyMS != nil {
				v := *pc.Thresholds.MaxP999LatencyMS
				ipc.Thresholds.MaxP999LatencyMS = &v
			}
		}
	}

	if rc.Queue != nil {
		if rc.Queue.PollMaxMessages > 0 {
			c.Queue.PollMaxMessages = rc.Queue.PollMaxMessages
		}
		if rc.Queue.PollWaitTimeoutSeconds > 0 {
			c.Queue.PollWaitTimeoutSeconds = rc.Queue.PollWaitTimeoutSeconds
		}
		c.Queue.AutoAck = rc.Queue.AutoAck
		if rc.Queue.MaxDepth > 0 {
			c.Queue.MaxDepth = rc.Queue.MaxDepth
		}
	}

	if rc.RPC != nil && rc.RPC.TimeoutMS > 0 {
		c.RPC.TimeoutMS = rc.RPC.TimeoutMS
	}

	if rc.Message != nil {
		if rc.Message.SizeMode != "" {
			c.Message.SizeMode = rc.Message.SizeMode
		}
		if rc.Message.SizeBytes > 0 {
			c.Message.SizeBytes = rc.Message.SizeBytes
		}
		if rc.Message.SizeDistribution != "" {
			c.Message.SizeDistribution = rc.Message.SizeDistribution
		}
		if rc.Message.ReorderWindow > 0 {
			c.Message.ReorderWindow = rc.Message.ReorderWindow
		}
	}

	if rc.Thresholds != nil {
		if rc.Thresholds.MaxDuplicationPct > 0 {
			c.Thresholds.MaxDuplicationPct = rc.Thresholds.MaxDuplicationPct
		}
		if rc.Thresholds.MaxErrorRatePct > 0 {
			c.Thresholds.MaxErrorRatePct = rc.Thresholds.MaxErrorRatePct
		}
		if rc.Thresholds.MaxMemoryGrowthFactor > 0 {
			c.Thresholds.MaxMemoryGrowth = rc.Thresholds.MaxMemoryGrowthFactor
		}
		if rc.Thresholds.MaxDowntimePct > 0 {
			c.Thresholds.MaxDowntimePct = rc.Thresholds.MaxDowntimePct
		}
		if rc.Thresholds.MinThroughputPct > 0 {
			c.Thresholds.MinThroughputPct = rc.Thresholds.MinThroughputPct
		}
		if rc.Thresholds.MaxDuration != "" {
			c.Thresholds.MaxDurationStr = rc.Thresholds.MaxDuration
		}
	}

	if rc.ForcedDisconnect != nil {
		if rc.ForcedDisconnect.Interval != "" {
			c.ForcedDisconnect.IntervalStr = rc.ForcedDisconnect.Interval
		}
		if rc.ForcedDisconnect.Duration != "" {
			c.ForcedDisconnect.DurationStr = rc.ForcedDisconnect.Duration
		}
	}

	if rc.Shutdown != nil {
		if rc.Shutdown.DrainTimeoutSeconds > 0 {
			c.Shutdown.DrainTimeoutSeconds = rc.Shutdown.DrainTimeoutSeconds
		}
		c.Shutdown.CleanupChannels = rc.Shutdown.CleanupChannels
	}

	if rc.Metrics != nil {
		if rc.Metrics.ReportInterval != "" {
			c.Metrics.ReportStr = rc.Metrics.ReportInterval
		}
	}

	if rc.Warmup != nil {
		if rc.Warmup.MaxParallelChannels > 0 {
			c.Warmup.MaxParallelChannels = rc.Warmup.MaxParallelChannels
		}
		if rc.Warmup.TimeoutPerChannelMs > 0 {
			c.Warmup.TimeoutPerChannelMs = rc.Warmup.TimeoutPerChannelMs
		}
		if rc.Warmup.WarmupDuration != "" {
			c.Warmup.WarmupDuration = rc.Warmup.WarmupDuration
		}
	}

	if err := config.ParseDurationsPublic(c); err != nil {
		return nil, err
	}

	if c.WarmupStr == "" && c.Warmup.WarmupDuration == "" {
		if c.Mode == "benchmark" {
			c.WarmupDuration = 60 * time.Second
		} else {
			c.WarmupDuration = 0
		}
	}

	return c, nil
}

// EnabledPatternCount returns the number of enabled patterns.
func (rc *RunConfig) EnabledPatternCount() int {
	count := 0
	for _, pname := range allPatternNames {
		pc := rc.getPattern(pname)
		enabled := true
		if pc != nil && pc.Enabled != nil {
			enabled = *pc.Enabled
		}
		if enabled {
			count++
		}
	}
	return count
}

// TotalChannelCount returns the total number of channels across all enabled patterns.
func (rc *RunConfig) TotalChannelCount() int {
	total := 0
	for _, pname := range allPatternNames {
		pc := rc.getPattern(pname)
		enabled := true
		if pc != nil && pc.Enabled != nil {
			enabled = *pc.Enabled
		}
		if !enabled {
			continue
		}
		channels := 1
		if pc != nil && pc.Channels > 0 {
			channels = pc.Channels
		}
		total += channels
	}
	return total
}

func randomRunID() string {
	b := make([]byte, 4)
	_, _ = cryptorand.Read(b)
	return fmt.Sprintf("%08x", b)
}
