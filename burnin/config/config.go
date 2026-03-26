package config

import (
	"bytes"
	cryptorand "crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const ConfigVersion = "2"

// ThresholdsConfig holds per-pattern threshold overrides.
type ThresholdsConfig struct {
	MaxLossPct       *float64 `yaml:"max_loss_pct" json:"max_loss_pct,omitempty"`
	MaxP99LatencyMS  *float64 `yaml:"max_p99_latency_ms" json:"max_p99_latency_ms,omitempty"`
	MaxP999LatencyMS *float64 `yaml:"max_p999_latency_ms" json:"max_p999_latency_ms,omitempty"`
}

// PatternConfig holds per-pattern configuration for v2.
type PatternConfig struct {
	Enabled              bool              `yaml:"enabled" json:"enabled"`
	Channels             int               `yaml:"channels" json:"channels"`
	ProducersPerChannel  int               `yaml:"producers_per_channel" json:"producers_per_channel"`
	ConsumersPerChannel  int               `yaml:"consumers_per_channel" json:"consumers_per_channel"`
	ConsumerGroup        bool              `yaml:"consumer_group" json:"consumer_group"`
	SendersPerChannel    int               `yaml:"senders_per_channel" json:"senders_per_channel"`
	RespondersPerChannel int               `yaml:"responders_per_channel" json:"responders_per_channel"`
	Rate                 int               `yaml:"rate" json:"rate"`
	Thresholds           *ThresholdsConfig `yaml:"thresholds,omitempty" json:"thresholds,omitempty"`
}

// WarmupConfig holds warmup settings.
type WarmupConfig struct {
	MaxParallelChannels int    `yaml:"max_parallel_channels" json:"max_parallel_channels"`
	TimeoutPerChannelMs int    `yaml:"timeout_per_channel_ms" json:"timeout_per_channel_ms"`
	WarmupDuration      string `yaml:"warmup_duration" json:"warmup_duration"`
}

// PerPatternThreshold holds per-pattern threshold overrides from the API config.
type PerPatternThreshold struct {
	MaxLossPct       float64
	MaxP99LatencyMS  float64
	MaxP999LatencyMS float64
	HasLossPct       bool
	HasP99           bool
	HasP999          bool
}

type Config struct {
	Version  string   `yaml:"version"`
	Warnings []string `yaml:"-"` // populated during Load for unknown YAML fields, etc.

	Broker struct {
		Address        string `yaml:"address"`
		ClientIDPrefix string `yaml:"client_id_prefix"`
	} `yaml:"broker"`

	Mode           string        `yaml:"mode"`
	Duration       time.Duration `yaml:"-"`
	DurationStr    string        `yaml:"duration"`
	RunID          string        `yaml:"run_id"`
	WarmupDuration time.Duration `yaml:"-"`
	WarmupStr      string        `yaml:"warmup_duration"`

	// v2 patterns config
	Patterns map[string]*PatternConfig `yaml:"patterns" json:"patterns"`

	// v2 warmup config
	Warmup WarmupConfig `yaml:"warmup" json:"warmup"`

	Queue struct {
		PollMaxMessages        int  `yaml:"poll_max_messages"`
		PollWaitTimeoutSeconds int  `yaml:"poll_wait_timeout_seconds"`
		AutoAck                bool `yaml:"auto_ack"`
		MaxDepth               int  `yaml:"max_depth"`
	} `yaml:"queue"`

	RPC struct {
		TimeoutMS int `yaml:"timeout_ms"`
	} `yaml:"rpc"`

	Message struct {
		SizeMode         string `yaml:"size_mode"`
		SizeBytes        int    `yaml:"size_bytes"`
		SizeDistribution string `yaml:"size_distribution"`
		ReorderWindow    int    `yaml:"reorder_window"`
	} `yaml:"message"`

	Metrics struct {
		Port           int           `yaml:"port"`
		ReportInterval time.Duration `yaml:"-"`
		ReportStr      string        `yaml:"report_interval"`
	} `yaml:"metrics"`

	Logging struct {
		Format string `yaml:"format"`
		Level  string `yaml:"level"`
	} `yaml:"logging"`

	ForcedDisconnect struct {
		Interval    time.Duration `yaml:"-"`
		IntervalStr string        `yaml:"interval"`
		Duration    time.Duration `yaml:"-"`
		DurationStr string        `yaml:"duration"`
	} `yaml:"forced_disconnect"`

	Recovery struct {
		ReconnectInterval    time.Duration `yaml:"-"`
		ReconnectIntervalStr string        `yaml:"reconnect_interval"`
		ReconnectMaxInterval time.Duration `yaml:"-"`
		ReconnectMaxStr      string        `yaml:"reconnect_max_interval"`
		ReconnectMultiplier  float64       `yaml:"reconnect_multiplier"`
	} `yaml:"recovery"`

	Shutdown struct {
		DrainTimeoutSeconds int  `yaml:"drain_timeout_seconds"`
		CleanupChannels     bool `yaml:"cleanup_channels"`
	} `yaml:"shutdown"`

	Output struct {
		ReportFile string `yaml:"report_file"`
		SDKVersion string `yaml:"sdk_version"`
	} `yaml:"output"`

	CORS struct {
		Origins string `yaml:"origins"`
	} `yaml:"cors"`

	StartingTimeoutSeconds int                             `yaml:"-"`
	PatternEnabled         map[string]bool                 `yaml:"-"`
	PerPatternThresholds   map[string]*PerPatternThreshold `yaml:"-"`

	Thresholds struct {
		MaxLossPct        float64       `yaml:"max_loss_pct"`
		MaxEventsLossPct  float64       `yaml:"max_events_loss_pct"`
		MaxDuplicationPct float64       `yaml:"max_duplication_pct"`
		MaxP99LatencyMS   float64       `yaml:"max_p99_latency_ms"`
		MaxP999LatencyMS  float64       `yaml:"max_p999_latency_ms"`
		MinThroughputPct  float64       `yaml:"min_throughput_pct"`
		MaxErrorRatePct   float64       `yaml:"max_error_rate_pct"`
		MaxMemoryGrowth   float64       `yaml:"max_memory_growth_factor"`
		MaxDowntimePct    float64       `yaml:"max_downtime_pct"`
		MaxDuration       time.Duration `yaml:"-"`
		MaxDurationStr    string        `yaml:"max_duration"`
	} `yaml:"thresholds"`
}

// AllPatternNames returns the list of all 6 pattern names.
var AllPatternNames = []string{"events", "events_store", "queue_stream", "queue_simple", "commands", "queries"}

// IsRPCPattern returns true for commands and queries.
func IsRPCPattern(p string) bool {
	return p == "commands" || p == "queries"
}

// IsPubSubPattern returns true for events and events_store.
func IsPubSubPattern(p string) bool {
	return p == "events" || p == "events_store"
}

// DefaultPatternConfig returns default pattern config for a given pattern.
func DefaultPatternConfig(pattern string) *PatternConfig {
	pc := &PatternConfig{
		Enabled:              true,
		Channels:             1,
		ProducersPerChannel:  1,
		ConsumersPerChannel:  1,
		ConsumerGroup:        false,
		SendersPerChannel:    1,
		RespondersPerChannel: 1,
		Rate:                 100,
	}
	switch pattern {
	case "queue_stream":
		pc.Rate = 50
	case "queue_simple":
		pc.Rate = 50
	case "commands":
		pc.Rate = 20
	case "queries":
		pc.Rate = 20
	}
	return pc
}

func DefaultConfig() *Config {
	c := &Config{}
	c.Version = ConfigVersion
	c.Broker.Address = "localhost:50000"
	c.Broker.ClientIDPrefix = "burnin-go"
	c.Mode = "soak"
	c.Duration = 1 * time.Hour
	c.DurationStr = "1h"
	c.RunID = ""
	c.WarmupStr = ""

	// Initialize v2 patterns with defaults
	c.Patterns = make(map[string]*PatternConfig)
	for _, p := range AllPatternNames {
		c.Patterns[p] = DefaultPatternConfig(p)
	}

	// Warmup defaults
	c.Warmup = WarmupConfig{
		MaxParallelChannels: 10,
		TimeoutPerChannelMs: 5000,
		WarmupDuration:      "",
	}

	c.Queue.PollMaxMessages = 10
	c.Queue.PollWaitTimeoutSeconds = 5
	c.Queue.AutoAck = false
	c.Queue.MaxDepth = 1_000_000

	c.RPC.TimeoutMS = 5000

	c.Message.SizeMode = "fixed"
	c.Message.SizeBytes = 1024
	c.Message.SizeDistribution = "256:80,4096:15,65536:5"
	c.Message.ReorderWindow = 10_000

	c.Metrics.Port = 8888
	c.Metrics.ReportInterval = 30 * time.Second
	c.Metrics.ReportStr = "30s"

	c.Logging.Format = "text"
	c.Logging.Level = "info"

	c.ForcedDisconnect.IntervalStr = "0"
	c.ForcedDisconnect.DurationStr = "5s"
	c.ForcedDisconnect.Duration = 5 * time.Second

	c.Recovery.ReconnectInterval = 1 * time.Second
	c.Recovery.ReconnectIntervalStr = "1s"
	c.Recovery.ReconnectMaxInterval = 30 * time.Second
	c.Recovery.ReconnectMaxStr = "30s"
	c.Recovery.ReconnectMultiplier = 2.0

	c.Shutdown.DrainTimeoutSeconds = 10
	c.Shutdown.CleanupChannels = true

	c.Thresholds.MaxLossPct = 0.0
	c.Thresholds.MaxEventsLossPct = 5.0
	c.Thresholds.MaxDuplicationPct = 0.1
	c.Thresholds.MaxP99LatencyMS = 1000
	c.Thresholds.MaxP999LatencyMS = 5000
	c.Thresholds.MinThroughputPct = 90
	c.Thresholds.MaxErrorRatePct = 1.0
	c.Thresholds.MaxMemoryGrowth = 2.0
	c.Thresholds.MaxDowntimePct = 10
	c.Thresholds.MaxDuration = 168 * time.Hour
	c.Thresholds.MaxDurationStr = "168h"

	c.CORS.Origins = "*"
	c.StartingTimeoutSeconds = 60
	c.PatternEnabled = map[string]bool{
		"events": true, "events_store": true, "queue_stream": true,
		"queue_simple": true, "commands": true, "queries": true,
	}

	return c
}

func Load(path string) (*Config, error) {
	c := DefaultConfig()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config file %s: %w", path, err)
		}

		// Try strict decode first to detect unknown YAML keys
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		decoder.KnownFields(true)
		if err := decoder.Decode(c); err != nil {
			// Check if it's just unknown fields (not a hard parse error)
			c.Warnings = append(c.Warnings, fmt.Sprintf("config has unknown fields: %v", err))
			// Re-decode permissively
			c2 := DefaultConfig()
			if err2 := yaml.Unmarshal(data, c2); err2 != nil {
				return nil, fmt.Errorf("parse config file %s: %w", path, err2)
			}
			*c = *c2
			c.Warnings = append(c.Warnings, fmt.Sprintf("config has unknown fields: %v", err))
		}
	}

	// Environment variable override for broker address
	if envAddr := os.Getenv("KUBEMQ_BROKER_ADDRESS"); envAddr != "" {
		c.Broker.Address = envAddr
	}

	if err := parseDurations(c); err != nil {
		return nil, err
	}

	if c.RunID == "" {
		c.RunID = RandomRunID()
	}

	// Ensure patterns map is populated
	if c.Patterns == nil {
		c.Patterns = make(map[string]*PatternConfig)
	}
	for _, p := range AllPatternNames {
		if _, ok := c.Patterns[p]; !ok {
			c.Patterns[p] = DefaultPatternConfig(p)
		}
	}

	// Build PatternEnabled from patterns
	c.PatternEnabled = make(map[string]bool)
	for _, p := range AllPatternNames {
		if pc, ok := c.Patterns[p]; ok {
			c.PatternEnabled[p] = pc.Enabled
		} else {
			c.PatternEnabled[p] = true
		}
	}

	// Build PerPatternThresholds from patterns
	c.PerPatternThresholds = make(map[string]*PerPatternThreshold)
	for _, p := range AllPatternNames {
		if pc, ok := c.Patterns[p]; ok && pc.Thresholds != nil {
			ppt := &PerPatternThreshold{}
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
			c.PerPatternThresholds[p] = ppt
		}
	}

	// Apply mode-dependent warmup default
	if c.WarmupStr == "" && c.Warmup.WarmupDuration == "" {
		if c.Mode == "benchmark" {
			c.WarmupDuration = 60 * time.Second
		} else {
			c.WarmupDuration = 0
		}
	} else if c.Warmup.WarmupDuration != "" && c.WarmupStr == "" {
		c.WarmupStr = c.Warmup.WarmupDuration
		d, err := parseDuration(c.WarmupStr)
		if err == nil {
			c.WarmupDuration = d
		}
	}

	return c, nil
}

func FindConfigFile(cliPath string) string {
	if cliPath != "" {
		return cliPath
	}
	if _, err := os.Stat("./burnin-config.yaml"); err == nil {
		return "./burnin-config.yaml"
	}
	if _, err := os.Stat("/etc/burnin/config.yaml"); err == nil {
		return "/etc/burnin/config.yaml"
	}
	return ""
}

// GetPatternRate returns the rate for a pattern from the v2 config.
func (c *Config) GetPatternRate(pattern string) int {
	if pc, ok := c.Patterns[pattern]; ok {
		return pc.Rate
	}
	return 100
}

// GetPatternChannels returns the number of channels for a pattern.
func (c *Config) GetPatternChannels(pattern string) int {
	if pc, ok := c.Patterns[pattern]; ok {
		return pc.Channels
	}
	return 1
}

// GetPatternConfig returns the PatternConfig for a pattern, or nil.
func (c *Config) GetPatternConfig(pattern string) *PatternConfig {
	if c.Patterns == nil {
		return nil
	}
	return c.Patterns[pattern]
}

func (c *Config) Validate() []error {
	var errs []error

	if c.Mode != "soak" && c.Mode != "benchmark" {
		errs = append(errs, fmt.Errorf("mode must be 'soak' or 'benchmark', got %q", c.Mode))
	}

	if c.Duration < 0 {
		errs = append(errs, fmt.Errorf("duration must be >= 0"))
	}

	if c.Mode == "soak" && c.Duration <= 0 {
		// Allow 0 for infinite soak
	}

	if c.Broker.Address == "" {
		errs = append(errs, fmt.Errorf("broker address is required"))
	}

	// Validate patterns
	enabledCount := 0
	totalWorkers := 0
	for _, pname := range AllPatternNames {
		pc := c.GetPatternConfig(pname)
		if pc == nil {
			continue
		}
		if !pc.Enabled {
			continue
		}
		enabledCount++

		// Validate channels
		if pc.Channels < 1 || pc.Channels > 1000 {
			errs = append(errs, fmt.Errorf("%s.channels: must be 1-1000, got %d", pname, pc.Channels))
		}

		// Validate rate
		if pc.Rate < 0 {
			errs = append(errs, fmt.Errorf("%s.rate: must be >= 0, got %d", pname, pc.Rate))
		}

		if IsRPCPattern(pname) {
			if pc.SendersPerChannel < 1 {
				errs = append(errs, fmt.Errorf("%s.senders_per_channel: must be >= 1, got %d", pname, pc.SendersPerChannel))
			}
			if pc.RespondersPerChannel < 1 {
				errs = append(errs, fmt.Errorf("%s.responders_per_channel: must be >= 1, got %d", pname, pc.RespondersPerChannel))
			}
			totalWorkers += pc.Channels * (pc.SendersPerChannel + pc.RespondersPerChannel)
		} else {
			if pc.ProducersPerChannel < 1 {
				errs = append(errs, fmt.Errorf("%s.producers_per_channel: must be >= 1, got %d", pname, pc.ProducersPerChannel))
			}
			if pc.ConsumersPerChannel < 1 {
				errs = append(errs, fmt.Errorf("%s.consumers_per_channel: must be >= 1, got %d", pname, pc.ConsumersPerChannel))
			}
			totalWorkers += pc.Channels * (pc.ProducersPerChannel + pc.ConsumersPerChannel)
		}

		// Soft limit warnings
		if !IsRPCPattern(pname) {
			if pc.ProducersPerChannel > 100 {
				errs = append(errs, fmt.Errorf("WARNING: %s.producers_per_channel: %d exceeds recommended max 100", pname, pc.ProducersPerChannel))
			}
			if pc.ConsumersPerChannel > 100 {
				errs = append(errs, fmt.Errorf("WARNING: %s.consumers_per_channel: %d exceeds recommended max 100", pname, pc.ConsumersPerChannel))
			}
		}

		// Validate per-pattern thresholds
		if pc.Thresholds != nil {
			if pc.Thresholds.MaxLossPct != nil {
				v := *pc.Thresholds.MaxLossPct
				if v < 0 || v > 100 {
					errs = append(errs, fmt.Errorf("%s.thresholds.max_loss_pct: must be 0-100, got %.1f", pname, v))
				}
			}
			if pc.Thresholds.MaxP99LatencyMS != nil {
				if *pc.Thresholds.MaxP99LatencyMS <= 0 {
					errs = append(errs, fmt.Errorf("%s.thresholds.max_p99_latency_ms: must be > 0", pname))
				}
			}
			if pc.Thresholds.MaxP999LatencyMS != nil {
				if *pc.Thresholds.MaxP999LatencyMS <= 0 {
					errs = append(errs, fmt.Errorf("%s.thresholds.max_p999_latency_ms: must be > 0", pname))
				}
			}
		}
	}

	if enabledCount == 0 {
		errs = append(errs, fmt.Errorf("at least one pattern must be enabled"))
	}

	if c.Queue.PollMaxMessages < 1 {
		errs = append(errs, fmt.Errorf("poll_max_messages must be >= 1"))
	}

	if c.RPC.TimeoutMS <= 0 {
		errs = append(errs, fmt.Errorf("rpc timeout_ms must be > 0"))
	}

	if c.Message.SizeMode != "fixed" && c.Message.SizeMode != "distribution" {
		errs = append(errs, fmt.Errorf("message size_mode must be 'fixed' or 'distribution', got %q", c.Message.SizeMode))
	}

	if c.Message.SizeMode == "fixed" && c.Message.SizeBytes < 64 {
		errs = append(errs, fmt.Errorf("message.size_bytes: must be >= 64, got %d", c.Message.SizeBytes))
	}

	if c.Message.ReorderWindow < 100 {
		errs = append(errs, fmt.Errorf("reorder_window must be >= 100, got %d", c.Message.ReorderWindow))
	}

	if c.Metrics.Port < 1 || c.Metrics.Port > 65535 {
		errs = append(errs, fmt.Errorf("api.port: must be 1-65535, got %d", c.Metrics.Port))
	}

	if c.Shutdown.DrainTimeoutSeconds <= 0 {
		errs = append(errs, fmt.Errorf("shutdown.drain_timeout_seconds: must be > 0, got %d", c.Shutdown.DrainTimeoutSeconds))
	}

	if c.Thresholds.MaxLossPct < 0 || c.Thresholds.MaxLossPct > 100 {
		errs = append(errs, fmt.Errorf("max_loss_pct must be 0-100"))
	}
	if c.Thresholds.MaxEventsLossPct < 0 || c.Thresholds.MaxEventsLossPct > 100 {
		errs = append(errs, fmt.Errorf("max_events_loss_pct must be 0-100"))
	}
	if c.Thresholds.MaxDuplicationPct < 0 || c.Thresholds.MaxDuplicationPct > 100 {
		errs = append(errs, fmt.Errorf("max_duplication_pct must be 0-100"))
	}
	if c.Thresholds.MaxErrorRatePct < 0 || c.Thresholds.MaxErrorRatePct > 100 {
		errs = append(errs, fmt.Errorf("max_error_rate_pct must be 0-100"))
	}
	if c.Thresholds.MaxP99LatencyMS <= 0 {
		errs = append(errs, fmt.Errorf("max_p99_latency_ms must be > 0"))
	}
	if c.Thresholds.MaxP999LatencyMS <= 0 {
		errs = append(errs, fmt.Errorf("max_p999_latency_ms must be > 0"))
	}
	if c.Thresholds.MinThroughputPct <= 0 || c.Thresholds.MinThroughputPct > 100 {
		errs = append(errs, fmt.Errorf("min_throughput_pct must be > 0 and <= 100"))
	}
	if c.Thresholds.MaxMemoryGrowth < 1.0 {
		errs = append(errs, fmt.Errorf("max_memory_growth_factor must be >= 1.0"))
	}
	if c.Thresholds.MaxDowntimePct < 0 || c.Thresholds.MaxDowntimePct > 100 {
		errs = append(errs, fmt.Errorf("max_downtime_pct must be 0-100"))
	}
	if c.Recovery.ReconnectMultiplier < 1.0 {
		errs = append(errs, fmt.Errorf("reconnect_multiplier must be >= 1.0, got %f", c.Recovery.ReconnectMultiplier))
	}

	// Resource guard warnings
	if totalWorkers > 500 {
		errs = append(errs, fmt.Errorf("WARNING: high worker count: %d -- may impact system resources", totalWorkers))
	}

	// Estimate memory
	estMemoryMB := float64(totalWorkers) * (float64(c.Message.ReorderWindow)*8/1024/1024 + 0.5)
	if estMemoryMB > 4096 {
		errs = append(errs, fmt.Errorf("WARNING: estimated memory %.0f MB exceeds 4096 MB for %d workers", estMemoryMB, totalWorkers))
	}

	// gRPC throughput warnings per client type
	c.checkGRPCThroughput(&errs)

	return errs
}

func (c *Config) checkGRPCThroughput(errs *[]error) {
	// pubSubClient: events + events_store
	pubsubRate := 0
	for _, p := range []string{"events", "events_store"} {
		if pc, ok := c.Patterns[p]; ok && pc.Enabled {
			pubsubRate += pc.Channels * pc.Rate
		}
	}
	if pubsubRate > 50000 {
		*errs = append(*errs, fmt.Errorf("WARNING: high aggregate rate %d msgs/s through single gRPC connection -- may cause transport bottleneck (pubSubClient)", pubsubRate))
	}

	// queuesClient: queue_stream + queue_simple
	queuesRate := 0
	for _, p := range []string{"queue_stream", "queue_simple"} {
		if pc, ok := c.Patterns[p]; ok && pc.Enabled {
			queuesRate += pc.Channels * pc.Rate
		}
	}
	if queuesRate > 50000 {
		*errs = append(*errs, fmt.Errorf("WARNING: high aggregate rate %d msgs/s through single gRPC connection -- may cause transport bottleneck (queuesClient)", queuesRate))
	}

	// cqClient: commands + queries
	cqRate := 0
	for _, p := range []string{"commands", "queries"} {
		if pc, ok := c.Patterns[p]; ok && pc.Enabled {
			cqRate += pc.Channels * pc.Rate
		}
	}
	if cqRate > 50000 {
		*errs = append(*errs, fmt.Errorf("WARNING: high aggregate rate %d msgs/s through single gRPC connection -- may cause transport bottleneck (cqClient)", cqRate))
	}
}

// LogResourceWarnings logs resource guard warnings from validation.
func (c *Config) LogResourceWarnings(logger *slog.Logger) {
	errs := c.Validate()
	for _, e := range errs {
		if strings.HasPrefix(e.Error(), "WARNING:") {
			logger.Warn(e.Error())
		}
	}
}

// BrokerHost returns host part of the broker address.
func (c *Config) BrokerHost() string {
	parts := strings.SplitN(c.Broker.Address, ":", 2)
	if len(parts) > 0 {
		return parts[0]
	}
	return "localhost"
}

// BrokerPort returns port part of the broker address.
func (c *Config) BrokerPort() int {
	parts := strings.SplitN(c.Broker.Address, ":", 2)
	if len(parts) == 2 {
		p, err := strconv.Atoi(parts[1])
		if err == nil {
			return p
		}
	}
	return 50000
}

func parseDurations(c *Config) error {
	var err error
	if c.DurationStr != "" && c.DurationStr != "0" {
		c.Duration, err = parseDuration(c.DurationStr)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", c.DurationStr, err)
		}
	} else if c.DurationStr == "0" {
		c.Duration = 0
	}

	if c.WarmupStr != "" {
		c.WarmupDuration, err = parseDuration(c.WarmupStr)
		if err != nil {
			return fmt.Errorf("invalid warmup_duration %q: %w", c.WarmupStr, err)
		}
	}

	if c.Metrics.ReportStr != "" {
		c.Metrics.ReportInterval, err = parseDuration(c.Metrics.ReportStr)
		if err != nil {
			return fmt.Errorf("invalid report_interval %q: %w", c.Metrics.ReportStr, err)
		}
	}

	if c.ForcedDisconnect.IntervalStr != "" && c.ForcedDisconnect.IntervalStr != "0" {
		c.ForcedDisconnect.Interval, err = parseDuration(c.ForcedDisconnect.IntervalStr)
		if err != nil {
			return fmt.Errorf("invalid forced_disconnect.interval %q: %w", c.ForcedDisconnect.IntervalStr, err)
		}
	}

	if c.ForcedDisconnect.DurationStr != "" {
		c.ForcedDisconnect.Duration, err = parseDuration(c.ForcedDisconnect.DurationStr)
		if err != nil {
			return fmt.Errorf("invalid forced_disconnect.duration %q: %w", c.ForcedDisconnect.DurationStr, err)
		}
	}

	if c.Recovery.ReconnectIntervalStr != "" {
		c.Recovery.ReconnectInterval, err = parseDuration(c.Recovery.ReconnectIntervalStr)
		if err != nil {
			return fmt.Errorf("invalid reconnect_interval %q: %w", c.Recovery.ReconnectIntervalStr, err)
		}
	}

	if c.Recovery.ReconnectMaxStr != "" {
		c.Recovery.ReconnectMaxInterval, err = parseDuration(c.Recovery.ReconnectMaxStr)
		if err != nil {
			return fmt.Errorf("invalid reconnect_max_interval %q: %w", c.Recovery.ReconnectMaxStr, err)
		}
	}

	if c.Thresholds.MaxDurationStr != "" {
		c.Thresholds.MaxDuration, err = parseDuration(c.Thresholds.MaxDurationStr)
		if err != nil {
			return fmt.Errorf("invalid max_duration %q: %w", c.Thresholds.MaxDurationStr, err)
		}
	}

	return nil
}

func parseDuration(s string) (time.Duration, error) {
	// Support "d" suffix for days
	if strings.HasSuffix(s, "d") {
		s = strings.TrimSuffix(s, "d")
		days, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("invalid day duration: %s", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

func RandomRunID() string {
	b := make([]byte, 4)
	_, _ = cryptorand.Read(b)
	return fmt.Sprintf("%08x", b)
}

// ParseDurationsPublic is the exported version of parseDurations.
func ParseDurationsPublic(c *Config) error {
	return parseDurations(c)
}

// ParseDurationValue parses a duration string supporting "d" suffix for days.
func ParseDurationValue(s string) (time.Duration, error) {
	return parseDuration(s)
}

// DetectV1Config checks if raw JSON contains v1 config fields and returns errors.
func DetectV1Config(raw map[string]interface{}) []string {
	var errs []string

	// Layer 1: top-level v1 keys
	if _, ok := raw["concurrency"]; ok {
		errs = append(errs, "detected v1 field: concurrency")
	}
	if _, ok := raw["rates"]; ok {
		errs = append(errs, "detected v1 field: rates")
	}
	if _, ok := raw["enabled_patterns"]; ok {
		errs = append(errs, "detected v1 field: enabled_patterns")
	}

	// Layer 2: old field names in patterns block
	if pats, ok := raw["patterns"]; ok {
		if patsMap, ok := pats.(map[string]interface{}); ok {
			for pname, pval := range patsMap {
				if pvalMap, ok := pval.(map[string]interface{}); ok {
					for _, oldField := range []string{"producers", "consumers", "senders", "responders"} {
						if _, ok := pvalMap[oldField]; ok {
							errs = append(errs, fmt.Sprintf("detected v1 field: patterns.%s.%s -- use %s_per_channel", pname, oldField, oldField))
						}
					}
				}
			}
		}
	}

	return errs
}

// TotalChannelCount returns the total number of channels across all enabled patterns.
func (c *Config) TotalChannelCount() int {
	total := 0
	for _, pname := range AllPatternNames {
		if pc, ok := c.Patterns[pname]; ok && pc.Enabled {
			total += pc.Channels
		}
	}
	return total
}
