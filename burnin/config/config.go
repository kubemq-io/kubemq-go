package config

import (
	"bytes"
	cryptorand "crypto/rand"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const ConfigVersion = 1

type Config struct {
	Version  int      `yaml:"version"`
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

	Rates struct {
		Events      int `yaml:"events"`
		EventsStore int `yaml:"events_store"`
		QueueStream int `yaml:"queue_stream"`
		QueueSimple int `yaml:"queue_simple"`
		Commands    int `yaml:"commands"`
		Queries     int `yaml:"queries"`
	} `yaml:"rates"`

	Concurrency struct {
		EventsProducers      int  `yaml:"events_producers"`
		EventsConsumers      int  `yaml:"events_consumers"`
		EventsConsumerGroup  bool `yaml:"events_consumer_group"`
		EventsStoreProducers int  `yaml:"events_store_producers"`
		EventsStoreConsumers int  `yaml:"events_store_consumers"`
		EventsStoreConsGroup bool `yaml:"events_store_consumer_group"`
		QueueStreamProducers int  `yaml:"queue_stream_producers"`
		QueueStreamConsumers int  `yaml:"queue_stream_consumers"`
		QueueSimpleProducers int  `yaml:"queue_simple_producers"`
		QueueSimpleConsumers int  `yaml:"queue_simple_consumers"`
		CommandsSenders      int  `yaml:"commands_senders"`
		CommandsResponders   int  `yaml:"commands_responders"`
		QueriesSenders       int  `yaml:"queries_senders"`
		QueriesResponders    int  `yaml:"queries_responders"`
	} `yaml:"concurrency"`

	Queue struct {
		PollMaxMessages        int  `yaml:"poll_max_messages"`
		PollWaitTimeoutSeconds int  `yaml:"poll_wait_timeout_seconds"`
		VisibilitySeconds      int  `yaml:"visibility_seconds"`
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

	c.Rates.Events = 100
	c.Rates.EventsStore = 100
	c.Rates.QueueStream = 50
	c.Rates.QueueSimple = 50
	c.Rates.Commands = 20
	c.Rates.Queries = 20

	c.Concurrency.EventsProducers = 1
	c.Concurrency.EventsConsumers = 1
	c.Concurrency.EventsStoreProducers = 1
	c.Concurrency.EventsStoreConsumers = 1
	c.Concurrency.QueueStreamProducers = 1
	c.Concurrency.QueueStreamConsumers = 1
	c.Concurrency.QueueSimpleProducers = 1
	c.Concurrency.QueueSimpleConsumers = 1
	c.Concurrency.CommandsSenders = 1
	c.Concurrency.CommandsResponders = 1
	c.Concurrency.QueriesSenders = 1
	c.Concurrency.QueriesResponders = 1

	c.Queue.PollMaxMessages = 10
	c.Queue.PollWaitTimeoutSeconds = 5
	c.Queue.VisibilitySeconds = 30
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

	applyEnvOverrides(c)

	if err := parseDurations(c); err != nil {
		return nil, err
	}

	if c.RunID == "" {
		c.RunID = randomRunID()
	}

	// Apply mode-dependent warmup default
	if c.WarmupStr == "" {
		if c.Mode == "benchmark" {
			c.WarmupDuration = 60 * time.Second
		} else {
			c.WarmupDuration = 0
		}
	}

	return c, nil
}

func FindConfigFile(cliPath string) string {
	if v := os.Getenv("BURNIN_CONFIG_FILE"); v != "" {
		return v
	}
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

func (c *Config) Validate() []error {
	var errs []error

	if c.Version > ConfigVersion {
		errs = append(errs, fmt.Errorf("config version %d is higher than supported version %d", c.Version, ConfigVersion))
	}

	if c.Mode != "soak" && c.Mode != "benchmark" {
		errs = append(errs, fmt.Errorf("mode must be 'soak' or 'benchmark', got %q", c.Mode))
	}

	if c.Duration < 0 {
		errs = append(errs, fmt.Errorf("duration must be >= 0"))
	}

	if c.Broker.Address == "" {
		errs = append(errs, fmt.Errorf("broker address is required"))
	}

	// Validate rates
	for _, r := range []struct {
		name string
		val  int
	}{
		{"events_rate", c.Rates.Events},
		{"events_store_rate", c.Rates.EventsStore},
		{"queue_stream_rate", c.Rates.QueueStream},
		{"queue_simple_rate", c.Rates.QueueSimple},
		{"commands_rate", c.Rates.Commands},
		{"queries_rate", c.Rates.Queries},
	} {
		if r.val <= 0 && c.Mode == "soak" {
			errs = append(errs, fmt.Errorf("%s must be > 0 in soak mode, got %d", r.name, r.val))
		}
	}

	// Validate concurrency
	for _, cc := range []struct {
		name string
		val  int
	}{
		{"events_producers", c.Concurrency.EventsProducers},
		{"events_consumers", c.Concurrency.EventsConsumers},
		{"events_store_producers", c.Concurrency.EventsStoreProducers},
		{"events_store_consumers", c.Concurrency.EventsStoreConsumers},
		{"queue_stream_producers", c.Concurrency.QueueStreamProducers},
		{"queue_stream_consumers", c.Concurrency.QueueStreamConsumers},
		{"queue_simple_producers", c.Concurrency.QueueSimpleProducers},
		{"queue_simple_consumers", c.Concurrency.QueueSimpleConsumers},
		{"commands_senders", c.Concurrency.CommandsSenders},
		{"commands_responders", c.Concurrency.CommandsResponders},
		{"queries_senders", c.Concurrency.QueriesSenders},
		{"queries_responders", c.Concurrency.QueriesResponders},
	} {
		if cc.val < 1 {
			errs = append(errs, fmt.Errorf("%s must be >= 1, got %d", cc.name, cc.val))
		}
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
		errs = append(errs, fmt.Errorf("message size_bytes must be >= 64, got %d", c.Message.SizeBytes))
	}

	if c.Message.ReorderWindow < 100 {
		errs = append(errs, fmt.Errorf("reorder_window must be >= 100, got %d", c.Message.ReorderWindow))
	}

	if c.Metrics.Port < 1 || c.Metrics.Port > 65535 {
		errs = append(errs, fmt.Errorf("metrics port must be 1-65535, got %d", c.Metrics.Port))
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
	if c.Recovery.ReconnectMultiplier < 1.0 {
		errs = append(errs, fmt.Errorf("reconnect_multiplier must be >= 1.0, got %f", c.Recovery.ReconnectMultiplier))
	}

	if c.Thresholds.MaxMemoryGrowth < 1.0 {
		errs = append(errs, fmt.Errorf("max_memory_growth_factor must be >= 1.0"))
	}

	// Warn: auto_ack with visibility_seconds
	if c.Queue.AutoAck && c.Queue.VisibilitySeconds > 0 {
		errs = append(errs, fmt.Errorf("WARNING: auto_ack=true with visibility_seconds=%d; visibility has no effect with auto_ack", c.Queue.VisibilitySeconds))
	}

	return errs
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

func randomRunID() string {
	b := make([]byte, 4)
	_, _ = cryptorand.Read(b)
	return fmt.Sprintf("%08x", b)
}

// Explicit env var mapping table per spec Section 6.0.3
func applyEnvOverrides(c *Config) {
	envStr("BURNIN_BROKER_ADDRESS", &c.Broker.Address)
	envStr("BURNIN_CLIENT_ID_PREFIX", &c.Broker.ClientIDPrefix)
	envStr("BURNIN_MODE", &c.Mode)
	envStr("BURNIN_DURATION", &c.DurationStr)
	envStr("BURNIN_RUN_ID", &c.RunID)
	envStr("BURNIN_WARMUP_DURATION", &c.WarmupStr)
	envStr("BURNIN_SDK_VERSION", &c.Output.SDKVersion)
	envStr("BURNIN_REPORT_OUTPUT_FILE", &c.Output.ReportFile)

	envInt("BURNIN_EVENTS_RATE", &c.Rates.Events)
	envInt("BURNIN_EVENTS_STORE_RATE", &c.Rates.EventsStore)
	envInt("BURNIN_QUEUE_STREAM_RATE", &c.Rates.QueueStream)
	envInt("BURNIN_QUEUE_SIMPLE_RATE", &c.Rates.QueueSimple)
	envInt("BURNIN_COMMANDS_RATE", &c.Rates.Commands)
	envInt("BURNIN_QUERIES_RATE", &c.Rates.Queries)

	envInt("BURNIN_EVENTS_PRODUCERS", &c.Concurrency.EventsProducers)
	envInt("BURNIN_EVENTS_CONSUMERS", &c.Concurrency.EventsConsumers)
	envBool("BURNIN_EVENTS_CONSUMER_GROUP", &c.Concurrency.EventsConsumerGroup)
	envInt("BURNIN_EVENTS_STORE_PRODUCERS", &c.Concurrency.EventsStoreProducers)
	envInt("BURNIN_EVENTS_STORE_CONSUMERS", &c.Concurrency.EventsStoreConsumers)
	envBool("BURNIN_EVENTS_STORE_CONSUMER_GROUP", &c.Concurrency.EventsStoreConsGroup)
	envInt("BURNIN_QUEUE_STREAM_PRODUCERS", &c.Concurrency.QueueStreamProducers)
	envInt("BURNIN_QUEUE_STREAM_CONSUMERS", &c.Concurrency.QueueStreamConsumers)
	envInt("BURNIN_QUEUE_SIMPLE_PRODUCERS", &c.Concurrency.QueueSimpleProducers)
	envInt("BURNIN_QUEUE_SIMPLE_CONSUMERS", &c.Concurrency.QueueSimpleConsumers)
	envInt("BURNIN_COMMANDS_SENDERS", &c.Concurrency.CommandsSenders)
	envInt("BURNIN_COMMANDS_RESPONDERS", &c.Concurrency.CommandsResponders)
	envInt("BURNIN_QUERIES_SENDERS", &c.Concurrency.QueriesSenders)
	envInt("BURNIN_QUERIES_RESPONDERS", &c.Concurrency.QueriesResponders)

	envInt("BURNIN_QUEUE_POLL_MAX_MESSAGES", &c.Queue.PollMaxMessages)
	envInt("BURNIN_QUEUE_POLL_WAIT_TIMEOUT_SECONDS", &c.Queue.PollWaitTimeoutSeconds)
	envInt("BURNIN_QUEUE_VISIBILITY_SECONDS", &c.Queue.VisibilitySeconds)
	envBool("BURNIN_QUEUE_AUTO_ACK", &c.Queue.AutoAck)
	envInt("BURNIN_MAX_QUEUE_DEPTH", &c.Queue.MaxDepth)

	envInt("BURNIN_RPC_TIMEOUT_MS", &c.RPC.TimeoutMS)

	envStr("BURNIN_MSG_SIZE_MODE", &c.Message.SizeMode)
	envInt("BURNIN_MSG_SIZE_BYTES", &c.Message.SizeBytes)
	envStr("BURNIN_MSG_SIZE_DISTRIBUTION", &c.Message.SizeDistribution)
	envInt("BURNIN_REORDER_WINDOW", &c.Message.ReorderWindow)

	envInt("BURNIN_METRICS_PORT", &c.Metrics.Port)
	envStr("BURNIN_REPORT_INTERVAL", &c.Metrics.ReportStr)

	envStr("BURNIN_LOG_FORMAT", &c.Logging.Format)
	envStr("BURNIN_LOG_LEVEL", &c.Logging.Level)

	envStr("BURNIN_FORCED_DISCONNECT_INTERVAL", &c.ForcedDisconnect.IntervalStr)
	envStr("BURNIN_FORCED_DISCONNECT_DURATION", &c.ForcedDisconnect.DurationStr)

	envStr("BURNIN_RECONNECT_INTERVAL", &c.Recovery.ReconnectIntervalStr)
	envStr("BURNIN_RECONNECT_MAX_INTERVAL", &c.Recovery.ReconnectMaxStr)
	envFloat("BURNIN_RECONNECT_MULTIPLIER", &c.Recovery.ReconnectMultiplier)

	envInt("BURNIN_SHUTDOWN_DRAIN_SECONDS", &c.Shutdown.DrainTimeoutSeconds)
	envBool("BURNIN_CLEANUP_CHANNELS", &c.Shutdown.CleanupChannels)

	envFloat("BURNIN_MAX_LOSS_PCT", &c.Thresholds.MaxLossPct)
	envFloat("BURNIN_MAX_EVENTS_LOSS_PCT", &c.Thresholds.MaxEventsLossPct)
	envFloat("BURNIN_MAX_DUPLICATION_PCT", &c.Thresholds.MaxDuplicationPct)
	envFloat("BURNIN_MAX_P99_LATENCY_MS", &c.Thresholds.MaxP99LatencyMS)
	envFloat("BURNIN_MAX_P999_LATENCY_MS", &c.Thresholds.MaxP999LatencyMS)
	envFloat("BURNIN_MIN_THROUGHPUT_PCT", &c.Thresholds.MinThroughputPct)
	envFloat("BURNIN_MAX_ERROR_RATE_PCT", &c.Thresholds.MaxErrorRatePct)
	envFloat("BURNIN_MAX_MEMORY_GROWTH_FACTOR", &c.Thresholds.MaxMemoryGrowth)
	envFloat("BURNIN_MAX_DOWNTIME_PCT", &c.Thresholds.MaxDowntimePct)
	envStr("BURNIN_MAX_DURATION", &c.Thresholds.MaxDurationStr)
}

func envStr(key string, dst *string) {
	if v := os.Getenv(key); v != "" {
		*dst = v
	}
}

func envInt(key string, dst *int) {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			*dst = n
		}
	}
}

func envFloat(key string, dst *float64) {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			*dst = f
		}
	}
}

func envBool(key string, dst *bool) {
	if v := os.Getenv(key); v != "" {
		switch strings.ToLower(v) {
		case "true", "1", "yes":
			*dst = true
		case "false", "0", "no":
			*dst = false
		}
	}
}
