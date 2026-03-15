package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/kubemq-io/kubemq-go/v2/burnin/config"
	"github.com/kubemq-io/kubemq-go/v2/burnin/engine"
)

func main() {
	configPath := flag.String("config", "", "path to YAML config file")
	validateConfig := flag.Bool("validate-config", false, "validate config and exit")
	cleanupOnly := flag.Bool("cleanup-only", false, "delete all burn-in channels and exit")
	flag.Parse()

	// Setup logger
	logLevel := slog.LevelInfo
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))

	// Find and load config
	cfgPath := config.FindConfigFile(*configPath)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(2)
	}

	// Update log level from config
	switch cfg.Logging.Level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	}

	// Recreate logger with correct level and format
	var handler slog.Handler
	if cfg.Logging.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	}
	logger = slog.New(handler)

	// Validate config
	if errs := cfg.Validate(); len(errs) > 0 {
		hasErrors := false
		for _, e := range errs {
			errStr := e.Error()
			if len(errStr) > 7 && errStr[:7] == "WARNING" {
				logger.Warn(errStr)
			} else {
				logger.Error("config validation error", "error", e)
				hasErrors = true
			}
		}
		if hasErrors {
			os.Exit(2)
		}
	}

	if *validateConfig {
		logger.Info("config validation passed")
		if cfgPath != "" {
			logger.Info("config file", "path", cfgPath)
		}
		logger.Info("effective config",
			"mode", cfg.Mode,
			"duration", cfg.Duration,
			"broker", cfg.Broker.Address,
			"run_id", cfg.RunID,
		)
		os.Exit(0)
	}

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	// Cleanup-only mode
	if *cleanupOnly {
		logger.Info("running cleanup-only mode")
		if err := engine.CleanupOnly(ctx, cfg, logger); err != nil {
			logger.Error("cleanup failed", "error", err)
			os.Exit(1)
		}
		logger.Info("cleanup complete")
		os.Exit(0)
	}

	// Print startup banner
	fmt.Println("===================================================================")
	fmt.Printf("  KUBEMQ BURN-IN TEST — Go SDK\n")
	fmt.Println("===================================================================")
	fmt.Printf("  Mode:     %s\n", cfg.Mode)
	fmt.Printf("  Broker:   %s\n", cfg.Broker.Address)
	fmt.Printf("  Duration: %s\n", cfg.Duration)
	fmt.Printf("  Run ID:   %s\n", cfg.RunID)
	fmt.Println("===================================================================")
	fmt.Println()

	// Run engine
	eng := engine.New(cfg, logger)
	if err := eng.Run(ctx); err != nil {
		if ctx.Err() == nil {
			logger.Error("engine run failed", "error", err)
			eng.Shutdown()
			os.Exit(1)
		}
	}

	passed := eng.Shutdown()

	logger.Info("burn-in test complete")

	if !passed {
		os.Exit(1)
	}
}
