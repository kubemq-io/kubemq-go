package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/burnin/config"
	"github.com/kubemq-io/kubemq-go/v2/burnin/engine"
	"github.com/kubemq-io/kubemq-go/v2/burnin/metrics"
	"github.com/kubemq-io/kubemq-go/v2/burnin/server"
)

func main() {
	configPath := flag.String("config", "", "path to YAML config file")
	validateConfig := flag.Bool("validate-config", false, "validate config and exit")
	cleanupOnly := flag.Bool("cleanup-only", false, "delete all burn-in channels and exit")
	autoRun := flag.Bool("run", false, "auto-start a run using YAML config (backward compat)")
	flag.Parse()

	logLevel := slog.LevelInfo
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))

	cfgPath := config.FindConfigFile(*configPath)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(2)
	}

	switch cfg.Logging.Level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	}

	var handler slog.Handler
	if cfg.Logging.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	}
	logger = slog.New(handler)

	// Validate startup config
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

	// --validate-config mode: validate and exit
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

	// --cleanup-only mode: connect, delete channels, exit
	if *cleanupOnly {
		go func() {
			sig := <-sigCh
			logger.Info("received signal, aborting cleanup", "signal", sig)
			cancel()
		}()
		logger.Info("running cleanup-only mode")
		if err := engine.CleanupOnly(ctx, cfg, logger); err != nil {
			logger.Error("cleanup failed", "error", err)
			os.Exit(1)
		}
		logger.Info("cleanup complete")
		os.Exit(0)
	}

	// --- API-driven mode (default) ---

	// Pre-initialize all Prometheus metrics to 0 (spec §2, §8.3)
	metrics.InitMetrics()

	// Create engine (starts in idle state)
	eng := engine.New(cfg, logger)

	// Start HTTP server FIRST, before any broker connection (spec §2)
	srv := server.New(cfg.Metrics.Port, eng, logger, cfg.CORS.Origins)
	srv.Start()
	logger.Info("HTTP server started", "port", cfg.Metrics.Port, "state", "idle")

	// Print startup banner
	fmt.Println("===================================================================")
	fmt.Printf("  KUBEMQ BURN-IN TEST — Go SDK (API Mode)\n")
	fmt.Println("===================================================================")
	fmt.Printf("  Broker:   %s\n", cfg.Broker.Address)
	fmt.Printf("  API Port: %d\n", cfg.Metrics.Port)
	fmt.Printf("  State:    idle (waiting for POST /run/start)\n")
	fmt.Println("===================================================================")
	fmt.Println()

	// --run flag: auto-start using YAML config (backward compat)
	if *autoRun {
		logger.Info("auto-run mode: starting run from YAML config",
			"mode", cfg.Mode, "duration", cfg.Duration, "run_id", cfg.RunID)
		if err := eng.StartRunFromConfig(cfg); err != nil {
			logger.Error("failed to start run", "error", err)
			os.Exit(1)
		}
	}

	// Wait for SIGTERM/SIGINT
	sig := <-sigCh
	logger.Info("received signal, shutting down", "signal", sig)

	// Graceful shutdown: stop active run, then stop HTTP server
	passed := eng.GracefulShutdown()

	srvCtx, srvCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer srvCancel()
	_ = srv.Stop(srvCtx)

	logger.Info("burn-in app exiting")

	if !passed {
		os.Exit(1)
	}
}
