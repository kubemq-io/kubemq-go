package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"

	"github.com/kubemq-io/kubemq-go/v2/burnin/api"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RunController is the interface the server uses to interact with the engine.
type RunController interface {
	State() string
	StartRun(rc *api.RunConfig) (runID string, enabledCount int, err error)
	StopRun() error
	GetInfo() map[string]any
	GetBrokerStatus(ctx context.Context) map[string]any
	GetRunFull() map[string]any
	GetRunStatus() map[string]any
	GetRunConfig() (map[string]any, bool)
	GetRunReport() (map[string]any, bool)
	CleanupChannels(ctx context.Context) map[string]any
	RunID() string
	RunStartedAt() string
	RunError() string
}

// Server provides all HTTP endpoints per the burn-in REST API spec.
type Server struct {
	httpSrv     *http.Server
	ctrl        RunController
	logger      *slog.Logger
	corsOrigins string

	deprecationMu     sync.Mutex
	statusDeprecated  bool
	summaryDeprecated bool
}

// New creates a new Server listening on the given port.
func New(port int, ctrl RunController, logger *slog.Logger, corsOrigins string) *Server {
	s := &Server{
		ctrl:        ctrl,
		logger:      logger,
		corsOrigins: corsOrigins,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/health", s.wrap(s.handleHealth))
	mux.HandleFunc("/ready", s.wrap(s.handleReady))
	mux.HandleFunc("/info", s.wrap(s.handleInfo))
	mux.HandleFunc("/broker/status", s.wrap(s.handleBrokerStatus))
	mux.HandleFunc("/run/start", s.wrap(s.handleRunStart))
	mux.HandleFunc("/run/stop", s.wrap(s.handleRunStop))
	mux.HandleFunc("/run", s.wrap(s.handleRun))
	mux.HandleFunc("/run/status", s.wrap(s.handleRunStatus))
	mux.HandleFunc("/run/config", s.wrap(s.handleRunConfig))
	mux.HandleFunc("/run/report", s.wrap(s.handleRunReport))
	mux.HandleFunc("/cleanup", s.wrap(s.handleCleanup))

	// Profiling endpoints
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Legacy aliases
	mux.HandleFunc("/status", s.wrap(s.handleLegacyStatus))
	mux.HandleFunc("/summary", s.wrap(s.handleLegacySummary))

	// Prometheus metrics -- wrap with CORS
	promHandler := promhttp.Handler()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s.setCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		promHandler.ServeHTTP(w, r)
	})

	s.httpSrv = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return s
}

func (s *Server) Start() {
	go func() {
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server error", "error", err)
		}
	}()
}

func (s *Server) Stop(ctx context.Context) error {
	return s.httpSrv.Shutdown(ctx)
}

// wrap adds CORS headers and OPTIONS handling to every endpoint.
func (s *Server) wrap(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.setCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		handler(w, r)
	}
}

func (s *Server) setCORS(w http.ResponseWriter) {
	origin := s.corsOrigins
	if origin == "" {
		origin = "*"
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"message": message})
}

func writeValidationError(w http.ResponseWriter, message string, errs []string) {
	writeJSON(w, http.StatusBadRequest, map[string]any{
		"message": message,
		"errors":  errs,
	})
}

// --- Endpoint handlers ---

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	state := s.ctrl.State()
	switch state {
	case "starting", "stopping":
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not_ready", "state": state,
		})
	default:
		writeJSON(w, http.StatusOK, map[string]string{
			"status": "ready", "state": state,
		})
	}
}

func (s *Server) handleInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.ctrl.GetInfo())
}

func (s *Server) handleBrokerStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.ctrl.GetBrokerStatus(r.Context()))
}

func (s *Server) handleRunStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, "Method not allowed, use POST")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Failed to read request body")
		return
	}

	// Allow empty body (all defaults)
	if len(body) == 0 {
		body = []byte("{}")
	}

	// v1 format detection (dual-layer)
	v1Errs := api.DetectV1Format(body)
	if len(v1Errs) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"message": "v1 config format not supported. Update to v2 patterns format.",
			"errors":  v1Errs,
		})
		return
	}

	rc, err := api.ParseRunConfig(body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"message": fmt.Sprintf("Invalid JSON: %s", err.Error()),
		})
		return
	}

	if errs := rc.Validate(); len(errs) > 0 {
		writeValidationError(w, "Configuration validation failed", errs)
		return
	}

	state := s.ctrl.State()
	switch state {
	case "idle", "stopped", "error":
		// OK to start
	default:
		resp := map[string]any{
			"message": "Run already active",
			"state":   state,
		}
		if rid := s.ctrl.RunID(); rid != "" {
			resp["run_id"] = rid
		}
		if sa := s.ctrl.RunStartedAt(); sa != "" {
			resp["started_at"] = sa
		}
		writeJSON(w, http.StatusConflict, resp)
		return
	}

	runID, enabledCount, err := s.ctrl.StartRun(rc)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to start run: %s", err.Error()))
		return
	}

	totalChannels := rc.TotalChannelCount()

	// v2: return 202 with channel count
	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":  "starting",
		"run_id":  runID,
		"message": fmt.Sprintf("run starting with %d channels across %d patterns", totalChannels, enabledCount),
	})
}

func (s *Server) handleRunStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, "Method not allowed, use POST")
		return
	}

	state := s.ctrl.State()
	switch state {
	case "starting", "running":
		if err := s.ctrl.StopRun(); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"run_id":  s.ctrl.RunID(),
			"state":   "stopping",
			"message": "Graceful shutdown initiated",
		})
	case "stopping":
		writeJSON(w, http.StatusConflict, map[string]any{
			"message": "Run is already stopping",
			"run_id":  s.ctrl.RunID(),
			"state":   "stopping",
		})
	default:
		resp := map[string]any{
			"message": "No active run to stop",
			"state":   state,
		}
		writeJSON(w, http.StatusConflict, resp)
	}
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	// Ensure we don't match /run/start, /run/stop, etc.
	if r.URL.Path != "/run" {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, s.ctrl.GetRunFull())
}

func (s *Server) handleRunStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.ctrl.GetRunStatus())
}

func (s *Server) handleRunConfig(w http.ResponseWriter, _ *http.Request) {
	data, ok := s.ctrl.GetRunConfig()
	if !ok {
		writeError(w, http.StatusNotFound, "No run configuration available")
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *Server) handleRunReport(w http.ResponseWriter, _ *http.Request) {
	data, ok := s.ctrl.GetRunReport()
	if !ok {
		writeError(w, http.StatusNotFound, "No completed run report available")
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *Server) handleCleanup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, "Method not allowed, use POST")
		return
	}

	state := s.ctrl.State()
	switch state {
	case "starting", "running", "stopping":
		resp := map[string]any{
			"message": "Cannot cleanup while a run is active",
			"state":   state,
		}
		if rid := s.ctrl.RunID(); rid != "" {
			resp["run_id"] = rid
		}
		writeJSON(w, http.StatusConflict, resp)
		return
	}

	result := s.ctrl.CleanupChannels(r.Context())
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleLegacyStatus(w http.ResponseWriter, r *http.Request) {
	s.deprecationMu.Lock()
	if !s.statusDeprecated {
		s.statusDeprecated = true
		s.logger.Warn("DEPRECATED: /status endpoint is deprecated, use /run/status instead")
	}
	s.deprecationMu.Unlock()

	s.handleRunStatus(w, r)
}

func (s *Server) handleLegacySummary(w http.ResponseWriter, r *http.Request) {
	s.deprecationMu.Lock()
	if !s.summaryDeprecated {
		s.summaryDeprecated = true
		s.logger.Warn("DEPRECATED: /summary endpoint is deprecated, use /run/report instead")
	}
	s.deprecationMu.Unlock()

	s.handleRunReport(w, r)
}

// SplitOrigins splits a comma-separated origin string into individual origins.
func SplitOrigins(origins string) []string {
	if origins == "" || origins == "*" {
		return []string{"*"}
	}
	parts := strings.Split(origins, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
