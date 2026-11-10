package api

import (
	"net/http"
	"strings"
)

// registerRoutes registers all API routes with the server's router.
// Routes are organized into logical groups:
//   - /health        - Health check (no auth required)
//   - /api/v1/*      - Versioned API endpoints (auth required when configured)
//   - /metrics       - Prometheus metrics (no auth required)
func (s *Server) registerRoutes() {
	// Health check endpoint - always accessible, no auth required
	s.router.HandleFunc("/health", s.handleHealth)

	// API v1 route group - catch-all handler for versioned endpoints
	s.router.HandleFunc("/api/v1/", s.handleAPIv1)

	// Metrics endpoint for Prometheus scraping
	s.router.HandleFunc("/metrics", s.handleMetrics)
}

// handleHealth handles GET /health requests.
// Returns aggregate health status from all registered health checkers.
// Status codes: 200 OK, 503 Service Unavailable (if unhealthy).
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", "use GET or HEAD")
		return
	}

	checkers := s.GetHealthCheckers()
	if len(checkers) == 0 {
		// No checkers registered - just report API is OK
		writeJSON(w, http.StatusOK, HealthResponse{
			Status: HealthStatusOK,
		})
		return
	}

	// Aggregate health from all checkers
	resp := AggregateHealth(r.Context(), checkers...)

	// Return 503 if unhealthy, otherwise 200
	status := http.StatusOK
	if resp.Status == HealthStatusUnhealthy {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, resp)
}

// handleAPIv1 is the catch-all handler for /api/v1/* routes.
func (s *Server) handleAPIv1(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1")

	switch {
	case path == "/status" || path == "/status/":
		s.handleStatus(w, r)
	case strings.HasPrefix(path, "/stats"):
		s.handleStats(w, r)
	default:
		writeError(w, http.StatusNotFound, "endpoint not found", "")
	}
}

// handleStatus handles GET /api/v1/status requests.
// Returns comprehensive filesystem status including version, uptime, and capacity.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", "use GET")
		return
	}

	build := GetBuildInfo()
	resp := StatusResponse{
		Status:        "ok",
		Version:       build.Version,
		GitCommit:     build.GitCommit,
		BuildTime:     build.BuildTime,
		GoVersion:     build.GoVersion,
		UptimeSeconds: int64(s.Uptime().Seconds()),
	}

	// Add filesystem status if provider is available
	if provider := s.GetStatusProvider(); provider != nil {
		resp.MountPath = provider.MountPath()
		resp.DevicePath = provider.DevicePath()
		resp.MountState = provider.MountState()
		resp.Capacity = provider.Capacity()
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleStats handles GET /api/v1/stats/* requests.
// Routes to specific stats handlers based on the path suffix.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", "use GET")
		return
	}

	// Extract the stats sub-path
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/stats")
	path = strings.TrimSuffix(path, "/")

	switch path {
	case "", "/overview":
		s.handleStatsOverview(w, r)
	case "/cache":
		s.handleStatsCache(w, r)
	case "/journal":
		s.handleStatsJournal(w, r)
	case "/trim":
		s.handleStatsTrim(w, r)
	case "/lock":
		s.handleStatsLock(w, r)
	case "/sfb":
		s.handleStatsSFB(w, r)
	case "/cluster":
		s.handleStatsCluster(w, r)
	case "/all":
		s.handleStatsAll(w, r)
	default:
		writeError(w, http.StatusNotFound, "stats endpoint not found", "valid endpoints: cache, journal, trim, lock, sfb, cluster, all")
	}
}

// handleStatsOverview handles GET /api/v1/stats requests.
// Returns a summary of key statistics from all components.
func (s *Server) handleStatsOverview(w http.ResponseWriter, r *http.Request) {
	provider := s.GetStatsProvider()
	if provider == nil {
		writeError(w, http.StatusServiceUnavailable, "stats not available", "no stats provider configured")
		return
	}

	resp := StatsOverviewResponse{
		Cache:   provider.CacheStats(),
		Journal: provider.JournalStats(),
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleStatsCache handles GET /api/v1/stats/cache requests.
// Returns detailed cache hit/miss statistics.
func (s *Server) handleStatsCache(w http.ResponseWriter, r *http.Request) {
	provider := s.GetStatsProvider()
	if provider == nil {
		writeError(w, http.StatusServiceUnavailable, "stats not available", "no stats provider configured")
		return
	}

	stats := provider.CacheStats()
	if stats == nil {
		writeError(w, http.StatusNotFound, "cache stats not available", "cache component not initialized")
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// handleStatsJournal handles GET /api/v1/stats/journal requests.
// Returns journal transaction and space statistics.
func (s *Server) handleStatsJournal(w http.ResponseWriter, r *http.Request) {
	provider := s.GetStatsProvider()
	if provider == nil {
		writeError(w, http.StatusServiceUnavailable, "stats not available", "no stats provider configured")
		return
	}

	stats := provider.JournalStats()
	if stats == nil {
		writeError(w, http.StatusNotFound, "journal stats not available", "journal component not initialized")
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// handleStatsTrim handles GET /api/v1/stats/trim requests.
// Returns TRIM/discard operation statistics.
func (s *Server) handleStatsTrim(w http.ResponseWriter, r *http.Request) {
	provider := s.GetStatsProvider()
	if provider == nil {
		writeError(w, http.StatusServiceUnavailable, "stats not available", "no stats provider configured")
		return
	}

	stats := provider.TrimStats()
	if stats == nil {
		writeError(w, http.StatusNotFound, "trim stats not available", "trim component not initialized")
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// handleStatsLock handles GET /api/v1/stats/lock requests.
// Returns lock contention and hint cache statistics.
func (s *Server) handleStatsLock(w http.ResponseWriter, r *http.Request) {
	provider := s.GetStatsProvider()
	if provider == nil {
		writeError(w, http.StatusServiceUnavailable, "stats not available", "no stats provider configured")
		return
	}

	stats := provider.LockStats()
	if stats == nil {
		writeError(w, http.StatusNotFound, "lock stats not available", "lock component not initialized")
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// handleStatsSFB handles GET /api/v1/stats/sfb requests.
// Returns Sparse File Bitmap (block allocation) statistics.
func (s *Server) handleStatsSFB(w http.ResponseWriter, r *http.Request) {
	provider := s.GetStatsProvider()
	if provider == nil {
		writeError(w, http.StatusServiceUnavailable, "stats not available", "no stats provider configured")
		return
	}

	stats := provider.SFBStats()
	if stats == nil {
		writeError(w, http.StatusNotFound, "sfb stats not available", "block allocator not initialized")
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// handleStatsCluster handles GET /api/v1/stats/cluster requests.
// Returns cluster membership and node information.
func (s *Server) handleStatsCluster(w http.ResponseWriter, r *http.Request) {
	provider := s.GetStatsProvider()
	if provider == nil {
		writeError(w, http.StatusServiceUnavailable, "stats not available", "no stats provider configured")
		return
	}

	stats := provider.ClusterInfo()
	if stats == nil {
		writeError(w, http.StatusNotFound, "cluster info not available", "running in standalone mode")
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// handleStatsAll handles GET /api/v1/stats/all requests.
// Returns a combined snapshot of all available statistics.
func (s *Server) handleStatsAll(w http.ResponseWriter, r *http.Request) {
	provider := s.GetStatsProvider()
	if provider == nil {
		writeError(w, http.StatusServiceUnavailable, "stats not available", "no stats provider configured")
		return
	}

	stats := provider.AllStats()
	if stats == nil {
		writeError(w, http.StatusServiceUnavailable, "failed to collect stats", "")
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// handleMetrics handles GET /metrics requests for Prometheus scraping.
// Delegates to the Prometheus metrics handler for proper metric exposition.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", "use GET")
		return
	}

	// Delegate to the Prometheus handler
	s.GetMetricsHandler().ServeHTTP(w, r)
}
