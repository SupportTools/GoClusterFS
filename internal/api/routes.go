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
// This endpoint always returns 200 OK and is used for load balancer
// health checks and basic connectivity verification.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", "use GET or HEAD")
		return
	}
	writeJSON(w, http.StatusOK, HealthResponse{
		Status: HealthStatusOK,
	})
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
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", "use GET")
		return
	}

	writeJSON(w, http.StatusOK, StatusResponse{
		Status:        "ok",
		Version:       "dev",
		UptimeSeconds: int64(s.Uptime().Seconds()),
	})
}

// handleStats handles GET /api/v1/stats/* requests.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", "use GET")
		return
	}
	writeError(w, http.StatusNotImplemented, "stats endpoint not yet implemented", "")
}

// handleMetrics handles GET /metrics requests for Prometheus scraping.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", "use GET")
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("# HELP goclusterfs_up Indicates if GoClusterFS is up\n"))
	w.Write([]byte("# TYPE goclusterfs_up gauge\n"))
	w.Write([]byte("goclusterfs_up 1\n"))
}
