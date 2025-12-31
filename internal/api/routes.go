package api

import (
	"net/http"
)

// registerRoutes registers all API routes with the server's router.
func (s *Server) registerRoutes() {
	// Health check endpoint - always accessible, no auth required
	s.router.HandleFunc("/health", s.handleHealth)

	// API v1 routes will be added in subsequent tasks:
	// - Task 5595: GET /api/v1/status
	// - Task 5602-5611: GET /api/v1/stats/*
	// - Task 5609: GET /metrics
}

// handleHealth handles GET /health requests.
// This endpoint always returns 200 OK and is used for load balancer
// health checks and basic connectivity verification.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, HealthResponse{
		Status: "ok",
	})
}
