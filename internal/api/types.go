package api

import (
	"encoding/json"
	"net/http"
)

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	Status string `json:"status"`
}

// ErrorResponse is returned for API errors.
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Details string `json:"details,omitempty"`
}

// StatusResponse is returned by GET /api/v1/status.
// This is a placeholder that will be expanded in Task 5595.
type StatusResponse struct {
	Status    string `json:"status"`
	Version   string `json:"version"`
	Uptime    int64  `json:"uptime_seconds"`
	MountPath string `json:"mount_path,omitempty"`
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		json.NewEncoder(w).Encode(v)
	}
}

// writeError writes an error response with the given status code.
func writeError(w http.ResponseWriter, status int, err string, details string) {
	writeJSON(w, status, ErrorResponse{
		Error:   err,
		Code:    status,
		Details: details,
	})
}
