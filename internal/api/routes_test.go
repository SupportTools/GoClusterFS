package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// startTestServer creates and starts a test server, returning it for cleanup.
func startTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	return server
}

func TestRoutes_Status(t *testing.T) {
	server := startTestServer(t)
	defer server.Stop()

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/status")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var status StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if status.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", status.Status)
	}
	if status.Version != "dev" {
		t.Errorf("expected version 'dev', got %q", status.Version)
	}
}

func TestRoutes_StatusWithTrailingSlash(t *testing.T) {
	server := startTestServer(t)
	defer server.Stop()

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/status/")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestRoutes_Metrics(t *testing.T) {
	server := startTestServer(t)
	defer server.Stop()

	resp, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "text/plain") {
		t.Errorf("expected text/plain content-type, got %q", contentType)
	}
}

func TestRoutes_Stats_NotImplemented(t *testing.T) {
	server := startTestServer(t)
	defer server.Stop()

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/stats/cache")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("expected status 501, got %d", resp.StatusCode)
	}
}

func TestRoutes_APIv1_NotFound(t *testing.T) {
	server := startTestServer(t)
	defer server.Stop()

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/unknown")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

func TestRoutes_MethodNotAllowed(t *testing.T) {
	server := startTestServer(t)
	defer server.Stop()

	tests := []struct {
		name   string
		path   string
		method string
	}{
		{"health POST", "/health", http.MethodPost},
		{"status POST", "/api/v1/status", http.MethodPost},
		{"metrics POST", "/metrics", http.MethodPost},
	}

	client := &http.Client{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, "http://"+server.Addr()+tt.path, nil)
			if err != nil {
				t.Fatalf("create request error: %v", err)
			}

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("request error: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("expected status 405, got %d", resp.StatusCode)
			}
		})
	}
}

func TestRoutes_HealthHEAD(t *testing.T) {
	server := startTestServer(t)
	defer server.Stop()

	client := &http.Client{}
	req, err := http.NewRequest(http.MethodHead, "http://"+server.Addr()+"/health", nil)
	if err != nil {
		t.Fatalf("create request error: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}
