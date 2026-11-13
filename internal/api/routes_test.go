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
	defer stopServer(t, server)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/status")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

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
	defer stopServer(t, server)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/status/")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestRoutes_Metrics(t *testing.T) {
	server := startTestServer(t)
	defer stopServer(t, server)

	resp, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "text/plain") {
		t.Errorf("expected text/plain content-type, got %q", contentType)
	}
}

func TestRoutes_Stats_NoProvider(t *testing.T) {
	server := startTestServer(t)
	defer stopServer(t, server)

	// Without a StatsProvider configured, stats endpoints return 503
	resp, err := http.Get("http://" + server.Addr() + "/api/v1/stats/cache")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", resp.StatusCode)
	}
}

func TestRoutes_APIv1_NotFound(t *testing.T) {
	server := startTestServer(t)
	defer stopServer(t, server)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/unknown")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

func TestRoutes_MethodNotAllowed(t *testing.T) {
	server := startTestServer(t)
	defer stopServer(t, server)

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
			defer closeBody(resp)

			if resp.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("expected status 405, got %d", resp.StatusCode)
			}
		})
	}
}

func TestRoutes_HealthHEAD(t *testing.T) {
	server := startTestServer(t)
	defer stopServer(t, server)

	client := &http.Client{}
	req, err := http.NewRequest(http.MethodHead, "http://"+server.Addr()+"/health", nil)
	if err != nil {
		t.Fatalf("create request error: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestRoutes_Status_WithBuildInfo(t *testing.T) {
	server := startTestServer(t)
	defer stopServer(t, server)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/status")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	var status StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Should have Go version
	if status.GoVersion == "" {
		t.Error("expected go_version to be set")
	}
	if !strings.HasPrefix(status.GoVersion, "go") {
		t.Errorf("expected go_version to start with 'go', got %q", status.GoVersion)
	}

	// Uptime should be positive (since server was started)
	if status.UptimeSeconds < 0 {
		t.Errorf("expected positive uptime, got %d", status.UptimeSeconds)
	}
}

// mockStatusProvider implements StatusProvider for testing.
type mockStatusProvider struct {
	mountPath  string
	devicePath string
	mountState MountState
	capacity   *CapacityInfo
}

func (m *mockStatusProvider) MountPath() string       { return m.mountPath }
func (m *mockStatusProvider) DevicePath() string      { return m.devicePath }
func (m *mockStatusProvider) MountState() MountState  { return m.mountState }
func (m *mockStatusProvider) Capacity() *CapacityInfo { return m.capacity }

func TestRoutes_Status_WithStatusProvider(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Set up mock status provider
	server.SetStatusProvider(&mockStatusProvider{
		mountPath:  "/mnt/test",
		devicePath: "/dev/sda1",
		mountState: MountStateMounted,
		capacity: &CapacityInfo{
			TotalBytes:  1073741824, // 1GB
			UsedBytes:   536870912,  // 512MB
			FreeBytes:   536870912,
			UsedPercent: 50.0,
			TotalBlocks: 1024,
			UsedBlocks:  512,
			FreeBlocks:  512,
		},
	})

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer stopServer(t, server)
	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/status")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	var status StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if status.MountPath != "/mnt/test" {
		t.Errorf("expected mount_path '/mnt/test', got %q", status.MountPath)
	}
	if status.DevicePath != "/dev/sda1" {
		t.Errorf("expected device_path '/dev/sda1', got %q", status.DevicePath)
	}
	if status.MountState != MountStateMounted {
		t.Errorf("expected mount_state 'mounted', got %q", status.MountState)
	}
	if status.Capacity == nil {
		t.Fatal("expected capacity to be set")
	}
	if status.Capacity.UsedPercent != 50.0 {
		t.Errorf("expected used_percent 50.0, got %f", status.Capacity.UsedPercent)
	}
}
