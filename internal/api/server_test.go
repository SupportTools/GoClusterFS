package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ListenAddr != ":8080" {
		t.Errorf("expected ListenAddr :8080, got %s", cfg.ListenAddr)
	}
	if cfg.AuthToken != "" {
		t.Errorf("expected empty AuthToken, got %s", cfg.AuthToken)
	}
	if cfg.CORSOrigins != nil {
		t.Errorf("expected nil CORSOrigins, got %v", cfg.CORSOrigins)
	}
	if cfg.ReadTimeout != 30*time.Second {
		t.Errorf("expected ReadTimeout 30s, got %s", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 30*time.Second {
		t.Errorf("expected WriteTimeout 30s, got %s", cfg.WriteTimeout)
	}
	if cfg.IdleTimeout != 60*time.Second {
		t.Errorf("expected IdleTimeout 60s, got %s", cfg.IdleTimeout)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("expected ShutdownTimeout 10s, got %s", cfg.ShutdownTimeout)
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "default config",
			cfg:  DefaultConfig(),
		},
		{
			name: "with auth token",
			cfg:  DefaultConfig().WithAuthToken("secret"),
		},
		{
			name: "with CORS origins",
			cfg:  DefaultConfig().WithCORSOrigins("http://localhost:3000", "https://example.com"),
		},
		{
			name: "with wildcard CORS",
			cfg:  DefaultConfig().WithCORSOrigins("*"),
		},
		{
			name: "localhost address",
			cfg:  DefaultConfig().WithListenAddr("127.0.0.1:9090"),
		},
		{
			name: "dynamic port",
			cfg:  DefaultConfig().WithListenAddr(":0"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); err != nil {
				t.Errorf("expected valid config, got error: %v", err)
			}
		})
	}
}

func TestConfig_Validate_Invalid(t *testing.T) {
	tests := []struct {
		name      string
		cfg       Config
		wantError error
	}{
		{
			name: "empty listen addr",
			cfg: func() Config {
				cfg := DefaultConfig()
				cfg.ListenAddr = ""
				return cfg
			}(),
			wantError: ErrInvalidListenAddr,
		},
		{
			name: "invalid listen addr format",
			cfg: func() Config {
				cfg := DefaultConfig()
				cfg.ListenAddr = "not-a-valid-addr"
				return cfg
			}(),
			wantError: ErrInvalidListenAddr,
		},
		{
			name: "negative read timeout",
			cfg: func() Config {
				cfg := DefaultConfig()
				cfg.ReadTimeout = -1 * time.Second
				return cfg
			}(),
			wantError: ErrInvalidTimeout,
		},
		{
			name: "negative write timeout",
			cfg: func() Config {
				cfg := DefaultConfig()
				cfg.WriteTimeout = -1 * time.Second
				return cfg
			}(),
			wantError: ErrInvalidTimeout,
		},
		{
			name: "empty CORS origin",
			cfg: func() Config {
				cfg := DefaultConfig()
				cfg.CORSOrigins = []string{""}
				return cfg
			}(),
			wantError: ErrInvalidCORSOrigin,
		},
		{
			name: "invalid CORS origin - no scheme",
			cfg: func() Config {
				cfg := DefaultConfig()
				cfg.CORSOrigins = []string{"localhost:3000"}
				return cfg
			}(),
			wantError: ErrInvalidCORSOrigin,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if err == nil {
				t.Errorf("expected error, got nil")
				return
			}
			if !errors.Is(err, tt.wantError) {
				t.Errorf("expected error %v, got %v", tt.wantError, err)
			}
		})
	}
}

func TestNewServer_ValidConfig(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0") // Use dynamic port
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if server == nil {
		t.Fatal("expected server, got nil")
	}
}

func TestNewServer_InvalidConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = ""

	server, err := NewServer(cfg)
	if err == nil {
		t.Error("expected error for invalid config, got nil")
	}
	if server != nil {
		t.Error("expected nil server for invalid config")
	}
}

func TestServer_StartStop(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Start server
	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	// Verify server is running
	if !server.Started() {
		t.Error("expected server to be started")
	}

	addr := server.Addr()
	if addr == "" {
		t.Error("expected non-empty address")
	}

	// Give it a moment to start listening
	time.Sleep(10 * time.Millisecond)

	// Stop server
	if err := server.Stop(); err != nil {
		t.Errorf("Stop error: %v", err)
	}
}

func TestServer_HealthEndpoint(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()

	// Give it a moment to start listening
	time.Sleep(10 * time.Millisecond)

	// Make health check request
	resp, err := http.Get("http://" + server.Addr() + "/health")
	if err != nil {
		t.Fatalf("health check request error: %v", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	// Check response body
	var health HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if health.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", health.Status)
	}
}

func TestServer_DoubleStart(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("first Start error: %v", err)
	}
	defer server.Stop()

	// Second start should return error
	if err := server.Start(ctx); !errors.Is(err, ErrServerAlreadyStarted) {
		t.Errorf("expected ErrServerAlreadyStarted, got: %v", err)
	}
}

func TestServer_StopWithoutStart(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Stop without start should return error
	if err := server.Stop(); !errors.Is(err, ErrServerNotStarted) {
		t.Errorf("expected ErrServerNotStarted, got: %v", err)
	}
}

func TestServer_Uptime(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Uptime before start should be 0
	if uptime := server.Uptime(); uptime != 0 {
		t.Errorf("expected 0 uptime before start, got %v", uptime)
	}

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()

	// Wait a bit
	time.Sleep(50 * time.Millisecond)

	// Uptime should be > 0
	if uptime := server.Uptime(); uptime <= 0 {
		t.Errorf("expected positive uptime after start, got %v", uptime)
	}
}

func TestServer_AddrBeforeStart(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Addr before start should be empty
	if addr := server.Addr(); addr != "" {
		t.Errorf("expected empty addr before start, got %q", addr)
	}
}
