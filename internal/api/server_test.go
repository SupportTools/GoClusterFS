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

// mockStatsProvider implements StatsProvider for testing.
type mockStatsProvider struct {
	cache   *CacheStatsResponse
	journal *JournalStatsResponse
	trim    *TrimStatsResponse
	lock    *LockStatsResponse
	sfb     *SFBStatsResponse
	cluster *ClusterInfoResponse
	all     *AllStatsResponse
}

func (m *mockStatsProvider) CacheStats() *CacheStatsResponse     { return m.cache }
func (m *mockStatsProvider) JournalStats() *JournalStatsResponse { return m.journal }
func (m *mockStatsProvider) TrimStats() *TrimStatsResponse       { return m.trim }
func (m *mockStatsProvider) LockStats() *LockStatsResponse       { return m.lock }
func (m *mockStatsProvider) SFBStats() *SFBStatsResponse         { return m.sfb }
func (m *mockStatsProvider) ClusterInfo() *ClusterInfoResponse   { return m.cluster }
func (m *mockStatsProvider) AllStats() *AllStatsResponse         { return m.all }

func TestServer_StatsEndpoints_NoProvider(t *testing.T) {
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

	time.Sleep(10 * time.Millisecond)

	endpoints := []string{
		"/api/v1/stats",
		"/api/v1/stats/cache",
		"/api/v1/stats/journal",
		"/api/v1/stats/trim",
		"/api/v1/stats/lock",
		"/api/v1/stats/sfb",
		"/api/v1/stats/cluster",
		"/api/v1/stats/all",
	}

	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			resp, err := http.Get("http://" + server.Addr() + endpoint)
			if err != nil {
				t.Fatalf("request error: %v", err)
			}
			defer resp.Body.Close()

			// Should return 503 when no provider is configured
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Errorf("expected status 503, got %d", resp.StatusCode)
			}
		})
	}
}

func TestServer_StatsCache_Success(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Set up mock provider with cache stats
	mockProvider := &mockStatsProvider{
		cache: &CacheStatsResponse{
			InodeCache: CacheHitRateInfo{
				Size:    1000,
				Hits:    800,
				Misses:  200,
				HitRate: 0.8,
			},
			ExtentCache: CacheHitRateInfo{
				Size:    500,
				Hits:    400,
				Misses:  100,
				HitRate: 0.8,
			},
			NegativeCache: CacheHitRateInfo{
				Size:    100,
				Hits:    50,
				Misses:  50,
				HitRate: 0.5,
			},
		},
	}
	server.SetStatsProvider(mockProvider)

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/stats/cache")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var stats CacheStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if stats.InodeCache.Size != 1000 {
		t.Errorf("expected inode cache size 1000, got %d", stats.InodeCache.Size)
	}
	if stats.InodeCache.HitRate != 0.8 {
		t.Errorf("expected inode hit rate 0.8, got %f", stats.InodeCache.HitRate)
	}
}

func TestServer_StatsCache_NotAvailable(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Provider with nil cache stats
	mockProvider := &mockStatsProvider{}
	server.SetStatsProvider(mockProvider)

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/stats/cache")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	// Should return 404 when cache stats are nil
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

func TestServer_StatsJournal_Success(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	mockProvider := &mockStatsProvider{
		journal: &JournalStatsResponse{
			State:             "clean",
			HeadOffset:        1024,
			TailOffset:        512,
			AvailableSpace:    4096,
			ActiveTxnCount:    5,
			CommitCount:       100,
			BytesWritten:      1024 * 1024,
			LastCheckpointSeq: 50,
		},
	}
	server.SetStatsProvider(mockProvider)

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/stats/journal")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var stats JournalStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if stats.State != "clean" {
		t.Errorf("expected state 'clean', got %q", stats.State)
	}
	if stats.CommitCount != 100 {
		t.Errorf("expected commit count 100, got %d", stats.CommitCount)
	}
}

func TestServer_StatsTrim_Success(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	mockProvider := &mockStatsProvider{
		trim: &TrimStatsResponse{
			TrimOperations: 50,
			TrimBytes:      1024 * 1024 * 10,
			TrimBlocks:     2560,
			TrimEnabled:    true,
			BatchSize:      64,
		},
	}
	server.SetStatsProvider(mockProvider)

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/stats/trim")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var stats TrimStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if stats.TrimOperations != 50 {
		t.Errorf("expected 50 trim operations, got %d", stats.TrimOperations)
	}
	if !stats.TrimEnabled {
		t.Error("expected trim enabled")
	}
}

func TestServer_StatsLock_Success(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	mockProvider := &mockStatsProvider{
		lock: &LockStatsResponse{
			HintCacheHits:    900,
			HintCacheMisses:  100,
			HintCacheHitRate: 0.9,
			ActiveLocks:      5,
			PendingLocks:     2,
		},
	}
	server.SetStatsProvider(mockProvider)

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/stats/lock")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var stats LockStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if stats.HintCacheHitRate != 0.9 {
		t.Errorf("expected hit rate 0.9, got %f", stats.HintCacheHitRate)
	}
}

func TestServer_StatsSFB_Success(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	mockProvider := &mockStatsProvider{
		sfb: &SFBStatsResponse{
			TotalBlocks:        10000,
			AllocatedBlocks:    7500,
			FreeBlocks:         2500,
			AllocationPercent:  75.0,
			AllocationsTotal:   5000,
			DeallocationsTotal: 2500,
		},
	}
	server.SetStatsProvider(mockProvider)

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/stats/sfb")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var stats SFBStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if stats.AllocationPercent != 75.0 {
		t.Errorf("expected allocation 75%%, got %f%%", stats.AllocationPercent)
	}
}

func TestServer_StatsCluster_Success(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	mockProvider := &mockStatsProvider{
		cluster: &ClusterInfoResponse{
			NodeID:       "node-1",
			Role:         "primary",
			State:        "active",
			ClusterSize:  3,
			HealthyNodes: 3,
			LeaderID:     "node-1",
			Peers: []ClusterPeerInfo{
				{NodeID: "node-2", Address: "192.168.1.2:7946", State: "active"},
				{NodeID: "node-3", Address: "192.168.1.3:7946", State: "active"},
			},
		},
	}
	server.SetStatsProvider(mockProvider)

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/stats/cluster")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var stats ClusterInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if stats.NodeID != "node-1" {
		t.Errorf("expected node-1, got %q", stats.NodeID)
	}
	if len(stats.Peers) != 2 {
		t.Errorf("expected 2 peers, got %d", len(stats.Peers))
	}
}

func TestServer_StatsAll_Success(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	mockProvider := &mockStatsProvider{
		all: &AllStatsResponse{
			CollectedAt: "2025-01-01T00:00:00Z",
			Cache: &CacheStatsResponse{
				InodeCache: CacheHitRateInfo{Size: 100},
			},
			Journal: &JournalStatsResponse{
				State: "clean",
			},
		},
	}
	server.SetStatsProvider(mockProvider)

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/stats/all")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var stats AllStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if stats.CollectedAt != "2025-01-01T00:00:00Z" {
		t.Errorf("expected collected_at, got %q", stats.CollectedAt)
	}
	if stats.Cache == nil {
		t.Error("expected cache stats")
	}
	if stats.Journal == nil {
		t.Error("expected journal stats")
	}
}

func TestServer_StatsOverview_Success(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	mockProvider := &mockStatsProvider{
		cache: &CacheStatsResponse{
			InodeCache: CacheHitRateInfo{Size: 100},
		},
		journal: &JournalStatsResponse{
			State: "clean",
		},
	}
	server.SetStatsProvider(mockProvider)

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/stats")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var stats StatsOverviewResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if stats.Cache == nil {
		t.Error("expected cache stats in overview")
	}
	if stats.Journal == nil {
		t.Error("expected journal stats in overview")
	}
}

func TestServer_StatsUnknownEndpoint(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	mockProvider := &mockStatsProvider{}
	server.SetStatsProvider(mockProvider)

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/stats/unknown")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

func TestServer_StatsMethodNotAllowed(t *testing.T) {
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

	time.Sleep(10 * time.Millisecond)

	// POST should not be allowed
	resp, err := http.Post("http://"+server.Addr()+"/api/v1/stats/cache", "application/json", nil)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.StatusCode)
	}
}
