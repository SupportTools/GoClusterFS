package stats

import (
	"testing"
	"time"
)

// mockCacheProvider implements CacheStatsProvider for testing.
type mockCacheProvider struct {
	stats *CacheStats
}

func (m *mockCacheProvider) CacheStats() *CacheStats {
	return m.stats
}

// mockJournalProvider implements JournalStatsProvider for testing.
type mockJournalProvider struct {
	stats *JournalStats
}

func (m *mockJournalProvider) JournalStats() *JournalStats {
	return m.stats
}

// mockFilesystemProvider implements FilesystemStatsProvider for testing.
type mockFilesystemProvider struct {
	stats *FilesystemStats
}

func (m *mockFilesystemProvider) FilesystemStats() *FilesystemStats {
	return m.stats
}

// mockLockProvider implements LockStatsProvider for testing.
type mockLockProvider struct {
	stats *LockStats
}

func (m *mockLockProvider) LockStats() *LockStats {
	return m.stats
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.CacheTTL != 1*time.Second {
		t.Errorf("expected CacheTTL 1s, got %v", cfg.CacheTTL)
	}
	if !cfg.EnableComponentStats {
		t.Error("expected EnableComponentStats true")
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr error
	}{
		{
			name:    "valid default",
			config:  DefaultConfig(),
			wantErr: nil,
		},
		{
			name:    "valid zero TTL",
			config:  Config{CacheTTL: 0},
			wantErr: nil,
		},
		{
			name:    "invalid negative TTL",
			config:  Config{CacheTTL: -1 * time.Second},
			wantErr: ErrInvalidTTL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err != tt.wantErr {
				t.Errorf("Validate() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewCollector(t *testing.T) {
	cfg := DefaultConfig()
	collector := NewCollector(cfg)

	if collector == nil {
		t.Fatal("NewCollector returned nil")
	}
}

func TestCollector_Collect_Empty(t *testing.T) {
	cfg := DefaultConfig()
	collector := NewCollector(cfg)

	snapshot := collector.Collect()

	if snapshot == nil {
		t.Fatal("Collect returned nil")
	}
	if snapshot.CollectedAt.IsZero() {
		t.Error("CollectedAt should not be zero")
	}
	if snapshot.Cache != nil {
		t.Error("expected Cache to be nil with no provider")
	}
	if snapshot.Journal != nil {
		t.Error("expected Journal to be nil with no provider")
	}
}

func TestCollector_SetProviders(t *testing.T) {
	cfg := DefaultConfig()
	collector := NewCollector(cfg)

	// Set cache provider
	cacheStats := &CacheStats{
		InodeCacheSize: 100,
		InodeHitRate:   0.95,
	}
	collector.SetCacheProvider(&mockCacheProvider{stats: cacheStats})

	// Set journal provider
	journalStats := &JournalStats{
		State:       "clean",
		CommitCount: 500,
	}
	collector.SetJournalProvider(&mockJournalProvider{stats: journalStats})

	// Collect and verify
	snapshot := collector.Collect()

	if snapshot.Cache == nil {
		t.Fatal("expected Cache to be set")
	}
	if snapshot.Cache.InodeCacheSize != 100 {
		t.Errorf("expected InodeCacheSize 100, got %d", snapshot.Cache.InodeCacheSize)
	}
	if snapshot.Cache.InodeHitRate != 0.95 {
		t.Errorf("expected InodeHitRate 0.95, got %f", snapshot.Cache.InodeHitRate)
	}

	if snapshot.Journal == nil {
		t.Fatal("expected Journal to be set")
	}
	if snapshot.Journal.State != "clean" {
		t.Errorf("expected State 'clean', got %q", snapshot.Journal.State)
	}
	if snapshot.Journal.CommitCount != 500 {
		t.Errorf("expected CommitCount 500, got %d", snapshot.Journal.CommitCount)
	}
}

func TestCollector_CacheTTL(t *testing.T) {
	cfg := Config{
		CacheTTL:             50 * time.Millisecond,
		EnableComponentStats: true,
	}
	collector := NewCollector(cfg)

	// Track how many times provider is called
	callCount := 0
	provider := &countingProvider{count: &callCount}
	collector.SetCacheProvider(provider)

	// First collect
	snapshot1 := collector.Collect()
	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}

	// Second collect within TTL - should use cache
	snapshot2 := collector.Collect()
	if callCount != 1 {
		t.Errorf("expected still 1 call (cached), got %d", callCount)
	}

	// Verify same snapshot returned (via CollectedAt)
	if !snapshot1.CollectedAt.Equal(snapshot2.CollectedAt) {
		t.Error("expected cached snapshot to have same CollectedAt")
	}

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	// Third collect after TTL - should refresh
	snapshot3 := collector.Collect()
	if callCount != 2 {
		t.Errorf("expected 2 calls after TTL, got %d", callCount)
	}
	if snapshot3.CollectedAt.Equal(snapshot1.CollectedAt) {
		t.Error("expected fresh snapshot after TTL")
	}
}

// countingProvider tracks how many times CacheStats is called.
type countingProvider struct {
	count *int
}

func (p *countingProvider) CacheStats() *CacheStats {
	*p.count++
	return &CacheStats{InodeCacheSize: *p.count}
}

func TestCollector_CollectFresh(t *testing.T) {
	cfg := Config{
		CacheTTL:             1 * time.Hour, // Long TTL
		EnableComponentStats: true,
	}
	collector := NewCollector(cfg)

	callCount := 0
	provider := &countingProvider{count: &callCount}
	collector.SetCacheProvider(provider)

	// First collect
	collector.Collect()
	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}

	// CollectFresh should bypass cache
	collector.CollectFresh()
	if callCount != 2 {
		t.Errorf("expected 2 calls with CollectFresh, got %d", callCount)
	}
}

func TestCollector_InvalidateCache(t *testing.T) {
	cfg := Config{
		CacheTTL:             1 * time.Hour, // Long TTL
		EnableComponentStats: true,
	}
	collector := NewCollector(cfg)

	callCount := 0
	provider := &countingProvider{count: &callCount}
	collector.SetCacheProvider(provider)

	// First collect
	collector.Collect()
	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}

	// Invalidate cache
	collector.InvalidateCache()

	// Next collect should refresh
	collector.Collect()
	if callCount != 2 {
		t.Errorf("expected 2 calls after InvalidateCache, got %d", callCount)
	}
}

func TestCollector_DisabledComponentStats(t *testing.T) {
	cfg := Config{
		CacheTTL:             1 * time.Second,
		EnableComponentStats: false, // Disabled
	}
	collector := NewCollector(cfg)

	// Set provider
	collector.SetCacheProvider(&mockCacheProvider{
		stats: &CacheStats{InodeCacheSize: 100},
	})

	// Collect - should not include stats
	snapshot := collector.Collect()

	if snapshot.Cache != nil {
		t.Error("expected Cache to be nil when component stats disabled")
	}
}

func TestCollector_DirectStats(t *testing.T) {
	cfg := DefaultConfig()
	collector := NewCollector(cfg)

	// No providers - should return nil
	if collector.CacheStats() != nil {
		t.Error("expected nil CacheStats with no provider")
	}
	if collector.JournalStats() != nil {
		t.Error("expected nil JournalStats with no provider")
	}
	if collector.FilesystemStats() != nil {
		t.Error("expected nil FilesystemStats with no provider")
	}
	if collector.LockStats() != nil {
		t.Error("expected nil LockStats with no provider")
	}

	// Set providers and verify direct access
	collector.SetCacheProvider(&mockCacheProvider{
		stats: &CacheStats{InodeCacheSize: 42},
	})
	collector.SetLockProvider(&mockLockProvider{
		stats: &LockStats{HintCacheHits: 999},
	})

	if stats := collector.CacheStats(); stats == nil || stats.InodeCacheSize != 42 {
		t.Error("expected CacheStats with InodeCacheSize 42")
	}
	if stats := collector.LockStats(); stats == nil || stats.HintCacheHits != 999 {
		t.Error("expected LockStats with HintCacheHits 999")
	}
}

func TestCacheInfo(t *testing.T) {
	cfg := Config{
		CacheTTL:             100 * time.Millisecond,
		EnableComponentStats: true,
	}
	collector := NewCollector(cfg)

	// Before any collection
	info := collector.CacheInfo()
	if info.Valid {
		t.Error("expected cache to be invalid before collection")
	}

	// After collection
	collector.Collect()
	info = collector.CacheInfo()
	if !info.Valid {
		t.Error("expected cache to be valid after collection")
	}
	if info.Stale {
		t.Error("expected cache to not be stale immediately after collection")
	}
	if info.TTL != 100*time.Millisecond {
		t.Errorf("expected TTL 100ms, got %v", info.TTL)
	}

	// Wait for TTL to expire
	time.Sleep(110 * time.Millisecond)
	info = collector.CacheInfo()
	if !info.Stale {
		t.Error("expected cache to be stale after TTL")
	}
}

func TestIsCacheValid(t *testing.T) {
	cfg := Config{
		CacheTTL:             50 * time.Millisecond,
		EnableComponentStats: true,
	}
	collector := NewCollector(cfg)

	// Before collection
	if collector.IsCacheValid() {
		t.Error("expected cache invalid before collection")
	}

	// After collection
	collector.Collect()
	if !collector.IsCacheValid() {
		t.Error("expected cache valid after collection")
	}

	// After TTL expires
	time.Sleep(60 * time.Millisecond)
	if collector.IsCacheValid() {
		t.Error("expected cache invalid after TTL")
	}
}

func TestCollectWithMeta(t *testing.T) {
	cfg := Config{
		CacheTTL:             100 * time.Millisecond,
		EnableComponentStats: true,
	}
	collector := NewCollector(cfg)

	// First collection - not from cache
	result1 := collector.CollectWithMeta()
	if result1.FromCache {
		t.Error("expected FromCache=false on first collection")
	}

	// Second collection - from cache
	result2 := collector.CollectWithMeta()
	if !result2.FromCache {
		t.Error("expected FromCache=true on second collection")
	}
	if result2.CacheAge < 0 {
		t.Error("expected positive CacheAge")
	}
}

func TestPresetConfigs(t *testing.T) {
	// Test HighFrequencyConfig
	high := HighFrequencyConfig()
	if high.CacheTTL != 100*time.Millisecond {
		t.Errorf("HighFrequencyConfig TTL = %v, want 100ms", high.CacheTTL)
	}
	if err := high.Validate(); err != nil {
		t.Errorf("HighFrequencyConfig validation failed: %v", err)
	}

	// Test LowOverheadConfig
	low := LowOverheadConfig()
	if low.CacheTTL != 10*time.Second {
		t.Errorf("LowOverheadConfig TTL = %v, want 10s", low.CacheTTL)
	}
	if low.EnableComponentStats {
		t.Error("LowOverheadConfig should have EnableComponentStats=false")
	}
	if err := low.Validate(); err != nil {
		t.Errorf("LowOverheadConfig validation failed: %v", err)
	}
}

func TestCollector_SetConfig(t *testing.T) {
	collector := NewCollector(DefaultConfig())

	// Verify initial config
	if collector.Config().CacheTTL != 1*time.Second {
		t.Error("expected default TTL")
	}

	// Update config
	newCfg := Config{CacheTTL: 5 * time.Second, EnableComponentStats: true}
	collector.SetConfig(newCfg)

	if collector.Config().CacheTTL != 5*time.Second {
		t.Errorf("expected new TTL 5s, got %v", collector.Config().CacheTTL)
	}
}
