package stats

import (
	"sync"
	"time"
)

// Collector aggregates statistics from multiple components.
// It caches collected statistics for a configurable TTL to avoid
// expensive repeated queries.
type Collector struct {
	mu     sync.RWMutex
	config Config

	// Providers
	cacheProvider      CacheStatsProvider
	journalProvider    JournalStatsProvider
	filesystemProvider FilesystemStatsProvider
	lockProvider       LockStatsProvider
	trimProvider       TrimStatsProvider
	sfbProvider        SFBStatsProvider
	clusterProvider    ClusterInfoProvider

	// Cached snapshot
	cache      *Snapshot
	cacheTime  time.Time
	cacheValid bool
}

// NewCollector creates a new statistics collector with the given configuration.
func NewCollector(cfg Config) *Collector {
	return &Collector{
		config: cfg,
	}
}

// SetCacheProvider sets the cache statistics provider.
func (c *Collector) SetCacheProvider(provider CacheStatsProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cacheProvider = provider
	c.invalidateCache()
}

// SetJournalProvider sets the journal statistics provider.
func (c *Collector) SetJournalProvider(provider JournalStatsProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.journalProvider = provider
	c.invalidateCache()
}

// SetFilesystemProvider sets the filesystem statistics provider.
func (c *Collector) SetFilesystemProvider(provider FilesystemStatsProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.filesystemProvider = provider
	c.invalidateCache()
}

// SetLockProvider sets the lock statistics provider.
func (c *Collector) SetLockProvider(provider LockStatsProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lockProvider = provider
	c.invalidateCache()
}

// SetTrimProvider sets the TRIM statistics provider.
func (c *Collector) SetTrimProvider(provider TrimStatsProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trimProvider = provider
	c.invalidateCache()
}

// SetSFBProvider sets the Sparse File Bitmap statistics provider.
func (c *Collector) SetSFBProvider(provider SFBStatsProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sfbProvider = provider
	c.invalidateCache()
}

// SetClusterProvider sets the cluster information provider.
func (c *Collector) SetClusterProvider(provider ClusterInfoProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clusterProvider = provider
	c.invalidateCache()
}

// Collect returns a snapshot of all current statistics.
// The snapshot may be cached if within the TTL period.
func (c *Collector) Collect() *Snapshot {
	c.mu.RLock()
	if c.cacheValid && time.Since(c.cacheTime) < c.config.CacheTTL {
		snapshot := c.cache
		c.mu.RUnlock()
		return snapshot
	}
	c.mu.RUnlock()

	// Need to collect fresh stats
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if c.cacheValid && time.Since(c.cacheTime) < c.config.CacheTTL {
		return c.cache
	}

	// Collect fresh statistics
	snapshot := c.collectLocked()
	c.cache = snapshot
	c.cacheTime = time.Now()
	c.cacheValid = true

	return snapshot
}

// CollectFresh forces collection of fresh statistics, bypassing the cache.
func (c *Collector) CollectFresh() *Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	snapshot := c.collectLocked()
	c.cache = snapshot
	c.cacheTime = time.Now()
	c.cacheValid = true

	return snapshot
}

// InvalidateCache forces the next Collect call to fetch fresh data.
func (c *Collector) InvalidateCache() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invalidateCache()
}

// invalidateCache marks the cache as invalid (caller must hold mu).
func (c *Collector) invalidateCache() {
	c.cacheValid = false
}

// collectLocked performs the actual statistics collection.
// Caller must hold c.mu.
func (c *Collector) collectLocked() *Snapshot {
	snapshot := &Snapshot{
		CollectedAt: time.Now(),
	}

	// Collect from each provider if available
	if c.config.EnableComponentStats {
		if c.cacheProvider != nil {
			snapshot.Cache = c.cacheProvider.CacheStats()
		}

		if c.journalProvider != nil {
			snapshot.Journal = c.journalProvider.JournalStats()
		}

		if c.filesystemProvider != nil {
			snapshot.Filesystem = c.filesystemProvider.FilesystemStats()
		}

		if c.lockProvider != nil {
			snapshot.Lock = c.lockProvider.LockStats()
		}

		if c.trimProvider != nil {
			snapshot.Trim = c.trimProvider.TrimStats()
		}

		if c.sfbProvider != nil {
			snapshot.SFB = c.sfbProvider.SFBStats()
		}

		if c.clusterProvider != nil {
			snapshot.Cluster = c.clusterProvider.ClusterInfo()
		}
	}

	return snapshot
}

// CacheStats returns just the cache statistics.
// Returns nil if no cache provider is set.
func (c *Collector) CacheStats() *CacheStats {
	c.mu.RLock()
	provider := c.cacheProvider
	c.mu.RUnlock()

	if provider == nil {
		return nil
	}
	return provider.CacheStats()
}

// JournalStats returns just the journal statistics.
// Returns nil if no journal provider is set.
func (c *Collector) JournalStats() *JournalStats {
	c.mu.RLock()
	provider := c.journalProvider
	c.mu.RUnlock()

	if provider == nil {
		return nil
	}
	return provider.JournalStats()
}

// FilesystemStats returns just the filesystem statistics.
// Returns nil if no filesystem provider is set.
func (c *Collector) FilesystemStats() *FilesystemStats {
	c.mu.RLock()
	provider := c.filesystemProvider
	c.mu.RUnlock()

	if provider == nil {
		return nil
	}
	return provider.FilesystemStats()
}

// LockStats returns just the lock statistics.
// Returns nil if no lock provider is set.
func (c *Collector) LockStats() *LockStats {
	c.mu.RLock()
	provider := c.lockProvider
	c.mu.RUnlock()

	if provider == nil {
		return nil
	}
	return provider.LockStats()
}

// TrimStats returns just the TRIM statistics.
// Returns nil if no trim provider is set.
func (c *Collector) TrimStats() *TrimStats {
	c.mu.RLock()
	provider := c.trimProvider
	c.mu.RUnlock()

	if provider == nil {
		return nil
	}
	return provider.TrimStats()
}

// SFBStats returns just the Sparse File Bitmap statistics.
// Returns nil if no SFB provider is set.
func (c *Collector) SFBStats() *SFBStats {
	c.mu.RLock()
	provider := c.sfbProvider
	c.mu.RUnlock()

	if provider == nil {
		return nil
	}
	return provider.SFBStats()
}

// ClusterInfo returns just the cluster information.
// Returns nil if no cluster provider is set.
func (c *Collector) ClusterInfo() *ClusterInfo {
	c.mu.RLock()
	provider := c.clusterProvider
	c.mu.RUnlock()

	if provider == nil {
		return nil
	}
	return provider.ClusterInfo()
}

// Config returns the collector's configuration.
func (c *Collector) Config() Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config
}

// SetConfig updates the collector's configuration.
func (c *Collector) SetConfig(cfg Config) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = cfg
	c.invalidateCache()
}
