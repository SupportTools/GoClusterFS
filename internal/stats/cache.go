package stats

import "time"

// CacheInfo contains metadata about the statistics cache state.
type CacheInfo struct {
	// Valid indicates if the cache contains valid data.
	Valid bool `json:"valid"`

	// Age is how long since the cached data was collected.
	Age time.Duration `json:"age"`

	// TTL is the configured cache time-to-live.
	TTL time.Duration `json:"ttl"`

	// Stale indicates if the cache is past its TTL (needs refresh).
	Stale bool `json:"stale"`

	// LastCollected is when the cached snapshot was collected.
	LastCollected time.Time `json:"last_collected,omitempty"`
}

// CacheInfo returns information about the current cache state.
func (c *Collector) CacheInfo() CacheInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info := CacheInfo{
		Valid: c.cacheValid,
		TTL:   c.config.CacheTTL,
	}

	if c.cacheValid {
		info.Age = time.Since(c.cacheTime)
		info.LastCollected = c.cacheTime
		info.Stale = info.Age >= c.config.CacheTTL
	}

	return info
}

// IsCacheValid returns true if the cache contains valid, non-stale data.
func (c *Collector) IsCacheValid() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cacheValid && time.Since(c.cacheTime) < c.config.CacheTTL
}

// CacheAge returns how long since the cache was last refreshed.
// Returns 0 if the cache is not valid.
func (c *Collector) CacheAge() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.cacheValid {
		return 0
	}
	return time.Since(c.cacheTime)
}

// CachedSnapshot wraps a Snapshot with cache metadata.
// This is useful for API responses that need to indicate cache freshness.
type CachedSnapshot struct {
	// Snapshot contains the actual statistics.
	*Snapshot

	// FromCache indicates if this snapshot was served from cache.
	FromCache bool `json:"from_cache"`

	// CacheAge is how old the cached data is.
	CacheAge time.Duration `json:"cache_age,omitempty"`
}

// CollectWithMeta returns a snapshot along with cache metadata.
func (c *Collector) CollectWithMeta() CachedSnapshot {
	c.mu.RLock()
	wasValid := c.cacheValid && time.Since(c.cacheTime) < c.config.CacheTTL
	c.mu.RUnlock()

	snapshot := c.Collect()

	c.mu.RLock()
	defer c.mu.RUnlock()

	return CachedSnapshot{
		Snapshot:  snapshot,
		FromCache: wasValid,
		CacheAge:  time.Since(c.cacheTime),
	}
}
