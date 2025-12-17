// Package cache implements in-memory caching for GoClusterFS with generation-based
// coherency across multiple nodes.
//
// The cache uses generation counters stored in each inode to detect stale entries.
// When a cached inode's generation doesn't match disk, it's reloaded. This provides
// eventual consistency without inter-node communication.
//
// Invalidation is triggered by:
//   - Acquiring exclusive locks (writer invalidates before modifying)
//   - TTL expiration (periodic validation against disk)
//   - LRU eviction (memory pressure)
//   - Network broadcast (optional, for near-instant cross-node invalidation)
package cache

import (
	"time"

	"github.com/supporttools/GoClusterFS/internal/cache/broadcast"
)

// Config holds configuration for the cache manager.
type Config struct {
	// InodeCacheSize is the maximum number of inodes to cache.
	// Each cached inode uses approximately 350 bytes of memory.
	// Default: 10000 (~3.5MB memory)
	InodeCacheSize int

	// InodeTTL is how long a cached inode is considered valid before
	// re-validation against disk. Shorter values provide stronger consistency
	// at the cost of more disk reads.
	// Default: 5 seconds
	InodeTTL time.Duration

	// ExtentCacheSize is the maximum number of extent mappings to cache.
	// Each extent entry uses approximately 64 bytes.
	// Default: 50000 (~3.2MB memory)
	ExtentCacheSize int

	// NegativeCacheSize is the maximum number of "not found" results to cache.
	// This prevents repeated disk lookups for non-existent inodes.
	// Default: 1000
	NegativeCacheSize int

	// NegativeTTL is how long a negative cache entry is valid.
	// Should be short to avoid stale "not found" responses when files are created.
	// Default: 1 second
	NegativeTTL time.Duration

	// StrictMode disables TTL-based caching. When true, every read validates
	// the generation counters against disk. This provides strong consistency
	// but significantly impacts performance.
	// Default: false
	StrictMode bool

	// Network holds configuration for network-based cache invalidation.
	// When enabled, nodes broadcast invalidations to each other for
	// near-instant cross-node cache coherency.
	// Default: broadcast.DisabledConfig() (disabled)
	Network broadcast.Config
}

// DefaultConfig returns a Config with sensible defaults for typical workloads.
// These defaults balance memory usage, consistency, and performance.
// Network broadcast is disabled by default for backward compatibility.
func DefaultConfig() Config {
	return Config{
		InodeCacheSize:    10000,
		InodeTTL:          5 * time.Second,
		ExtentCacheSize:   50000,
		NegativeCacheSize: 1000,
		NegativeTTL:       1 * time.Second,
		StrictMode:        false,
		Network:           broadcast.DisabledConfig(),
	}
}

// NetworkEnabledConfig returns a Config with network broadcast enabled.
// This provides near-instant (<100ms) cross-node cache invalidation.
func NetworkEnabledConfig() Config {
	return Config{
		InodeCacheSize:    10000,
		InodeTTL:          5 * time.Second,
		ExtentCacheSize:   50000,
		NegativeCacheSize: 1000,
		NegativeTTL:       1 * time.Second,
		StrictMode:        false,
		Network:           broadcast.DefaultConfig(),
	}
}

// HighConsistencyConfig returns a Config optimized for strong consistency.
// Use this when data correctness is more important than performance.
// Network broadcast is enabled for fastest invalidation.
func HighConsistencyConfig() Config {
	return Config{
		InodeCacheSize:    10000,
		InodeTTL:          1 * time.Second,
		ExtentCacheSize:   50000,
		NegativeCacheSize: 500,
		NegativeTTL:       500 * time.Millisecond,
		StrictMode:        true,
		Network:           broadcast.DefaultConfig(),
	}
}

// HighPerformanceConfig returns a Config optimized for read-heavy workloads.
// Use this when reads vastly outnumber writes and brief staleness is acceptable.
// Network broadcast is disabled to minimize network overhead.
func HighPerformanceConfig() Config {
	return Config{
		InodeCacheSize:    50000,
		InodeTTL:          30 * time.Second,
		ExtentCacheSize:   100000,
		NegativeCacheSize: 5000,
		NegativeTTL:       5 * time.Second,
		StrictMode:        false,
		Network:           broadcast.DisabledConfig(),
	}
}

// Validate checks that the configuration is valid.
// Returns an error describing any invalid settings.
func (c *Config) Validate() error {
	if c.InodeCacheSize < 0 {
		return ErrInvalidCacheSize
	}
	if c.ExtentCacheSize < 0 {
		return ErrInvalidCacheSize
	}
	if c.NegativeCacheSize < 0 {
		return ErrInvalidCacheSize
	}
	if c.InodeTTL < 0 {
		return ErrInvalidTTL
	}
	if c.NegativeTTL < 0 {
		return ErrInvalidTTL
	}
	return nil
}
