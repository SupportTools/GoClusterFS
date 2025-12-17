package cache

import (
	"sync"
	"time"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
)

// CachedInode holds a cached inode along with validation metadata.
type CachedInode struct {
	// Inode is the cached inode data.
	Inode *inode.Inode

	// DataGeneration is the inode's DataGeneration at cache time.
	// Used to detect if file data has changed.
	DataGeneration uint64

	// MetaGeneration is the inode's MetaGeneration at cache time.
	// Used to detect if metadata (perms, times, etc.) has changed.
	MetaGeneration uint64

	// CachedAt is when this entry was added to the cache.
	CachedAt time.Time

	// LastValidated is when we last confirmed this entry matches disk.
	// Updated on successful validation checks.
	LastValidated time.Time
}

// IsStale returns true if the cached generations don't match the provided disk generations.
func (c *CachedInode) IsStale(diskDataGen, diskMetaGen uint64) bool {
	return c.DataGeneration != diskDataGen || c.MetaGeneration != diskMetaGen
}

// NeedsValidation returns true if the cache entry needs revalidation against disk.
func (c *CachedInode) NeedsValidation(ttl time.Duration, strictMode bool) bool {
	if strictMode {
		return true
	}
	return time.Since(c.LastValidated) > ttl
}

// negativeEntry tracks a "not found" lookup result.
type negativeEntry struct {
	CachedAt time.Time
}

// Manager coordinates all caching for a GoClusterFS mount.
// It maintains separate caches for inodes, extents, and negative lookups,
// with generation-based validation for coherency across nodes.
type Manager struct {
	mu sync.RWMutex

	// Configuration
	config Config

	// Inode cache (inode number -> cached inode)
	inodes *LRU[uint32, *CachedInode]

	// Extent cache (inode+logical block -> physical block)
	// Validated against inode's DataGeneration
	extents *LRU[ExtentCacheKey, *CachedExtent]

	// Negative cache (inode number -> "not found" entry)
	negatives *LRU[uint32, *negativeEntry]

	// Dependencies
	inodeTbl *inode.Table
	lockMgr  *lock.Manager
}

// NewManager creates a new cache manager.
//
// Parameters:
//   - cfg: Cache configuration (use DefaultConfig() for sensible defaults)
//   - inodeTbl: The inode table for disk reads
//   - lockMgr: Lock manager for invalidation hooks (can be nil)
func NewManager(cfg Config, inodeTbl *inode.Table, lockMgr *lock.Manager) (*Manager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Manager{
		config:    cfg,
		inodes:    NewLRU[uint32, *CachedInode](cfg.InodeCacheSize),
		extents:   NewLRU[ExtentCacheKey, *CachedExtent](cfg.ExtentCacheSize),
		negatives: NewLRU[uint32, *negativeEntry](cfg.NegativeCacheSize),
		inodeTbl:  inodeTbl,
		lockMgr:   lockMgr,
	}, nil
}

// Config returns the cache configuration.
func (m *Manager) Config() Config {
	return m.config
}

// InodeTable returns the underlying inode table.
// Use this when you need direct disk access (e.g., for writes).
func (m *Manager) InodeTable() *inode.Table {
	return m.inodeTbl
}

// Stats returns cache statistics for monitoring.
func (m *Manager) Stats() Stats {
	inodeHits, inodeMisses := m.inodes.Stats()
	extentHits, extentMisses := m.extents.Stats()
	negHits, negMisses := m.negatives.Stats()

	return Stats{
		InodeCacheSize:    m.inodes.Len(),
		InodeCacheHits:    inodeHits,
		InodeCacheMisses:  inodeMisses,
		InodeHitRate:      m.inodes.HitRate(),
		ExtentCacheSize:   m.extents.Len(),
		ExtentCacheHits:   extentHits,
		ExtentCacheMisses: extentMisses,
		ExtentHitRate:     m.extents.HitRate(),
		NegativeCacheSize: m.negatives.Len(),
		NegativeHits:      negHits,
		NegativeMisses:    negMisses,
	}
}

// Stats holds cache statistics for monitoring and debugging.
type Stats struct {
	// Inode cache stats
	InodeCacheSize   int
	InodeCacheHits   uint64
	InodeCacheMisses uint64
	InodeHitRate     float64

	// Extent cache stats
	ExtentCacheSize   int
	ExtentCacheHits   uint64
	ExtentCacheMisses uint64
	ExtentHitRate     float64

	// Negative cache stats
	NegativeCacheSize int
	NegativeHits      uint64
	NegativeMisses    uint64
}

// Clear removes all entries from all caches.
// This is useful after filesystem operations that might invalidate many entries,
// such as fsck or unmount/remount.
func (m *Manager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.inodes.Clear()
	m.extents.Clear()
	m.negatives.Clear()
}

// ResetStats resets all cache statistics.
func (m *Manager) ResetStats() {
	m.inodes.ResetStats()
	m.extents.ResetStats()
	m.negatives.ResetStats()
}

// OnLockAcquire is a callback for the lock manager that invalidates
// cached data when a lock is acquired. This ensures that after acquiring
// an exclusive lock, we read fresh data from disk rather than using
// potentially stale cached data.
//
// The callback is designed to be passed to lock.Manager.SetOnAcquire().
//
// Invalidation rules:
//   - LockTypeInode (8): Invalidate the cached inode
//   - Other types: No action (extent cache not yet implemented)
func (m *Manager) OnLockAcquire(lockType uint8, resourceID uint64, mode uint8) {
	// Only handle inode locks for now (type 8)
	// In the future, extent cache invalidation will be added here
	const LockTypeInode = 8

	if lockType == LockTypeInode {
		inodeNum := uint32(resourceID)
		m.InvalidateInode(inodeNum)
	}
}
