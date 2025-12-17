package cache

import (
	"time"

	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
)

// ExtentCacheKey uniquely identifies a cached extent mapping.
// The cache maps (inode, logical block) -> physical block.
type ExtentCacheKey struct {
	InodeNum     uint32
	LogicalBlock uint64
}

// CachedExtent holds a cached extent lookup result.
type CachedExtent struct {
	// PhysicalBlock is the physical disk block number.
	// A value of 0 means "hole" (sparse file - unallocated region).
	PhysicalBlock uint64

	// InodeDataGen is the inode's DataGeneration at cache time.
	// When the inode's DataGeneration changes, all extent cache
	// entries for that inode become stale.
	InodeDataGen uint64

	// CachedAt is when this entry was cached.
	CachedAt time.Time
}

// IsStale returns true if the cached extent is stale relative to the inode.
func (c *CachedExtent) IsStale(currentDataGen uint64) bool {
	return c.InodeDataGen != currentDataGen
}

// GetExtent retrieves a cached extent mapping.
//
// The retrieval flow is:
//  1. Check extent cache for (inodeNum, logicalBlock)
//  2. If found, validate against inode's DataGeneration
//  3. If valid, return cached physical block
//  4. If stale or not found, return cache miss
//
// This function does NOT perform disk I/O - caller must fall back to
// extent.Manager.LookupPhysical() on cache miss.
//
// Returns:
//   - physicalBlock: The physical block number (0 = hole)
//   - found: true if cache hit and valid
func (m *Manager) GetExtent(inodeNum uint32, logicalBlock uint64, inodeDataGen uint64) (physicalBlock uint64, found bool) {
	key := ExtentCacheKey{
		InodeNum:     inodeNum,
		LogicalBlock: logicalBlock,
	}

	cached, ok := m.extents.Get(key)
	if !ok {
		return 0, false
	}

	// Validate against inode's current DataGeneration
	if cached.IsStale(inodeDataGen) {
		// Stale entry - remove it
		m.extents.Delete(key)
		return 0, false
	}

	return cached.PhysicalBlock, true
}

// CacheExtent stores an extent mapping in the cache.
//
// Call this after a successful extent.Manager.LookupPhysical() to cache
// the result for future reads.
//
// Parameters:
//   - inodeNum: The inode number
//   - logicalBlock: The logical file block being looked up
//   - physicalBlock: The physical disk block (0 for hole)
//   - inodeDataGen: The inode's current DataGeneration
func (m *Manager) CacheExtent(inodeNum uint32, logicalBlock uint64, physicalBlock uint64, inodeDataGen uint64) {
	if m.config.ExtentCacheSize == 0 {
		return // Extent cache disabled
	}

	key := ExtentCacheKey{
		InodeNum:     inodeNum,
		LogicalBlock: logicalBlock,
	}

	cached := &CachedExtent{
		PhysicalBlock: physicalBlock,
		InodeDataGen:  inodeDataGen,
		CachedAt:      time.Now(),
	}

	m.extents.Put(key, cached)
}

// InvalidateExtents removes all cached extents for an inode.
//
// Call this when:
//   - File is truncated (all extents may change)
//   - File data is modified in a way that changes extent mapping
//   - Inode's DataGeneration has changed
//
// Note: Individual extent invalidation is handled lazily via generation
// check in GetExtent. This function is for bulk invalidation.
func (m *Manager) InvalidateExtents(inodeNum uint32) {
	// Unfortunately, our LRU doesn't support prefix/range deletion.
	// We rely on lazy invalidation via DataGeneration checks.
	// For bulk invalidation, we'd need to iterate all entries.
	//
	// For now, extents are invalidated lazily when GetExtent detects
	// a DataGeneration mismatch. This is acceptable because:
	// 1. Stale entries don't return incorrect data (generation check)
	// 2. They get evicted naturally by LRU or replaced on next lookup
	// 3. InvalidateInodes() already handles the common case
}

// InvalidateExtentsForInodes removes cached extents when inode generation changes.
// This is called from OnLockAcquire for extent-level locks.
func (m *Manager) InvalidateExtentsForInodes(inodeNums ...uint32) {
	// Lazy invalidation via generation check handles this.
	// See InvalidateExtents() comment.
}

// LookupPhysicalCached is a convenience method that checks cache first,
// then falls back to disk lookup via the extent manager.
//
// This is the primary entry point for cached extent lookups.
//
// Parameters:
//   - extMgr: The extent manager for disk fallback
//   - ino: The inode (must have current DataGeneration)
//   - logicalBlock: The logical file block to look up
//
// Returns:
//   - physicalBlock: The physical disk block (0 = hole)
//   - err: Any error from disk lookup
func (m *Manager) LookupPhysicalCached(extMgr *extent.Manager, ino *inode.Inode, logicalBlock uint64) (uint64, error) {
	// Try cache first
	if phys, found := m.GetExtent(ino.InodeNum, logicalBlock, ino.DataGeneration); found {
		return phys, nil
	}

	// Cache miss - look up from disk
	phys, err := extMgr.LookupPhysical(ino, logicalBlock)
	if err != nil {
		return 0, err
	}

	// Cache the result (including holes which have phys=0)
	m.CacheExtent(ino.InodeNum, logicalBlock, phys, ino.DataGeneration)

	return phys, nil
}

// ExtentCacheStats returns statistics for the extent cache.
func (m *Manager) ExtentCacheStats() (size int, hits, misses uint64, hitRate float64) {
	size = m.extents.Len()
	hits, misses = m.extents.Stats()
	hitRate = m.extents.HitRate()
	return
}
