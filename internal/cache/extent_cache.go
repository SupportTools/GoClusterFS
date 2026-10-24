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

	// InodeDataGen and InodeMetaGen are the inode's generations at cache time.
	// Every extent mapping change is committed by an inode write, which bumps
	// MetaGeneration, so a mapping is only valid for the inode version it was
	// looked up with.
	InodeDataGen uint64
	InodeMetaGen uint64

	// CachedAt is when this entry was cached.
	CachedAt time.Time
}

// IsStale returns true if the cached extent was looked up with a different inode version.
func (c *CachedExtent) IsStale(dataGen, metaGen uint64) bool {
	return c.InodeDataGen != dataGen || c.InodeMetaGen != metaGen
}

// GetExtent retrieves a cached extent mapping for this version of ino.
//
// This function does NOT perform disk I/O - caller must fall back to
// extent.Manager.LookupPhysical() on cache miss.
//
// Returns:
//   - physicalBlock: The physical block number (0 = hole)
//   - found: true if cache hit and valid
func (m *Manager) GetExtent(ino *inode.Inode, logicalBlock uint64) (physicalBlock uint64, found bool) {
	key := ExtentCacheKey{
		InodeNum:     ino.InodeNum,
		LogicalBlock: logicalBlock,
	}

	cached, ok := m.extents.Get(key)
	if !ok {
		return 0, false
	}

	if cached.IsStale(ino.DataGeneration, ino.MetaGeneration) {
		m.extents.Delete(key)
		return 0, false
	}

	return cached.PhysicalBlock, true
}

// CacheExtent stores an extent mapping looked up with this version of ino.
//
// Call this after a successful extent.Manager.LookupPhysical() to cache
// the result for future reads. physicalBlock is 0 for a hole.
func (m *Manager) CacheExtent(ino *inode.Inode, logicalBlock uint64, physicalBlock uint64) {
	if m.config.ExtentCacheSize == 0 {
		return // Extent cache disabled
	}

	key := ExtentCacheKey{
		InodeNum:     ino.InodeNum,
		LogicalBlock: logicalBlock,
	}

	cached := &CachedExtent{
		PhysicalBlock: physicalBlock,
		InodeDataGen:  ino.DataGeneration,
		InodeMetaGen:  ino.MetaGeneration,
		CachedAt:      time.Now(),
	}

	m.extents.Put(key, cached)
}

// InvalidateExtents makes cached extents for an inode stale.
//
// Call this when another node changed the inode's extent mapping (truncate,
// hole fill, CoW). It drops the cached inode, so the next lookup reads the
// current inode version from disk and every mapping cached for an older
// version misses.
func (m *Manager) InvalidateExtents(inodeNum uint32) {
	m.inodes.Delete(inodeNum)
}

// InvalidateExtentsForInodes makes cached extents for several inodes stale.
func (m *Manager) InvalidateExtentsForInodes(inodeNums ...uint32) {
	for _, inodeNum := range inodeNums {
		m.InvalidateExtents(inodeNum)
	}
}

// InvalidateExtentsForInode makes cached extents for a single inode stale.
// This satisfies the broadcast.InvalidationHandler interface.
func (m *Manager) InvalidateExtentsForInode(inodeNum uint32) {
	m.InvalidateExtents(inodeNum)
}

// LookupPhysicalCached is a convenience method that checks cache first,
// then falls back to disk lookup via the extent manager.
//
// This is the primary entry point for cached extent lookups.
//
// Parameters:
//   - extMgr: The extent manager for disk fallback
//   - ino: A committed inode version (no uncommitted extent edits)
//   - logicalBlock: The logical file block to look up
//
// Returns:
//   - physicalBlock: The physical disk block (0 = hole)
//   - err: Any error from disk lookup
func (m *Manager) LookupPhysicalCached(extMgr *extent.Manager, ino *inode.Inode, logicalBlock uint64) (uint64, error) {
	if phys, found := m.GetExtent(ino, logicalBlock); found {
		return phys, nil
	}

	phys, err := extMgr.LookupPhysical(ino, logicalBlock)
	if err != nil {
		return 0, err
	}

	// Cache the result (including holes which have phys=0)
	m.CacheExtent(ino, logicalBlock, phys)

	return phys, nil
}

// ExtentCacheStats returns statistics for the extent cache.
func (m *Manager) ExtentCacheStats() (size int, hits, misses uint64, hitRate float64) {
	size = m.extents.Len()
	hits, misses = m.extents.Stats()
	hitRate = m.extents.HitRate()
	return
}
