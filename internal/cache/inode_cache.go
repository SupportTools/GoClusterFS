package cache

import (
	"time"

	"github.com/supporttools/GoClusterFS/internal/inode"
)

// GetInode retrieves an inode, using the cache when possible.
//
// The retrieval flow is:
//  1. Check negative cache - if "not found" is cached, return early
//  2. Check inode cache - if valid (not stale, not expired), return cached
//  3. If cached but needs validation:
//     a. Read generation counters from disk (cheap: one sector)
//     b. If generations match, update LastValidated and return cached
//     c. If generations differ, reload full inode from disk
//  4. If not cached, read from disk and cache the result
//
// This provides O(1) cache hits while ensuring consistency via generation counters.
func (m *Manager) GetInode(inodeNum uint32) (*inode.Inode, error) {
	// Fast path: check negative cache first
	if m.isNegativeCached(inodeNum) {
		return nil, inode.ErrInodeNotInUse
	}

	// Check inode cache
	cached, found := m.inodes.Get(inodeNum)
	if found {
		// Cache hit - check if validation needed
		if !cached.NeedsValidation(m.config.InodeTTL, m.config.StrictMode) {
			// Fast path: cache is fresh enough
			return cached.Inode, nil
		}

		// Validation needed - read current inode from disk
		diskInode, err := m.inodeTbl.Read(inodeNum)
		if err != nil {
			// Inode became invalid - remove from cache
			m.inodes.Delete(inodeNum)
			return nil, err
		}

		// Check if it's now free (Magic == 0)
		if diskInode.Magic == 0 {
			m.inodes.Delete(inodeNum)
			m.addNegative(inodeNum)
			return nil, inode.ErrInodeNotInUse
		}

		// Compare generations
		if !cached.IsStale(diskInode.DataGeneration, diskInode.MetaGeneration) {
			// Cache is still valid - update validation time
			m.updateValidationTime(inodeNum)
			return cached.Inode, nil
		}

		// Cache is stale - update with fresh data
		m.cacheInode(diskInode)
		return diskInode, nil
	}

	// Cache miss - read from disk
	diskInode, err := m.inodeTbl.Read(inodeNum)
	if err != nil {
		return nil, err
	}

	// Check if inode is in use
	if diskInode.Magic == 0 {
		m.addNegative(inodeNum)
		return nil, inode.ErrInodeNotInUse
	}

	// Cache the inode for future reads
	m.cacheInode(diskInode)
	return diskInode, nil
}

// InvalidateInode removes an inode from the cache.
// Call this before modifying an inode to ensure consistency.
//
// The invalidation protocol is:
//  1. Acquire exclusive lock (external - not done here)
//  2. Call InvalidateInode to clear local cache
//  3. Read fresh from disk
//  4. Modify and write
//  5. Release lock
func (m *Manager) InvalidateInode(inodeNum uint32) {
	m.inodes.Delete(inodeNum)
	m.negatives.Delete(inodeNum) // Also clear negative cache
}

// InvalidateAll clears all cached inodes.
// Call this after operations that might invalidate many entries,
// such as fsck, unmount/remount, or network partition recovery.
func (m *Manager) InvalidateAll() {
	m.inodes.Clear()
	m.negatives.Clear()
}

// CacheInode explicitly caches an inode.
// Use this after reading an inode from disk in a write path,
// so subsequent reads can benefit from the cached data.
func (m *Manager) CacheInode(in *inode.Inode) {
	m.cacheInode(in)
}

// cacheInode adds an inode to the cache (internal helper).
func (m *Manager) cacheInode(in *inode.Inode) {
	now := time.Now()
	cached := &CachedInode{
		Inode:          in,
		DataGeneration: in.DataGeneration,
		MetaGeneration: in.MetaGeneration,
		CachedAt:       now,
		LastValidated:  now,
	}
	m.inodes.Put(in.InodeNum, cached)
}

// updateValidationTime updates the LastValidated timestamp for a cached inode.
func (m *Manager) updateValidationTime(inodeNum uint32) {
	cached, found := m.inodes.Peek(inodeNum)
	if found {
		cached.LastValidated = time.Now()
	}
}

// isNegativeCached checks if an inode lookup previously returned "not found".
func (m *Manager) isNegativeCached(inodeNum uint32) bool {
	entry, found := m.negatives.Peek(inodeNum)
	if !found {
		return false
	}

	// Check TTL
	if time.Since(entry.CachedAt) > m.config.NegativeTTL {
		m.negatives.Delete(inodeNum)
		return false
	}

	// Promote in LRU (negative lookups are still lookups)
	m.negatives.Get(inodeNum)
	return true
}

// addNegative adds an inode to the negative cache.
func (m *Manager) addNegative(inodeNum uint32) {
	entry := &negativeEntry{
		CachedAt: time.Now(),
	}
	m.negatives.Put(inodeNum, entry)
}

// PeekInode returns a cached inode without affecting LRU order or validation.
// Returns nil if the inode is not cached. This is useful for debugging.
func (m *Manager) PeekInode(inodeNum uint32) *CachedInode {
	cached, found := m.inodes.Peek(inodeNum)
	if !found {
		return nil
	}
	return cached
}

// IsCached returns true if the inode is currently in the cache.
func (m *Manager) IsCached(inodeNum uint32) bool {
	_, found := m.inodes.Peek(inodeNum)
	return found
}

// ForceReload reads an inode from disk and updates the cache,
// regardless of whether the cached version is valid.
// Use this after external modifications that bypass the normal write path.
func (m *Manager) ForceReload(inodeNum uint32) (*inode.Inode, error) {
	// Remove any existing cache entry
	m.InvalidateInode(inodeNum)

	// Read fresh from disk
	diskInode, err := m.inodeTbl.Read(inodeNum)
	if err != nil {
		return nil, err
	}

	// Check if inode is in use
	if diskInode.Magic == 0 {
		m.addNegative(inodeNum)
		return nil, inode.ErrInodeNotInUse
	}

	// Cache and return
	m.cacheInode(diskInode)
	return diskInode, nil
}
