# GoClusterFS Cache Coherency Design

## Overview

This document specifies the cache coherency system for GoClusterFS. The design uses **generation counters** and **lock-based invalidation** to maintain consistency across multiple nodes without external communication channels.

---

## 1. Design Principles

| Principle | Rationale |
|-----------|-----------|
| No external communication | Only shared block storage for coordination |
| Lock-based synchronization | Leverage existing CAS lock protocol |
| Generation counters | Detect stale cache without full data comparison |
| Hybrid granularity | File-level metadata, extent-level mappings |

---

## 2. Inode Extensions

```go
type Inode struct {
    // ... existing fields ...

    // Cache coherency fields
    DataGeneration   uint64    // Incremented on ANY data modification
    MetaGeneration   uint64    // Incremented on metadata change
    LastModifierNode [16]byte  // UUID of node that last modified
    LastModifyLockSeq uint64   // Lock sequence at modification time
}
```

### Generation Counter Rules

| Modification Type | DataGeneration | MetaGeneration |
|-------------------|----------------|----------------|
| Data write | +1 | +1 (mtime changes) |
| Truncate | +1 | +1 |
| Extent allocation | +1 | 0 |
| chmod/chown | 0 | +1 |
| touch (times only) | 0 | +1 |

---

## 3. Cache Structures

### Inode Cache Entry

```go
type CachedInode struct {
    Inode           Inode
    DataGeneration  uint64      // Generation when cached
    MetaGeneration  uint64      // Generation when cached
    CachedAt        time.Time
    LastValidated   time.Time
    LocalLockHeld   bool
    LocalLockMode   LockMode
}
```

### Extent Cache Entry

```go
type CachedExtent struct {
    InodeNum        uint64
    LogicalBlock    uint64
    Extent          Extent
    State           ExtentCacheState
    InodeGeneration uint64
}

type ExtentCacheState uint8
const (
    ExtentCacheHole      ExtentCacheState = 0  // Known unallocated
    ExtentCacheUnwritten ExtentCacheState = 1  // Allocated, lazy zero
    ExtentCacheWritten   ExtentCacheState = 2  // Has data
    ExtentCacheUnknown   ExtentCacheState = 3  // Needs verification
)
```

### Cache Manager

```go
type CacheManager struct {
    mu              sync.RWMutex

    // Inode cache (LRU eviction)
    inodes          map[uint64]*CachedInode
    inodeLRU        *list.List
    maxInodes       int

    // Extent cache (per-inode)
    extents         map[uint64]map[uint64]*CachedExtent
    extentLRU       *list.List
    maxExtents      int

    // Negative cache (failed lookups)
    negatives       map[string]time.Time
    negativeTTL     time.Duration

    // Configuration
    validationTTL   time.Duration  // Default: 5s
    strictMode      bool
}
```

---

## 4. Cache Invalidation Protocol

### Lock-Based Invalidation

```
┌─────────────────────────────────────────────────────────────────┐
│           Lock-Based Cache Invalidation Protocol                 │
└─────────────────────────────────────────────────────────────────┘

EXCLUSIVE Lock Acquisition (for writes):
1. Node A wants to MODIFY inode X
2. Acquire exclusive lock on inode X
3. INVALIDATE local cache entry for inode X
   - Remove from inodes map
   - Remove all extent entries for inode X
4. READ inode X from disk with O_DIRECT
5. Perform modifications
6. INCREMENT DataGeneration and/or MetaGeneration
7. WRITE inode X to disk
8. UPDATE local cache with new data
9. RELEASE lock

SHARED Lock Acquisition (for reads):
1. Node B wants to READ inode X
2. Check local cache:
   a. If cached AND not expired AND generations match hint:
      - Use cached data (fast path)
   b. Otherwise:
      - Acquire shared lock
      - READ generation counters (single sector)
      - If match: cache valid
      - If mismatch: full re-read
3. RELEASE shared lock
```

### Invalidation Triggers

| Event | Invalidation Action |
|-------|---------------------|
| Acquire exclusive lock | Invalidate resource immediately |
| Lock stolen (node failure) | Invalidate all from failed node |
| TTL expiration | Mark "needs validation" |
| Memory pressure | LRU eviction |

---

## 5. Validation Protocol

```go
func (cm *CacheManager) ValidateInode(ctx context.Context, inodeNum uint64) (*CachedInode, error) {
    cm.mu.RLock()
    cached, exists := cm.inodes[inodeNum]
    cm.mu.RUnlock()

    if !exists {
        return cm.loadAndCacheInode(ctx, inodeNum)
    }

    // Check TTL
    if time.Since(cached.LastValidated) < cm.validationTTL && !cm.strictMode {
        return cached, nil
    }

    // Read just generation counters (64 bytes, single sector)
    diskGen, err := cm.readInodeGenerations(inodeNum)
    if err != nil {
        return nil, err
    }

    // Compare generations
    if diskGen.DataGeneration == cached.DataGeneration &&
       diskGen.MetaGeneration == cached.MetaGeneration {
        cm.mu.Lock()
        cached.LastValidated = time.Now()
        cm.mu.Unlock()
        return cached, nil
    }

    // Cache stale - full reload
    return cm.loadAndCacheInode(ctx, inodeNum)
}
```

---

## 6. Generation Counter Management

```go
func (inode *Inode) IncrementGenerations(modType ModificationType, nodeUUID [16]byte, lockSeq uint64) {
    switch modType {
    case ModificationData:
        inode.DataGeneration++
        inode.MetaGeneration++

    case ModificationMetaOnly:
        inode.MetaGeneration++

    case ModificationExtents:
        inode.DataGeneration++
    }

    inode.LastModifierNode = nodeUUID
    inode.LastModifyLockSeq = lockSeq
}
```

---

## 7. Extent Cache Invalidation

```go
func (cm *CacheManager) InvalidateExtents(inodeNum uint64, startBlock, endBlock uint64) {
    cm.mu.Lock()
    defer cm.mu.Unlock()

    extentMap, exists := cm.extents[inodeNum]
    if !exists {
        return
    }

    for logicalBlock := startBlock; logicalBlock <= endBlock; logicalBlock++ {
        delete(extentMap, logicalBlock)
    }
}

func (cm *CacheManager) InvalidateAllExtents(inodeNum uint64) {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    delete(cm.extents, inodeNum)
}
```

---

## 8. Configuration

```go
type CacheConfig struct {
    // Inode cache
    InodeCacheSize    int           // Default: 10000
    InodeTTL          time.Duration // Default: 5s

    // Extent cache
    ExtentCacheSize   int           // Default: 50000

    // Negative cache
    NegativeCacheSize int           // Default: 1000
    NegativeTTL       time.Duration // Default: 1s

    // Validation
    StrictMode        bool          // Default: false
}
```

---

## 9. Multi-Node Scenarios

### Scenario: Node A writes, Node B reads

```
Time    Node A                          Node B
─────────────────────────────────────────────────────────
t0      Cache: inode X (gen=5)          Cache: inode X (gen=5)
t1      Acquire exclusive lock
t2      Invalidate local cache
t3      Read inode from disk
t4      Modify inode
t5      Set gen=6
t6      Write inode to disk
t7      Release lock
t8                                      Want to read inode X
t9                                      Cache TTL expired
t10                                     Read generations: gen=6
t11                                     Cache mismatch (5 vs 6)
t12                                     Full re-read inode
t13                                     Cache updated (gen=6)
```

### Scenario: Concurrent readers

```
Multiple nodes can hold shared locks simultaneously.
All validate against same generation counters.
No invalidation needed for read-only access.
```
