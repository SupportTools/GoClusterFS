# GoClusterFS Snapshot Design

## Overview

This document specifies the snapshot system for GoClusterFS, including block-level reference counting, copy-on-write (CoW), fast snapshot creation, and space-efficient snapshot deletion.

---

## 1. Design Goals

| Goal | Description |
|------|-------------|
| O(1) snapshot creation | Create snapshots instantly regardless of file size |
| Space efficiency | Shared blocks consume storage only once |
| Fast CoW | Minimal overhead for writes to snapshotted data |
| Safe deletion | Proper refcount management, no orphaned blocks |
| Writable clones | Support for branching from snapshots |

---

## 2. Reference Count Table

### On-Disk Layout

```
┌─────────────────────────────────────────────────────────┐
│                 Reference Count Region                   │
├─────────────────────────────────────────────────────────┤
│ RefCount Header (4KB)                                    │
├─────────────────────────────────────────────────────────┤
│ RefCount Entries (8 bytes each)                          │
│ Entry[0]: Block 0 refcount                               │
│ Entry[1]: Block 1 refcount                               │
│ ...                                                      │
│ Entry[N]: Block N refcount                               │
└─────────────────────────────────────────────────────────┘

For 1TB volume with 1MB blocks:
- 1,048,576 data blocks
- 8 bytes per entry = 8MB refcount table
- Overhead: 0.0008% of volume
```

### Reference Count Entry

```go
type RefCountEntry struct {
    RefCount uint32   // Number of references (0 = free, 1 = exclusive, >1 = shared)
    Flags    uint16   // RefCountFlagPinned, RefCountFlagPendingFree
    Reserved uint16   // Alignment padding
}

type RefCountFlags uint16
const (
    RefCountFlagPinned      RefCountFlags = 1 << 0  // Cannot be freed (active write)
    RefCountFlagPendingFree RefCountFlags = 1 << 1  // Scheduled for TRIM
)
```

### Reference Count Header

```go
type RefCountHeader struct {
    Magic           [8]byte    // "GCLSREFC"
    Version         uint32
    TotalBlocks     uint64     // Total data blocks in volume
    FreeBlocks      uint64     // Blocks with refcount = 0
    SharedBlocks    uint64     // Blocks with refcount > 1
    Checksum        uint32     // CRC32C of header
}
```

---

## 3. Snapshot Registry

### Registry Header

```go
type SnapshotRegistry struct {
    Magic            [8]byte    // "GCLSSNAP"
    Version          uint32
    MaxSnapshots     uint32     // Default: 256
    ActiveSnapshots  uint32     // Current count
    NextSnapshotID   uint64     // Monotonically increasing
    RootDescriptor   uint64     // Offset to first descriptor
    Checksum         uint32
}
```

### Snapshot Descriptor

```go
type SnapshotDescriptor struct {
    // Identity
    SnapshotID       uint64     // Unique identifier
    ParentSnapshotID uint64     // 0 if root snapshot

    // Target
    TargetInodeNum   uint64     // Inode this snapshot captures
    TargetType       uint8      // File or Directory subtree

    // State
    State            SnapshotState
    Flags            SnapshotFlags

    // Timestamps
    CreatedAt        int64      // Unix timestamp
    DeletedAt        int64      // 0 if not deleted

    // Extent tracking
    ExtentTreeRoot   uint64     // Root of CoW extent tree

    // Space accounting
    ExclusiveBlocks  uint64     // Blocks only this snapshot references
    SharedBlocks     uint64     // Blocks shared with parent/children
    LogicalSize      uint64     // Apparent size

    // Metadata
    Name             [64]byte   // User-provided name
    Description      [256]byte  // Optional description
}

type SnapshotState uint8
const (
    SnapshotStateCreating   SnapshotState = 0  // Creation in progress
    SnapshotStateActive     SnapshotState = 1  // Ready for use
    SnapshotStateDeleting   SnapshotState = 2  // Deletion in progress
    SnapshotStateOrphaned   SnapshotState = 3  // Parent deleted, pending cleanup
)

type SnapshotFlags uint32
const (
    SnapshotFlagReadOnly    SnapshotFlags = 1 << 0  // Immutable snapshot
    SnapshotFlagWritable    SnapshotFlags = 1 << 1  // Writable clone
    SnapshotFlagAutoDelete  SnapshotFlags = 1 << 2  // Delete when parent updates
)
```

---

## 4. Snapshot Tree Structure

```
                    ┌──────────────┐
                    │   Original   │
                    │   File/Dir   │
                    │  (inode 42)  │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
        ┌─────▼─────┐ ┌────▼────┐ ┌─────▼─────┐
        │ Snapshot  │ │Snapshot │ │ Snapshot  │
        │    A      │ │   B     │ │    C      │
        │ (ID: 1)   │ │ (ID: 2) │ │ (ID: 3)   │
        └─────┬─────┘ └─────────┘ └─────┬─────┘
              │                         │
        ┌─────▼─────┐             ┌─────▼─────┐
        │  Clone    │             │  Clone    │
        │  A1       │             │  C1       │
        │ (ID: 4)   │             │ (ID: 5)   │
        └───────────┘             └───────────┘

Relationships:
- Snapshot A, B, C share original blocks
- Clone A1 can diverge from Snapshot A
- Clone C1 can diverge from Snapshot C
```

---

## 5. Inode Extensions for Snapshots

```go
type Inode struct {
    // ... existing fields ...

    // Snapshot tracking
    HasSnapshot      bool       // True if any snapshot references this inode
    SnapshotCount    uint32     // Number of snapshots
    FirstSnapshotID  uint64     // ID of oldest snapshot
    LastSnapshotID   uint64     // ID of newest snapshot

    // CoW state
    CoWGeneration    uint64     // Incremented on each CoW operation
}
```

---

## 6. Copy-on-Write Protocol

### Write Path with Snapshots

```go
func (f *File) WriteWithCoW(ctx context.Context, offset uint64, data []byte) (int, error) {
    f.lockManager.AcquireExclusive(LockTypeInode, f.inodeNum)
    defer f.lockManager.Release(LockTypeInode, f.inodeNum)

    for logicalBlock := startBlock; logicalBlock <= endBlock; logicalBlock++ {
        extent := f.findExtent(logicalBlock)

        if extent == nil {
            // HOLE: Allocate new block (refcount = 1)
            extent = f.allocateNewBlock(logicalBlock)
            continue
        }

        // Check if block is shared
        refCount := f.refCountTable.Get(extent.DiskBlock)

        if refCount == 1 {
            // EXCLUSIVE: Direct write (fast path)
            f.writeToBlock(extent.DiskBlock, blockOffset, data)
            continue
        }

        // SHARED: Must copy-on-write
        newBlock := f.copyOnWrite(extent, logicalBlock, blockOffset, data)
        f.updateExtent(logicalBlock, newBlock)
    }

    return len(data), nil
}
```

### Copy-on-Write Implementation

```go
func (f *File) copyOnWrite(oldExtent *Extent, logicalBlock uint64, writeOffset int, data []byte) uint64 {
    // 1. Allocate new block
    f.lockManager.AcquireExclusive(LockTypeBitmap, 0)
    newBlock := f.bitmap.AllocateBlock()
    f.lockManager.Release(LockTypeBitmap, 0)

    // 2. Initialize refcount for new block
    f.lockManager.AcquireExclusive(LockTypeRefCount, newBlock/refCountLockGranularity)
    f.refCountTable.Set(newBlock, 1)
    f.lockManager.Release(LockTypeRefCount, newBlock/refCountLockGranularity)

    // 3. Copy existing data if partial write
    if writeOffset > 0 || len(data) < blockSize {
        existingData := f.readBlock(oldExtent.DiskBlock)
        copy(existingData[writeOffset:], data)
        data = existingData
    }

    // 4. Write to new block
    f.writeBlock(newBlock, data)

    // 5. Decrement old block refcount
    f.lockManager.AcquireExclusive(LockTypeRefCount, oldExtent.DiskBlock/refCountLockGranularity)
    oldRef := f.refCountTable.Decrement(oldExtent.DiskBlock)
    if oldRef == 0 {
        // Last reference - free block
        f.bitmap.FreeBlock(oldExtent.DiskBlock)
        f.trimManager.QueueTRIM(oldExtent.DiskBlock, 1)
    }
    f.lockManager.Release(LockTypeRefCount, oldExtent.DiskBlock/refCountLockGranularity)

    // 6. Mark extent as CoW'd
    f.inode.CoWGeneration++

    return newBlock
}
```

---

## 7. Snapshot Creation (O(1))

### Fast Snapshot Algorithm

```go
func (sm *SnapshotManager) CreateSnapshot(ctx context.Context, inodeNum uint64, name string) (*SnapshotDescriptor, error) {
    // 1. Lock snapshot registry and target inode
    sm.lockManager.AcquireExclusive(LockTypeSnapshotRegistry, 0)
    defer sm.lockManager.Release(LockTypeSnapshotRegistry, 0)

    sm.lockManager.AcquireExclusive(LockTypeInode, inodeNum)
    defer sm.lockManager.Release(LockTypeInode, inodeNum)

    inode := sm.inodeTable.Load(inodeNum)

    // 2. Allocate snapshot ID
    snapID := sm.registry.NextSnapshotID
    sm.registry.NextSnapshotID++

    // 3. Create snapshot descriptor
    snap := &SnapshotDescriptor{
        SnapshotID:       snapID,
        ParentSnapshotID: inode.LastSnapshotID,
        TargetInodeNum:   inodeNum,
        State:            SnapshotStateCreating,
        Flags:            SnapshotFlagReadOnly,
        CreatedAt:        time.Now().Unix(),
        ExtentTreeRoot:   inode.ExtentTreeRoot,  // Share extent tree!
        LogicalSize:      inode.Size,
    }
    copy(snap.Name[:], name)

    // 4. Update inode
    inode.HasSnapshot = true
    inode.SnapshotCount++
    inode.LastSnapshotID = snapID
    if inode.FirstSnapshotID == 0 {
        inode.FirstSnapshotID = snapID
    }

    // 5. Write snapshot descriptor
    sm.writeDescriptor(snap)

    // 6. Queue background refcount increment
    sm.queueRefCountUpdate(snap, RefCountOpIncrement)

    // 7. Mark snapshot active
    snap.State = SnapshotStateActive
    sm.writeDescriptor(snap)

    sm.registry.ActiveSnapshots++

    return snap, nil
}
```

### Background Reference Count Update

```go
func (sm *SnapshotManager) processRefCountQueue() {
    for req := range sm.refCountQueue {
        switch req.Op {
        case RefCountOpIncrement:
            sm.incrementBlockRefCounts(req.Snapshot)
        case RefCountOpDecrement:
            sm.decrementBlockRefCounts(req.Snapshot)
        }
    }
}

func (sm *SnapshotManager) incrementBlockRefCounts(snap *SnapshotDescriptor) {
    // Walk extent tree and increment refcount for each block
    extents := sm.walkExtentTree(snap.ExtentTreeRoot)

    for _, extent := range extents {
        for i := uint32(0); i < extent.Length; i++ {
            blockNum := extent.DiskBlock + uint64(i)

            sm.lockManager.AcquireExclusive(LockTypeRefCount, blockNum/refCountLockGranularity)
            sm.refCountTable.Increment(blockNum)
            sm.lockManager.Release(LockTypeRefCount, blockNum/refCountLockGranularity)
        }

        snap.SharedBlocks += uint64(extent.Length)
    }

    sm.writeDescriptor(snap)
}
```

---

## 8. Snapshot Deletion

### Deletion Strategy

```
┌─────────────────────────────────────────────────────────────┐
│               Snapshot Deletion Scenarios                    │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Case 1: Leaf Snapshot (no children)                         │
│  ─────────────────────────────────                           │
│  Before:  [Parent] ──▶ [Snap A] ──▶ [Snap B (leaf)]         │
│  After:   [Parent] ──▶ [Snap A]                              │
│  Action:  Decrement all block refcounts, free if refcount=0  │
│                                                              │
│  Case 2: Middle Snapshot (has children)                      │
│  ─────────────────────────────────────                       │
│  Before:  [Parent] ──▶ [Snap A] ──▶ [Snap B]                │
│  After:   [Parent] ──▶ [Snap B]                              │
│  Action:  Merge unique blocks to child, update parent refs   │
│                                                              │
│  Case 3: Root Snapshot (first in chain)                      │
│  ─────────────────────────────────────                       │
│  Before:  [Snap A (root)] ──▶ [Snap B] ──▶ [Snap C]         │
│  After:   [Snap B (new root)] ──▶ [Snap C]                  │
│  Action:  Transfer ownership to first child                  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Delete Leaf Snapshot

```go
func (sm *SnapshotManager) DeleteLeafSnapshot(ctx context.Context, snapID uint64) error {
    sm.lockManager.AcquireExclusive(LockTypeSnapshot, snapID)
    defer sm.lockManager.Release(LockTypeSnapshot, snapID)

    snap := sm.loadDescriptor(snapID)
    if snap.State != SnapshotStateActive {
        return ErrSnapshotNotActive
    }

    // Verify no children
    if sm.hasChildren(snapID) {
        return ErrSnapshotHasChildren
    }

    // Mark deleting
    snap.State = SnapshotStateDeleting
    sm.writeDescriptor(snap)

    // Walk extent tree and decrement refcounts
    extents := sm.walkExtentTree(snap.ExtentTreeRoot)
    freedBlocks := uint64(0)

    for _, extent := range extents {
        for i := uint32(0); i < extent.Length; i++ {
            blockNum := extent.DiskBlock + uint64(i)

            sm.lockManager.AcquireExclusive(LockTypeRefCount, blockNum/refCountLockGranularity)
            newRef := sm.refCountTable.Decrement(blockNum)

            if newRef == 0 {
                // Last reference - free block
                sm.bitmap.FreeBlock(blockNum)
                sm.trimManager.QueueTRIM(blockNum, 1)
                freedBlocks++
            }

            sm.lockManager.Release(LockTypeRefCount, blockNum/refCountLockGranularity)
        }
    }

    // Update parent snapshot
    if snap.ParentSnapshotID != 0 {
        parent := sm.loadDescriptor(snap.ParentSnapshotID)
        // Recompute shared blocks
        sm.updateSharedBlocks(parent)
    }

    // Update target inode
    sm.lockManager.AcquireExclusive(LockTypeInode, snap.TargetInodeNum)
    inode := sm.inodeTable.Load(snap.TargetInodeNum)
    inode.SnapshotCount--
    if inode.SnapshotCount == 0 {
        inode.HasSnapshot = false
        inode.FirstSnapshotID = 0
        inode.LastSnapshotID = 0
    }
    sm.inodeTable.Save(inode)
    sm.lockManager.Release(LockTypeInode, snap.TargetInodeNum)

    // Remove descriptor
    sm.removeDescriptor(snapID)
    sm.registry.ActiveSnapshots--

    return nil
}
```

### Delete Middle Snapshot (Merge-Up)

```go
func (sm *SnapshotManager) DeleteMiddleSnapshot(ctx context.Context, snapID uint64) error {
    snap := sm.loadDescriptor(snapID)
    children := sm.getChildren(snapID)
    firstChild := children[0]

    // Mark deleting
    snap.State = SnapshotStateDeleting
    sm.writeDescriptor(snap)

    // Find blocks unique to this snapshot (not in parent or children)
    uniqueBlocks := sm.findUniqueBlocks(snap)

    // Transfer unique blocks to first child
    for _, blockNum := range uniqueBlocks {
        // Block is now owned by child
        // No refcount change needed - just update extent tree
        sm.transferExtent(snap.ExtentTreeRoot, firstChild.ExtentTreeRoot, blockNum)
    }

    // Update children to point to our parent
    for _, child := range children {
        child.ParentSnapshotID = snap.ParentSnapshotID
        sm.writeDescriptor(child)
    }

    // Decrement refcounts for shared blocks
    sharedBlocks := sm.findSharedBlocks(snap)
    for _, blockNum := range sharedBlocks {
        sm.lockManager.AcquireExclusive(LockTypeRefCount, blockNum/refCountLockGranularity)
        sm.refCountTable.Decrement(blockNum)
        sm.lockManager.Release(LockTypeRefCount, blockNum/refCountLockGranularity)
    }

    // Remove descriptor
    sm.removeDescriptor(snapID)
    sm.registry.ActiveSnapshots--

    return nil
}
```

---

## 9. Writable Clones

### Clone Creation

```go
func (sm *SnapshotManager) CreateWritableClone(ctx context.Context, snapID uint64, name string) (*SnapshotDescriptor, error) {
    parentSnap := sm.loadDescriptor(snapID)

    // Create new snapshot with writable flag
    clone := &SnapshotDescriptor{
        SnapshotID:       sm.registry.NextSnapshotID,
        ParentSnapshotID: snapID,
        TargetInodeNum:   parentSnap.TargetInodeNum,
        State:            SnapshotStateActive,
        Flags:            SnapshotFlagWritable,  // Key difference
        CreatedAt:        time.Now().Unix(),
        ExtentTreeRoot:   sm.cloneExtentTree(parentSnap.ExtentTreeRoot),
    }

    sm.registry.NextSnapshotID++

    // Increment refcounts for all blocks
    sm.queueRefCountUpdate(clone, RefCountOpIncrement)

    sm.writeDescriptor(clone)
    sm.registry.ActiveSnapshots++

    return clone, nil
}
```

### Writing to Clone

```go
func (clone *WritableClone) Write(ctx context.Context, offset uint64, data []byte) (int, error) {
    // Same as WriteWithCoW - CoW on shared blocks
    return clone.file.WriteWithCoW(ctx, offset, data)
}
```

---

## 10. Space Accounting

```go
type SnapshotSpaceStats struct {
    TotalSnapshots    uint32

    // Block counts
    ExclusiveBlocks   uint64  // Blocks with refcount = 1
    SharedBlocks      uint64  // Blocks with refcount > 1
    TotalBlocks       uint64  // Sum of all snapshot logical blocks

    // Size calculations
    LogicalSize       uint64  // Sum of all snapshot logical sizes
    PhysicalSize      uint64  // Actual disk usage (unique blocks only)
    SavedBySharing    uint64  // Space saved by deduplication

    // Per-snapshot breakdown
    PerSnapshot       []SnapshotStats
}

type SnapshotStats struct {
    SnapshotID        uint64
    LogicalSize       uint64
    ExclusiveSize     uint64  // Only this snapshot's unique blocks
    SharedSize        uint64  // Blocks shared with others
    ReclaimableSize   uint64  // Space freed if deleted (exclusive blocks)
}

func (sm *SnapshotManager) GetSpaceStats() *SnapshotSpaceStats {
    stats := &SnapshotSpaceStats{}

    // Walk refcount table
    for blockNum := uint64(0); blockNum < sm.totalBlocks; blockNum++ {
        ref := sm.refCountTable.Get(blockNum)
        if ref == 0 {
            continue
        }

        stats.TotalBlocks++
        if ref == 1 {
            stats.ExclusiveBlocks++
        } else {
            stats.SharedBlocks++
            stats.SavedBySharing += uint64(ref-1) * blockSize
        }
    }

    stats.PhysicalSize = stats.TotalBlocks * blockSize

    return stats
}
```

---

## 11. Configuration

```go
type SnapshotConfig struct {
    // Limits
    MaxSnapshots          int           // Default: 256
    MaxSnapshotDepth      int           // Default: 32 (chain length)
    MaxClonesPerSnapshot  int           // Default: 64

    // Background processing
    RefCountBatchSize     int           // Default: 1000
    RefCountWorkers       int           // Default: 4

    // Deletion
    AutoPruneOrphans      bool          // Default: true
    OrphanRetention       time.Duration // Default: 24h
}
```

---

## 12. Lock Hierarchy for Snapshots

```
Snapshot Registry Lock
  └─▶ Individual Snapshot Lock
        └─▶ RefCount Region Lock (granular)
              └─▶ Inode Lock
                    └─▶ Extent Lock
```

### Deadlock Prevention

1. Always acquire snapshot registry lock before individual snapshot locks
2. Acquire refcount locks in ascending block number order
3. Never hold inode lock while acquiring snapshot registry lock

---

## 13. Recovery

### Incomplete Snapshot Creation

```go
func (sm *SnapshotManager) RecoverIncompleteCreation(snap *SnapshotDescriptor) {
    if snap.State != SnapshotStateCreating {
        return
    }

    // Check if refcount increment completed
    completed := sm.verifyRefCountsIncremented(snap)

    if completed {
        snap.State = SnapshotStateActive
        sm.writeDescriptor(snap)
    } else {
        // Rollback - decrement any incremented refcounts
        sm.rollbackRefCountIncrement(snap)
        sm.removeDescriptor(snap.SnapshotID)
    }
}
```

### Incomplete Snapshot Deletion

```go
func (sm *SnapshotManager) RecoverIncompleteDeletion(snap *SnapshotDescriptor) {
    if snap.State != SnapshotStateDeleting {
        return
    }

    // Complete the deletion
    sm.completeSnapshotDeletion(snap)
}
```

---

## 14. Integration with Other Systems

### Journal Integration

All snapshot operations are journaled:

```go
type SnapshotJournalOps uint8
const (
    JournalOpSnapshotCreate  SnapshotJournalOps = 20
    JournalOpSnapshotDelete  SnapshotJournalOps = 21
    JournalOpSnapshotUpdate  SnapshotJournalOps = 22
    JournalOpRefCountUpdate  SnapshotJournalOps = 23
    JournalOpCloneCreate     SnapshotJournalOps = 24
)
```

### Cache Integration

Snapshot descriptors are cached with generation counters:

```go
type CachedSnapshot struct {
    Descriptor       SnapshotDescriptor
    Generation       uint64
    CachedAt         time.Time
    LastValidated    time.Time
}
```

### Thin Provisioning Integration

- Snapshots can reference unwritten (lazy-zero) extents
- CoW of unwritten extent creates new unwritten extent
- TRIM respects refcounts (only TRIM when refcount = 0)
