# GoClusterFS Design Document v2

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Heartbeat Timeout | 10 seconds | VMs are large files, single-node access pattern, no need for aggressive failover |
| Lock Region | Dynamic allocation | Scales with actual usage, not worst-case |
| Block Allocation | Sub-block for small files | Space efficiency without sacrificing large-file performance |

---

## Updated On-Disk Layout (Complete)

```
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    Block Device Layout                                        │
├────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬──────────────┤
│ Super  │ Node    │ Journal │ Lock    │ Inode   │ Block   │ RefCount│ Snapshot│              │
│ Block  │ Registry│ Region  │ Bitmap  │ Table   │ Bitmap  │ Table   │ Registry│ Data Blocks  │
│        │ + HB    │         │ +Records│         │         │         │         │ + SFB Region │
├────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼──────────────┤
│ 4KB    │ 1MB     │ 256MB   │ Dynamic │ Variable│ Variable│ Variable│ Variable│  Remainder   │
│Block 0 │ Blocks  │ 64×4MB  │         │         │         │ 8B/block│         │  (1MB each)  │
│        │ 1-255   │         │         │         │         │         │         │              │
└────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴──────────────┘

Key:
- Journal Region: Per-node intent logs (4MB × 64 nodes = 256MB)
- RefCount Table: Block reference counting for snapshots (8 bytes per data block)
- Snapshot Registry: Snapshot descriptors and metadata
- SFB Region: Sub-block allocation within data blocks
```

### Detailed Region Sizes (for 1TB volume)

| Region | Size | Notes |
|--------|------|-------|
| Superblock | 4KB | Volume metadata, checksums |
| Node Registry + Heartbeat | 1MB | 64 node slots × 4KB + heartbeat records |
| Journal Region | 256MB | 64 nodes × 4MB per-node journal |
| Lock Bitmap + Records | ~16MB | Dynamic: ~64K lock slots × 256 bytes |
| Inode Table | ~256MB | 1M inodes × 256 bytes |
| Block Bitmap | ~128KB | 1M blocks / 8 bits per byte |
| RefCount Table | 8MB | 1M blocks × 8 bytes |
| Snapshot Registry | ~1MB | Up to 256 snapshots × 4KB |
| **Total Metadata Overhead** | **~530MB** | ~0.05% of 1TB volume |
| Data Blocks | ~999.5GB | 1MB blocks (includes SFB containers) |

---

## 1. Dynamic Lock Allocation

Instead of fixed lock slots, we dynamically allocate lock records from a pool.

### Lock Region Structure

```go
type LockRegionHeader struct {
    Magic           [8]byte    // "GCLSLOCKS"
    Version         uint32

    // Pool management
    TotalSlots      uint32     // Total lock slots available
    UsedSlots       uint32     // Currently allocated slots
    FreeListHead    uint32     // First free slot index (linked list)

    // Bitmap for fast allocation (1 bit per slot)
    // Follows header: ceil(TotalSlots/8) bytes

    Checksum        uint32
}

type LockRecord struct {
    // Allocation state
    Allocated       bool       // Is this slot in use?
    NextFree        uint32     // If free, index of next free slot (linked list)

    // Lock identity (when allocated)
    ResourceType    uint32     // LockResourceType enum
    ResourceID      uint64     // Inode number, extent ID, etc.

    // Ownership
    OwnerNodeUUID   [16]byte
    OwnerGeneration uint64

    // Lock state
    LockMode        uint32     // None, Shared, Exclusive
    LockSeq         uint64     // For CAS operations
    AcquireTime     int64

    // Shared lock support
    ShareCount      uint32     // Number of shared holders
    SharedOwners    [8]NodeRef // Up to 8 simultaneous readers

    Checksum        uint32
}

type NodeRef struct {
    NodeUUID    [16]byte
    Generation  uint64
}

// Lock slot size: 256 bytes (aligned for O_DIRECT)
const LockSlotSize = 256
```

### Lock Allocation Protocol

```
┌─────────────────────────────────────────────────────────────────┐
│                  Dynamic Lock Allocation                         │
└─────────────────────────────────────────────────────────────────┘

To acquire a lock on resource R:

1. HASH: slot_hint = hash(ResourceType, ResourceID) % TotalSlots
   └─▶ Provides locality for same resource

2. SEARCH: Starting from slot_hint, find existing lock for R
   └─▶ Linear probe with wrap-around
   └─▶ If found → attempt to acquire (see locking protocol)

3. If not found, ALLOCATE new slot:
   a. Read lock region header
   b. Pop slot from free list (CAS on FreeListHead)
   c. Initialize lock record for resource R
   d. Write lock record
   e. Update header (UsedSlots++, FreeListHead = NextFree)

4. ACQUIRE lock using CAS protocol (see v1 design)

Lock Release:
1. Clear lock record (LockMode = None, Allocated = false)
2. Push slot back to free list
3. Update header (UsedSlots--, link into FreeListHead)
```

### Lock Lookup Optimization

```go
// In-memory hash table for fast lock lookups
// Rebuilt on mount by scanning lock region

type LockIndex struct {
    mu       sync.RWMutex
    byResource map[LockKey]uint32  // ResourceType+ID → slot index
}

type LockKey struct {
    Type uint32
    ID   uint64
}

// On mount: scan all allocated lock slots, build index
// On lock create: add to index
// On lock free: remove from index
// Periodically: verify index matches on-disk state
```

---

## 2. Sub-Block Allocation (Small File Blocks)

For files smaller than a threshold (e.g., 64KB), we use sub-block allocation to avoid wasting a full 1MB block.

### Tiered Allocation Strategy

```
┌─────────────────────────────────────────────────────────────────┐
│                  File Size → Allocation Strategy                 │
└─────────────────────────────────────────────────────────────────┘

File Size              Allocation Method
─────────────────────────────────────────────────────
0 - 64KB               Sub-block (SFB) allocation
64KB - 1MB             Single data block (partial use OK)
1MB+                   Extent-based (multiple 1MB blocks)
```

### Small File Block (SFB) Design

Inspired by VMFS's sub-block allocation:

```go
// SFB Container: A 1MB block subdivided into smaller chunks
type SFBContainer struct {
    Header SFBHeader
    // Followed by allocation bitmap
    // Followed by sub-blocks
}

type SFBHeader struct {
    Magic           [8]byte    // "GCLSSFB\0"
    ContainerID     uint64     // Unique ID for this container

    // Sub-block configuration
    SubBlockSize    uint32     // Size of each sub-block (e.g., 4KB, 8KB)
    TotalSubBlocks  uint32     // Total sub-blocks in container
    FreeSubBlocks   uint32     // Available sub-blocks

    // Bitmap offset (relative to container start)
    BitmapOffset    uint32     // Where allocation bitmap starts
    DataOffset      uint32     // Where sub-block data starts

    Checksum        uint32
}

// Sub-block sizes (configurable per-container)
const (
    SFBSize4K  = 4096      // 256 sub-blocks per 1MB container
    SFBSize8K  = 8192      // 128 sub-blocks per 1MB container
    SFBSize16K = 16384     // 64 sub-blocks per 1MB container
    SFBSize64K = 65536     // 16 sub-blocks per 1MB container
)
```

### SFB Layout Within Container

```
┌─────────────────────────────────────────────────────────────────┐
│                    1MB SFB Container (4KB sub-blocks)            │
├──────────┬──────────┬───────────────────────────────────────────┤
│ Header   │ Bitmap   │              Sub-Blocks                    │
│ 512B     │ 32B      │         256 × 4KB = 1,048,064B            │
├──────────┼──────────┼─────┬─────┬─────┬─────┬───────────────────┤
│          │ 1 bit    │ SB0 │ SB1 │ SB2 │ ... │       SB255       │
│          │ per SB   │ 4KB │ 4KB │ 4KB │     │        4KB        │
└──────────┴──────────┴─────┴─────┴─────┴─────┴───────────────────┘
```

### Inode Changes for Sub-Block Support

```go
type Inode struct {
    // ... existing fields ...

    // Storage type
    StorageType     uint8      // StorageType enum

    // For sub-block storage (StorageTypeSFB)
    SFBContainerID  uint64     // Which SFB container
    SFBOffset       uint32     // Sub-block index within container
    SFBCount        uint32     // Number of contiguous sub-blocks used

    // For extent storage (StorageTypeExtent) - existing fields
    ExtentCount     uint32
    Extents         [4]Extent
    ExtentTreeRoot  uint64
}

type StorageType uint8
const (
    StorageTypeInline  StorageType = 0  // Data in inode (tiny files, future)
    StorageTypeSFB     StorageType = 1  // Sub-block allocation
    StorageTypeExtent  StorageType = 2  // Full block extents
)
```

### SFB Allocation Protocol

```
┌─────────────────────────────────────────────────────────────────┐
│              Small File Allocation Protocol                      │
└─────────────────────────────────────────────────────────────────┘

Creating a small file (< 64KB):

1. CALCULATE sub-blocks needed:
   subBlocksNeeded = ceil(fileSize / SubBlockSize)

2. FIND suitable SFB container:
   a. Check cached "active" container for this sub-block size
   b. If no space, scan SFB containers for one with room
   c. If none found, allocate new 1MB block as SFB container

3. LOCK the SFB container (exclusive)

4. ALLOCATE sub-blocks:
   a. Scan bitmap for 'subBlocksNeeded' contiguous free sub-blocks
   b. If not contiguous, find best fit (may need multiple ranges)
   c. Set bitmap bits

5. UPDATE container header (FreeSubBlocks -= subBlocksNeeded)

6. RELEASE SFB container lock

7. UPDATE inode:
   - StorageType = StorageTypeSFB
   - SFBContainerID = containerID
   - SFBOffset = firstSubBlockIndex
   - SFBCount = subBlocksNeeded
```

### Growing Files: SFB → Extent Migration

```
┌─────────────────────────────────────────────────────────────────┐
│                   File Growth Strategy                           │
└─────────────────────────────────────────────────────────────────┘

When file grows beyond SFB threshold (64KB):

1. ALLOCATE new extent block(s)

2. COPY data from SFB sub-blocks to new extent

3. UPDATE inode:
   - StorageType = StorageTypeExtent
   - Clear SFB fields
   - Set extent fields

4. FREE SFB sub-blocks:
   a. Lock SFB container
   b. Clear bitmap bits
   c. Update container header
   d. Release lock

This is transparent to the application.
```

---

## 3. Updated Inode Structure (Complete)

```go
type Inode struct {
    // Identity
    InodeNum        uint64

    // Type and permissions
    Mode            uint32     // File type + permission bits
    Flags           uint32     // FS_IMMUTABLE_FL, etc.

    // Ownership
    UID             uint32
    GID             uint32

    // Timestamps (nanoseconds since epoch)
    AccessTime      int64
    ModifyTime      int64
    ChangeTime      int64
    CreateTime      int64

    // Size and blocks (thin provisioning - see DESIGN-thin.md)
    Size            uint64     // Logical file size
    AllocatedBlocks uint64     // Physical blocks (including unwritten)
    WrittenBlocks   uint64     // Actually written blocks

    // Links
    LinkCount       uint32

    // Cache coherency (see DESIGN-cache.md)
    DataGeneration   uint64    // Incremented on ANY data modification
    MetaGeneration   uint64    // Incremented on metadata change
    LastModifierNode [16]byte  // UUID of node that last modified
    LastModifyLockSeq uint64   // Lock sequence at modification time

    // Provisioning mode (see DESIGN-thin.md)
    ProvisioningMode ProvisioningMode  // Thin, ThickLazy, ThickEager

    // Storage type
    StorageType     StorageType

    // Sub-block storage (when StorageType == SFB)
    SFBContainerID  uint64     // Container block number
    SFBOffset       uint32     // Starting sub-block index
    SFBCount        uint32     // Number of sub-blocks

    // Extent storage (when StorageType == Extent)
    ExtentCount     uint32
    InlineExtents   [4]Extent  // Up to 4 inline (covers 4MB)
    ExtentTreeRoot  uint64     // For files > 4MB: root of extent B-tree

    // Snapshot tracking (see DESIGN-snapshot.md)
    HasSnapshot      bool      // True if any snapshot references this inode
    SnapshotCount    uint32    // Number of snapshots
    FirstSnapshotID  uint64    // ID of oldest snapshot
    LastSnapshotID   uint64    // ID of newest snapshot
    CoWGeneration    uint64    // Incremented on each CoW operation

    // Extended attributes
    XattrBlock      uint64

    // Checksum
    Checksum        uint32
}

const InodeSize = 256  // Aligned for O_DIRECT operations

type ProvisioningMode uint8
const (
    ProvisioningThin       ProvisioningMode = 0  // Allocate on demand
    ProvisioningThickLazy  ProvisioningMode = 1  // Pre-allocate, lazy zero
    ProvisioningThickEager ProvisioningMode = 2  // Pre-allocate, immediate zero
)
```

**Cross-References:**
- Cache coherency fields: See `docs/DESIGN-cache.md` for generation counter usage
- Thin provisioning fields: See `docs/DESIGN-thin.md` for block tracking
- Snapshot fields: See `docs/DESIGN-snapshot.md` for CoW and refcounting

---

## 4. SFB Region Management

### SFB Registry (in Superblock Extended Area)

```go
type SFBRegistry struct {
    // Active containers per sub-block size (for fast allocation)
    Active4K        uint64     // Block number of active 4KB container
    Active8K        uint64     // Block number of active 8KB container
    Active16K       uint64     // Block number of active 16KB container
    Active64K       uint64     // Block number of active 64KB container

    // Total containers
    ContainerCount  uint32

    // Container list (stored in dedicated blocks if > inline capacity)
    ContainerListBlock uint64
}
```

### Choosing Sub-Block Size

```
┌─────────────────────────────────────────────────────────────────┐
│              Sub-Block Size Selection                            │
└─────────────────────────────────────────────────────────────────┘

File Size           Optimal SFB Size    Waste
─────────────────────────────────────────────────
0 - 4KB             4KB                 0-4KB
4KB - 8KB           8KB                 0-4KB
8KB - 16KB          16KB                0-8KB
16KB - 64KB         64KB                0-48KB

Rule: Use smallest sub-block size that fits the file
      (minimize internal fragmentation)
```

---

## 5. Lock Hierarchy (Complete)

```
┌─────────────────────────────────────────────────────────────────┐
│                   Lock Hierarchy (Deadlock Prevention)           │
│              Includes Journaling, Snapshots, RefCounts           │
└─────────────────────────────────────────────────────────────────┘

Level 0: Volume Lock (mount/unmount, fsck)
    │
    └─▶ Level 1: Snapshot Registry Lock (create/delete snapshots)
            │
            └─▶ Level 2: Individual Snapshot Lock (ordered by snapshot ID)
                    │
                    └─▶ Level 3: Journal Lock (per-node, own node only)
                            │
                            └─▶ Level 4: RefCount Region Lock (granular by block range)
                                    │
                                    └─▶ Level 5: Lock Region Lock (allocating/freeing lock slots)
                                            │
                                            └─▶ Level 6: Block Bitmap Lock (allocating 1MB blocks)
                                                    │
                                                    └─▶ Level 7: SFB Container Lock (sub-block allocation)
                                                            │
                                                            └─▶ Level 8: Inode Lock (ordered by inode#)
                                                                    │
                                                                    └─▶ Level 9: Extent Lock (ordered by block#)

Rules:
- Always acquire locks in level order (lower → higher number)
- Within same level, acquire in ID order (lower → higher)
- Never hold higher-level lock while acquiring lower-level
- Journal locks: Each node only locks its own journal slot
- RefCount locks: Granular locking by block range (e.g., 1024 blocks per lock)
```

**Lock Type Reference:**

| Level | Lock Type | Purpose | Document |
|-------|-----------|---------|----------|
| 0 | Volume | Mount/unmount, fsck | DESIGN.md |
| 1 | Snapshot Registry | Snapshot create/delete | DESIGN-snapshot.md |
| 2 | Snapshot | Individual snapshot ops | DESIGN-snapshot.md |
| 3 | Journal | Transaction writes | DESIGN-journal.md |
| 4 | RefCount Region | Reference count updates | DESIGN-snapshot.md |
| 5 | Lock Region | Lock slot allocation | DESIGN-v2.md |
| 6 | Block Bitmap | Block allocation | DESIGN.md |
| 7 | SFB Container | Sub-block allocation | DESIGN-v2.md |
| 8 | Inode | File metadata | DESIGN.md |
| 9 | Extent | Block extent mapping | DESIGN.md |

---

## 6. Operation Flows (Updated)

### Create Small File (< 64KB)

```
1. Acquire parent directory inode lock (exclusive)
2. Allocate new inode number
3. Determine optimal SFB size for initial size hint
4. Acquire SFB container lock for that size
5. Allocate sub-blocks from container
6. Update container bitmap and header
7. Release SFB container lock
8. Initialize inode with SFB storage type
9. Write inode
10. Add directory entry to parent
11. Release parent directory lock
```

### Create Large File (≥ 64KB) or VM Disk

```
1. Acquire parent directory inode lock (exclusive)
2. Allocate new inode number
3. Acquire block bitmap lock (exclusive)
4. Allocate extent blocks
5. Update block bitmap
6. Release block bitmap lock
7. Initialize inode with Extent storage type
8. Write inode
9. Add directory entry to parent
10. Release parent directory lock
```

### Write to Small File (May Trigger Migration)

```
1. Acquire file inode lock (exclusive)
2. Check if write would exceed SFB threshold:

   If staying in SFB:
     a. If need more sub-blocks:
        - Acquire SFB container lock
        - Extend allocation (or relocate within container)
        - Release SFB container lock
     b. Write data to sub-blocks
     c. Update inode size

   If migrating to Extent:
     a. Acquire block bitmap lock
     b. Allocate extent block(s)
     c. Release block bitmap lock
     d. Copy existing data from SFB to extent
     e. Write new data to extent
     f. Acquire SFB container lock
     g. Free old sub-blocks
     h. Release SFB container lock
     i. Update inode (change StorageType, clear SFB fields, set extent)

3. Release file inode lock
```

---

## 7. Fragmentation Management

### SFB Fragmentation

Over time, SFB containers may become fragmented (scattered free sub-blocks).

```go
type SFBDefragStrategy int
const (
    // Online defrag: relocate files during normal operation
    SFBDefragOnline SFBDefragStrategy = 1

    // Background defrag: separate goroutine during idle periods
    SFBDefragBackground SFBDefragStrategy = 2

    // On-demand: only when container is nearly full but fragmented
    SFBDefragOnDemand SFBDefragStrategy = 3
)

// Fragmentation metric
func (c *SFBContainer) FragmentationRatio() float64 {
    if c.FreeSubBlocks == 0 {
        return 0
    }
    // Count number of free "runs" vs total free
    // Higher ratio = more fragmented
    freeRuns := c.countFreeRuns()
    return float64(freeRuns) / float64(c.FreeSubBlocks)
}
```

### When to Create New SFB Container

```
Create new container when:
1. No existing container has enough contiguous space, AND
2. Existing containers have < 10% free space, OR
3. Existing containers fragmentation ratio > 0.8

This avoids creating many sparse containers.
```

---

## 8. Recovery Additions

### SFB Container Recovery

On mount, validate SFB containers:

```
1. Scan all SFB containers
2. For each container:
   a. Verify header checksum
   b. Rebuild bitmap from allocated inodes if corrupt
   c. Update FreeSubBlocks count
3. Rebuild active container pointers
```

### Orphan Sub-Block Detection

```
During fsck or mount:
1. Build set of all SFB allocations from inodes
2. Scan all SFB container bitmaps
3. Any allocated sub-blocks not referenced by inodes = orphans
4. Free orphan sub-blocks
```

---

## 9. Performance Characteristics

### Expected Performance

| Operation | Small File (SFB) | Large File (Extent) |
|-----------|------------------|---------------------|
| Create | 2-3 I/Os | 3-4 I/Os |
| Read 4KB | 1 I/O | 1 I/O |
| Write 4KB | 1-2 I/Os | 1 I/O |
| Delete | 2-3 I/Os | 2-3 I/Os |
| Grow past threshold | 4-6 I/Os (migration) | N/A |

### Space Efficiency

| File Size | Old Design (1MB blocks) | New Design (SFB) |
|-----------|------------------------|------------------|
| 1KB | 1MB (0.1% efficient) | 4KB (25% efficient) |
| 10KB | 1MB (1% efficient) | 12KB (83% efficient) |
| 100KB | 1MB (10% efficient) | 1MB (10% efficient)* |
| 10MB | 10MB (100% efficient) | 10MB (100% efficient) |

*100KB files use extent allocation, so full block is allocated but only 100KB used.

---

## 10. Configuration (Updated)

```go
type Config struct {
    // Device
    DevicePath      string

    // Geometry
    DataBlockSize   int        // Default: 1MB (1048576)
    MaxNodes        int        // Default: 64
    MaxInodes       uint64     // Default: 1M

    // Sub-block allocation
    SFBEnabled      bool       // Default: true
    SFBThreshold    int        // Max file size for SFB (default: 65536)
    SFBDefaultSize  int        // Default sub-block size (default: 4096)

    // Dynamic locks
    InitialLockSlots  int      // Starting lock pool size (default: 1024)
    MaxLockSlots      int      // Maximum locks (default: 1M)

    // Heartbeat
    HeartbeatInterval time.Duration  // Default: 1s
    HeartbeatTimeout  time.Duration  // Default: 10s

    // Locking
    LockTimeout      time.Duration  // Default: 30s
    LockRetryBase    time.Duration  // Default: 10ms
    LockRetryMax     time.Duration  // Default: 1s

    // Cache
    InodeCacheSize   int    // Default: 10000
    SFBCacheSize     int    // Default: 100 (active containers)
}
```

---

## 11. Implementation Phases (Complete)

### Phase 1: Core Infrastructure (Foundation)
- [ ] O_DIRECT device I/O wrapper with aligned buffers
- [ ] Superblock read/write with CRC32C
- [ ] Node registry and heartbeat (1s interval, 10s timeout)
- [ ] Basic CAS locking protocol

### Phase 2: Basic Filesystem
- [ ] Inode table with 256-byte inodes
- [ ] Block bitmap allocation
- [ ] Extent-based file storage
- [ ] Directory operations (flat initially)
- [ ] FUSE integration

### Phase 3: Journaling (see DESIGN-journal.md)
- [ ] Per-node journal region (4MB each)
- [ ] Transaction serialization with checksums
- [ ] Write-ahead logging for all metadata ops
- [ ] Self-recovery on mount
- [ ] Cross-node recovery on heartbeat timeout

### Phase 4: Cache Coherency (see DESIGN-cache.md)
- [ ] Add DataGeneration/MetaGeneration to inodes
- [ ] LRU inode cache with TTL validation
- [ ] Lock-based cache invalidation
- [ ] Extent mapping cache

### Phase 5: Thin Provisioning (see DESIGN-thin.md)
- [ ] ExtentFlags with Unwritten support
- [ ] Allocate-on-write for holes
- [ ] fallocate() with lazy zeroing
- [ ] TRIM manager with batching
- [ ] Accurate space reporting (stat/statfs)

### Phase 6: Sub-Block Allocation
- [ ] SFB containers (4KB/8KB/16KB/64KB)
- [ ] Automatic SFB → Extent migration at 64KB
- [ ] SFB container locking
- [ ] Dynamic lock allocation (moved from earlier)

### Phase 7: Snapshots (see DESIGN-snapshot.md)
- [ ] Reference count table (8 bytes/block)
- [ ] Snapshot registry and descriptors
- [ ] CoW write path
- [ ] Fast snapshot creation (O(1))
- [ ] Snapshot deletion (leaf + merge-up)
- [ ] Writable clones

### Phase 8: Production Hardening
- [ ] Intent logging for atomic operations
- [ ] fsck / consistency verification
- [ ] Defragmentation (SFB containers)
- [ ] Performance tuning
- [ ] Multi-node integration testing

---

## 12. Design Document Index

| Document | Contents |
|----------|----------|
| `DESIGN.md` | Core architecture: superblock, node registry, CAS locks, heartbeat, extents |
| `DESIGN-v2.md` | This document: dynamic locks, SFB, integrated layout, complete inode |
| `DESIGN-journal.md` | Journaling: per-node intent logs, write-ahead protocol, recovery |
| `DESIGN-cache.md` | Cache coherency: generation counters, TTL, invalidation protocol |
| `DESIGN-thin.md` | Thin provisioning: extent flags, lazy zeroing, TRIM manager |
| `DESIGN-snapshot.md` | Snapshots: refcount table, CoW protocol, snapshot create/delete |
