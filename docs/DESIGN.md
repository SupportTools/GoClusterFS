# GoClusterFS Design Document

## Overview

GoClusterFS is a VMFS-inspired clustered filesystem that enables multiple nodes to simultaneously access shared block storage. Like VMFS, it uses **on-disk locking** with no central metadata server—all coordination happens through the shared storage itself.

## Design Goals

1. **Shared-nothing coordination** - No external services required (no etcd, no lock server)
2. **Crash resilient** - Node failures are detected and locks are reclaimed
3. **Large file support** - Extent-based allocation for multi-GB files
4. **POSIX-like semantics** - Standard filesystem operations
5. **VM-workload optimized** - Efficient for large, sparse files

---

## On-Disk Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Block Device Layout                                │
├─────────┬───────────┬───────────┬────────────┬────────────┬─────────────────┤
│ Super   │ Node      │ Lock      │ Inode      │ Block      │                 │
│ Block   │ Registry  │ Region    │ Table      │ Bitmap     │   Data Blocks   │
│         │ + HB      │           │            │            │                 │
├─────────┼───────────┼───────────┼────────────┼────────────┼─────────────────┤
│ 4KB     │ 1MB       │ 16MB      │ Variable   │ Variable   │    Remainder    │
│ Block 0 │ Blocks    │ Blocks    │            │            │                 │
│         │ 1-255     │ 256-4351  │            │            │                 │
└─────────┴───────────┴───────────┴────────────┴────────────┴─────────────────┘
```

### Block Size Strategy

| Region | Block Size | Rationale |
|--------|-----------|-----------|
| Superblock | 4KB | Single atomic read/write |
| Node Registry | 4KB per node | O_DIRECT alignment, atomic heartbeat |
| Lock Region | 512B per lock | Sector-atomic writes |
| Data Blocks | 1MB | Optimized for large files (VM images) |

---

## 1. Superblock (Block 0)

```go
type Superblock struct {
    // Identity
    Magic           [8]byte    // "GCLSTRFS"
    Version         uint32     // Filesystem version (1)
    UUID            [16]byte   // Unique filesystem ID

    // Geometry
    BlockSize       uint32     // Data block size (1MB default)
    TotalBlocks     uint64     // Total data blocks

    // Region Pointers (byte offsets)
    NodeRegionStart uint64     // Start of node registry
    NodeRegionSize  uint64     // Size in bytes
    LockRegionStart uint64     // Start of lock region
    LockRegionSize  uint64     // Size in bytes
    InodeTableStart uint64     // Start of inode table
    InodeTableSize  uint64     // Size in bytes
    BitmapStart     uint64     // Start of block bitmap
    BitmapSize      uint64     // Size in bytes
    DataRegionStart uint64     // Start of data blocks

    // Filesystem State
    MaxNodes        uint32     // Maximum concurrent nodes (default: 64)
    MaxLocks        uint32     // Maximum concurrent locks
    MaxInodes       uint64     // Maximum files

    // Timestamps
    CreateTime      int64      // Filesystem creation time
    MountCount      uint64     // Total mount count

    // Checksum
    Checksum        uint32     // CRC32 of superblock
}
```

---

## 2. Node Registry & Heartbeat Region

Each node that mounts the filesystem registers itself and maintains a heartbeat.

```go
type NodeRecord struct {
    // Identity
    NodeUUID        [16]byte   // Unique node identifier
    NodeName        [64]byte   // Human-readable name (hostname)

    // State
    State           uint32     // NodeState enum
    MountTime       int64      // When this node mounted

    // Heartbeat (updated every HeartbeatInterval)
    HeartbeatSeq    uint64     // Monotonic sequence number
    HeartbeatTime   int64      // Unix timestamp (nanoseconds)

    // Fencing
    Generation      uint64     // Incremented on each mount

    // Reserved for future use
    Reserved        [128]byte

    // Checksum
    Checksum        uint32     // CRC32 of record
}

type NodeState uint32
const (
    NodeStateEmpty    NodeState = 0  // Slot available
    NodeStateMounting NodeState = 1  // Node is mounting
    NodeStateActive   NodeState = 2  // Node is active
    NodeStateStale    NodeState = 3  // Heartbeat expired (being reclaimed)
    NodeStateEvicted  NodeState = 4  // Forcibly evicted
)
```

### Heartbeat Protocol

```
┌─────────────────────────────────────────────────────────────────┐
│                     Heartbeat State Machine                      │
└─────────────────────────────────────────────────────────────────┘

    ┌──────────┐     register()      ┌───────────┐
    │  Empty   │ ─────────────────▶  │ Mounting  │
    └──────────┘                     └───────────┘
                                           │
                                           │ heartbeat success
                                           ▼
    ┌──────────┐    timeout expired   ┌──────────┐
    │  Stale   │ ◀───────────────── │  Active   │ ◀─┐
    └──────────┘                     └──────────┘   │
         │                                │         │
         │ reclaim locks                  └─────────┘
         │ clear slot                     heartbeat()
         ▼
    ┌──────────┐
    │  Empty   │
    └──────────┘
```

**Parameters:**
- `HeartbeatInterval`: 1 second (how often to write heartbeat)
- `HeartbeatTimeout`: 10 seconds (when to consider node dead)
- `HeartbeatRetries`: 3 (failed writes before self-fencing)

---

## 3. On-Disk Lock Region

The lock region contains fixed-size lock records. Each lockable resource has a dedicated slot.

```go
type LockRecord struct {
    // Lock Identity
    ResourceType    uint32     // LockResourceType enum
    ResourceID      uint64     // Inode number, block number, etc.

    // Ownership
    OwnerNodeUUID   [16]byte   // Which node holds the lock
    OwnerGeneration uint64     // Node's generation (prevents stale locks)

    // Lock State
    LockMode        uint32     // LockMode enum
    LockSeq         uint64     // Monotonic sequence (for CAS)
    AcquireTime     int64      // When lock was acquired

    // Waiters (optional, for fairness)
    WaiterCount     uint32     // Number of waiting nodes

    // Checksum
    Checksum        uint32     // CRC32 of record
}

type LockResourceType uint32
const (
    LockTypeVolume   LockResourceType = 1  // Whole filesystem
    LockTypeInode    LockResourceType = 2  // Single file/directory
    LockTypeExtent   LockResourceType = 3  // Block range
    LockTypeBitmap   LockResourceType = 4  // Allocation bitmap region
)

type LockMode uint32
const (
    LockModeNone      LockMode = 0  // Not locked
    LockModeShared    LockMode = 1  // Read lock (multiple holders)
    LockModeExclusive LockMode = 2  // Write lock (single holder)
)
```

### Lock Hierarchy (Deadlock Prevention)

Locks must be acquired in this order to prevent deadlocks:

```
Level 1: Volume Lock
    │
    └─▶ Level 2: Bitmap Lock
            │
            └─▶ Level 3: Inode Lock (ordered by inode number)
                    │
                    └─▶ Level 4: Extent Lock (ordered by block number)
```

---

## 4. O_DIRECT Compare-and-Swap Protocol

Since we can't use true hardware CAS, we simulate it with careful I/O ordering.

### Write Protocol (Acquire Lock)

```
┌─────────────────────────────────────────────────────────────────┐
│              O_DIRECT Lock Acquisition Protocol                  │
└─────────────────────────────────────────────────────────────────┘

Node A wants to acquire lock L:

1. READ lock record L with O_DIRECT
   └─▶ If locked by another ACTIVE node → FAIL (retry with backoff)
   └─▶ If locked by STALE node → proceed to reclaim (see recovery)
   └─▶ If unlocked → proceed

2. PREPARE new lock record:
   - OwnerNodeUUID = self
   - OwnerGeneration = self.Generation
   - LockSeq = old.LockSeq + 1
   - Calculate Checksum

3. WRITE lock record with O_DIRECT + O_SYNC
   └─▶ Ensures write hits disk before returning

4. FENCE: fdatasync() on device
   └─▶ Ensures all previous writes are durable

5. VERIFY: READ lock record again with O_DIRECT
   └─▶ If OwnerNodeUUID == self AND LockSeq matches → SUCCESS
   └─▶ If different → CONFLICT (another node won the race)
       └─▶ Back off with randomized delay, retry from step 1

6. CONFIRM: Write heartbeat immediately
   └─▶ Proves node is alive and lock is valid
```

### Key Implementation Details

```go
type LockManager struct {
    device       *os.File      // Opened with O_DIRECT | O_SYNC
    nodeUUID     [16]byte
    generation   uint64
    lockRegion   uint64        // Byte offset to lock region

    // Aligned buffers for O_DIRECT (must be 512-byte aligned)
    readBuf      []byte        // Aligned read buffer
    writeBuf     []byte        // Aligned write buffer
}

// Key syscall flags
const openFlags = os.O_RDWR | syscall.O_DIRECT | syscall.O_SYNC

// Buffer alignment for O_DIRECT
func alignedBuffer(size int) []byte {
    // Allocate extra space for alignment
    buf := make([]byte, size+512)
    // Find 512-byte aligned offset
    offset := 512 - (uintptr(unsafe.Pointer(&buf[0])) % 512)
    return buf[offset : offset+size]
}
```

### Conflict Resolution

```
┌─────────────────────────────────────────────────────────────────┐
│                    Conflict Resolution                           │
└─────────────────────────────────────────────────────────────────┘

When two nodes race for the same lock:

    Node A                              Node B
       │                                   │
       │ READ: LockSeq=5, unlocked         │
       │                                   │ READ: LockSeq=5, unlocked
       │                                   │
       │ WRITE: LockSeq=6, owner=A         │
       │                                   │ WRITE: LockSeq=6, owner=B
       │                                   │
       │ VERIFY: reads owner=B ✗           │ VERIFY: reads owner=B ✓
       │                                   │
       │ BACKOFF + RETRY                   │ SUCCESS
       │                                   │

Backoff formula: delay = baseDelay * (2^attempt) + random(0, jitter)
- baseDelay: 10ms
- maxDelay: 1s
- jitter: 50ms
```

---

## 5. Inode Table

```go
type Inode struct {
    // Identity
    InodeNum        uint64

    // Type and Mode
    Mode            uint32     // File type + permissions
    Flags           uint32     // Immutable, append-only, etc.

    // Ownership
    UID             uint32
    GID             uint32

    // Timestamps (nanoseconds)
    AccessTime      int64
    ModifyTime      int64
    ChangeTime      int64
    CreateTime      int64

    // Size
    Size            uint64     // File size in bytes
    Blocks          uint64     // Number of allocated blocks

    // Links
    LinkCount       uint32

    // Extent Tree (inline for small files)
    ExtentCount     uint32
    Extents         [4]Extent  // Inline extents (covers up to 4MB)
    ExtentTreeRoot  uint64     // Block pointer to extent tree (for large files)

    // Extended Attributes
    XattrBlock      uint64     // Block pointer to xattrs

    // Checksum
    Checksum        uint32
}

type Extent struct {
    FileBlock   uint64   // Logical block offset in file
    DiskBlock   uint64   // Physical block on device
    Length      uint32   // Number of contiguous blocks
    Flags       uint32   // Unwritten, encrypted, etc.
}
```

### Directory Entries

```go
type DirEntry struct {
    InodeNum    uint64
    EntryType   uint8      // DT_REG, DT_DIR, DT_LNK, etc.
    NameLen     uint8
    Name        [254]byte  // Null-terminated
}
```

---

## 6. Block Allocation Bitmap

```go
type BitmapRegion struct {
    // Each bit represents one data block
    // 0 = free, 1 = allocated
    //
    // For a 1TB volume with 1MB blocks:
    // - 1,048,576 blocks = 131,072 bytes = 128KB bitmap

    Bits []byte
}

// Bitmap operations require holding LockTypeBitmap
```

### Allocation Strategy

1. **First-fit with roaming pointer** - Start search from last allocation point
2. **Per-node allocation zones** - Each node prefers different regions (reduces contention)
3. **Extent coalescing** - Try to extend existing extents before allocating new ones

---

## 7. Recovery & Fencing

### Node Failure Detection

```go
func (nm *NodeManager) MonitorNodes() {
    for {
        time.Sleep(HeartbeatInterval)

        for slot := range nodeSlots {
            record := nm.readNodeRecord(slot)

            if record.State == NodeStateActive {
                age := time.Now().UnixNano() - record.HeartbeatTime

                if age > HeartbeatTimeout {
                    // Node appears dead
                    nm.initiateRecovery(slot, record)
                }
            }
        }
    }
}
```

### Lock Reclamation Protocol

```
┌─────────────────────────────────────────────────────────────────┐
│                    Lock Reclamation Protocol                     │
└─────────────────────────────────────────────────────────────────┘

When Node B detects Node A has failed:

1. MARK node A as STALE
   └─▶ Write NodeStateStale to A's node record

2. WAIT for consensus period (HeartbeatTimeout * 2)
   └─▶ Ensures other nodes also see A as dead

3. SCAN lock region for locks owned by A
   └─▶ Check OwnerNodeUUID matches A
   └─▶ Check OwnerGeneration matches A's last known generation

4. For each lock owned by A:
   a. Verify A is still STALE (didn't recover)
   b. Clear the lock (set LockModeNone)
   c. Log the reclamation

5. CLEAR node A's slot (set NodeStateEmpty)
```

### Self-Fencing

If a node cannot write its heartbeat (I/O errors), it must self-fence:

```go
func (nm *NodeManager) Heartbeat() error {
    for attempt := 0; attempt < HeartbeatRetries; attempt++ {
        err := nm.writeHeartbeat()
        if err == nil {
            return nil
        }
        log.Printf("Heartbeat failed (attempt %d): %v", attempt, err)
        time.Sleep(100 * time.Millisecond)
    }

    // Cannot write heartbeat - we may have lost storage access
    // MUST self-fence to prevent split-brain
    log.Fatal("FATAL: Cannot write heartbeat, self-fencing")
    // Process exits, triggering FUSE unmount
}
```

---

## 8. Operation Flows

### Mount Sequence

```
1. Open block device with O_DIRECT | O_SYNC
2. Read and validate superblock
3. Find empty node slot or reclaim stale slot
4. Write node record (State = Mounting)
5. Acquire volume lock (shared for read-write mount)
6. Write node record (State = Active)
7. Start heartbeat goroutine
8. Start FUSE server
```

### File Create

```
1. Acquire parent directory inode lock (exclusive)
2. Acquire bitmap lock (exclusive)
3. Allocate new inode number
4. Allocate initial extent (if non-empty)
5. Update bitmap
6. Release bitmap lock
7. Write new inode
8. Add directory entry to parent
9. Release parent directory lock
```

### File Write

```
1. Acquire file inode lock (exclusive)
2. If extending file and need new blocks:
   a. Acquire bitmap lock
   b. Allocate blocks
   c. Update inode extents
   d. Release bitmap lock
3. Write data to extent blocks
4. Update inode size/mtime
5. Release file inode lock
```

### File Read

```
1. Acquire file inode lock (shared)
2. Read extent information from inode
3. Read data from extent blocks
4. Release file inode lock
```

---

## 9. Performance Considerations

### Lock Contention Mitigation

1. **Lock granularity** - Use finest granularity needed (inode vs extent)
2. **Read vs write locks** - Use shared locks for reads
3. **Lock timeouts** - Don't wait forever for contested locks
4. **Allocation zones** - Per-node preferred regions reduce bitmap contention

### I/O Optimization

1. **O_DIRECT** - Bypass page cache for lock operations (consistency)
2. **Buffered I/O** - Use page cache for data reads (performance)
3. **Write coalescing** - Batch metadata updates where safe
4. **Readahead** - Prefetch extent data for sequential reads

### Caching

```go
type Cache struct {
    // Inode cache (validated against on-disk lock state)
    inodes     map[uint64]*CachedInode

    // Negative entry cache (recently failed lookups)
    negatives  map[string]time.Time

    // Extent cache (physical block locations)
    extents    map[uint64][]Extent
}

// Cache invalidation on lock release
func (c *Cache) InvalidateInode(ino uint64) {
    delete(c.inodes, ino)
    delete(c.extents, ino)
}
```

---

## 10. Configuration

```go
type Config struct {
    // Device
    DevicePath      string

    // Geometry
    BlockSize       int    // Default: 1MB
    MaxNodes        int    // Default: 64
    MaxFiles        int64  // Default: 1M

    // Heartbeat
    HeartbeatInterval time.Duration  // Default: 1s
    HeartbeatTimeout  time.Duration  // Default: 10s

    // Locking
    LockTimeout      time.Duration  // Default: 30s
    LockRetryDelay   time.Duration  // Default: 10ms
    LockMaxRetries   int            // Default: 100

    // Performance
    InodeCacheSize   int    // Default: 10000
    ExtentCacheSize  int    // Default: 50000
}
```

---

## 11. Future Enhancements

1. **Journaling** - Metadata journal for crash consistency
2. **Snapshots** - Copy-on-write snapshots
3. **Thin provisioning** - Sparse allocation with trim support
4. **Encryption** - Per-file or per-volume encryption
5. **Quotas** - Per-user/group space limits
6. **SCSI PR integration** - Use real SCSI-3 reservations when available

---

## Appendix: Error Codes

```go
type FSError int

const (
    ErrLockTimeout      FSError = 1001
    ErrLockConflict     FSError = 1002
    ErrNodeEvicted      FSError = 1003
    ErrHeartbeatFailed  FSError = 1004
    ErrCorruptMetadata  FSError = 1005
    ErrNoSpace          FSError = 1006
    ErrNoInodes         FSError = 1007
    ErrStaleHandle      FSError = 1008
)
```
