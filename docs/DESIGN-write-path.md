# GoClusterFS Write Path Design

## Overview

This document traces complete write operations through all GoClusterFS subsystems: locking, cache invalidation, journaling, thin provisioning, and copy-on-write. Three scenarios are covered:

1. **Simple write** - Writing to existing block, no snapshots
2. **Write to hole** - Allocate-on-write (thin provisioning)
3. **CoW write** - Writing to shared block (file has snapshot)

---

## 1. Write Path Components

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Write Path Component Flow                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│    FUSE Layer                                                                 │
│        │                                                                      │
│        ▼                                                                      │
│    Lock Manager ──────► Acquire exclusive inode lock (CAS protocol)          │
│        │                                                                      │
│        ▼                                                                      │
│    Cache Manager ─────► Invalidate local cache                               │
│        │                                                                      │
│        ▼                                                                      │
│    Inode Loader ──────► Read inode from disk (O_DIRECT)                      │
│        │                                                                      │
│        ▼                                                                      │
│    Extent Lookup ─────► Find extent for logical block                        │
│        │                                                                      │
│        ├── HOLE? ─────► Block Allocator (thin provisioning)                  │
│        │                                                                      │
│        ▼                                                                      │
│    RefCount Check ────► Check if block is shared (CoW decision)              │
│        │                                                                      │
│        ├── Shared? ───► CoW: Allocate new block, copy, update refs           │
│        │                                                                      │
│        ▼                                                                      │
│    Journal ───────────► Write transaction (commit point)                     │
│        │                                                                      │
│        ▼                                                                      │
│    Data Write ────────► Write data to block (O_DIRECT)                       │
│        │                                                                      │
│        ▼                                                                      │
│    Inode Update ──────► Update inode (generations, timestamps)               │
│        │                                                                      │
│        ▼                                                                      │
│    Journal Checkpoint ► Advance journal head                                 │
│        │                                                                      │
│        ▼                                                                      │
│    Cache Update ──────► Store updated inode in cache                         │
│        │                                                                      │
│        ▼                                                                      │
│    Lock Release ──────► Release exclusive lock                               │
│        │                                                                      │
│        ▼                                                                      │
│    Return to FUSE                                                             │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Scenario 1: Simple Write (Existing Block, No Snapshot)

**Context**: Writing 512KB to offset 2MB in a file with no snapshots.

### Timeline

```
Time    Component           Action
─────────────────────────────────────────────────────────────────────────────────
t0      FUSE Layer          Receive write(fd, buf, 512KB, offset=2MB)

t1      Lock Manager        ACQUIRE EXCLUSIVE LOCK on inode
                            - Read lock slot from disk (O_DIRECT)
                            - CAS: set OwnerNodeUUID, LockMode=Exclusive
                            - Write lock slot, fsync
                            - Lock acquired (if failed, retry with backoff)

t2      Cache Manager       INVALIDATE LOCAL CACHE
                            - Remove inode from cache (will re-read fresh)
                            - Clear any cached extent mappings for this inode

t3      Inode Loader        READ INODE FROM DISK (O_DIRECT)
                            - Calculate offset: InodeTableOffset + (inodeNum × 256)
                            - Read 256 bytes with O_DIRECT
                            - Verify checksum
                            - Result: Size=10MB, HasSnapshot=false

t4      Extent Lookup       FIND EXTENT for offset 2MB
                            - Logical block = 2MB / 1MB = block 2
                            - Search inode.InlineExtents[]: found!
                            - Extent: FileBlock=2, DiskBlock=1047, Length=1
                            - Physical block: 1047

t5      RefCount Check      CHECK REFCOUNT (for CoW decision)
                            - RefCount[1047] = 1 (exclusive, no sharing)
                            - No CoW needed! Direct write path.

t6      Journal             BEGIN TRANSACTION
                            - Allocate TxnSeq (atomic increment)
                            - Build transaction:
                              ┌────────────────────────────────────┐
                              │ TxnHeader                         │
                              │ - Magic: 0x54584E52               │
                              │ - Seq: 1000042                    │
                              │ - EntryCount: 2                   │
                              │ - Checksum: (calculated)          │
                              ├────────────────────────────────────┤
                              │ Entry 1: InodeUpdate              │
                              │ - InodeNum, new ModifyTime        │
                              │ - new DataGeneration              │
                              │ - new MetaGeneration              │
                              ├────────────────────────────────────┤
                              │ Entry 2: DataWrite (intent)       │
                              │ - DiskBlock: 1047                 │
                              │ - Offset within block: 0          │
                              │ - Length: 512KB                   │
                              └────────────────────────────────────┘

t7      Journal             WRITE JOURNAL (O_DIRECT)
                            - Write TxnHeader + Entries to journal ring buffer
                            - fsync() ← COMMIT POINT
                            - If crash after this, recovery will replay

t8      Data Write          WRITE DATA BLOCK (O_DIRECT)
                            - Calculate offset: 1047 × 1MB + DataRegionStart
                            - Write 512KB with O_DIRECT

t9      Inode Update        UPDATE INODE ON DISK
                            - inode.ModifyTime = now()
                            - inode.DataGeneration++ (5→6)
                            - inode.MetaGeneration++ (7→8)
                            - inode.LastModifierNode = thisNodeUUID
                            - Recalculate checksum
                            - Write inode (O_DIRECT), fsync

t10     Journal             CHECKPOINT (advance head)
                            - Mark transaction as committed
                            - Update journal HeadOffset

t11     Cache Manager       UPDATE LOCAL CACHE
                            - Store updated inode in cache
                            - Set DataGeneration=6, MetaGeneration=8
                            - Set LastValidated=now()

t12     Lock Manager        RELEASE LOCK
                            - CAS: clear OwnerNodeUUID, set LockMode=None
                            - Write lock slot, fsync

t13     FUSE Layer          Return success, bytes written = 512KB
```

### I/O Summary

| Operation | Count | Type |
|-----------|-------|------|
| Lock slot read/write | 2 | O_DIRECT |
| Inode read | 1 | O_DIRECT |
| Journal write + fsync | 1 | O_DIRECT + fsync |
| Data write | 1 | O_DIRECT |
| Inode write + fsync | 1 | O_DIRECT + fsync |
| **Total** | **6 I/Os** | **2 fsyncs** |

---

## 3. Scenario 2: Write to Hole (Thin Provisioning)

**Context**: Writing 512KB to offset 50MB in a sparse file (no block allocated).

### Timeline

```
Time    Component           Action
─────────────────────────────────────────────────────────────────────────────────
t0-t3   (Same as Scenario 1: Lock acquire, cache invalidate, inode load)

t4      Extent Lookup       FIND EXTENT for offset 50MB
                            - Logical block = 50MB / 1MB = block 50
                            - Search inode.InlineExtents[]: NOT FOUND
                            - Search extent B-tree: NOT FOUND
                            - HOLE detected!

t5      Block Allocator     ALLOCATE NEW BLOCK
                            - Acquire Block Bitmap Lock
                            - Read bitmap region
                            - Find first free bit (e.g., bit 2048)
                            - Set bit, write bitmap
                            - Release Block Bitmap Lock
                            - New block allocated: DiskBlock=2048

t6      RefCount Init       INITIALIZE REFCOUNT
                            - RefCount[2048] = 1 (new block)
                            - Write refcount entry

t7      Journal             BEGIN TRANSACTION
                              ┌────────────────────────────────────┐
                              │ TxnHeader (EntryCount: 4)         │
                              ├────────────────────────────────────┤
                              │ Entry 1: BlockAlloc               │
                              │ - DiskBlock: 2048                 │
                              ├────────────────────────────────────┤
                              │ Entry 2: ExtentAdd                │
                              │ - InodeNum, FileBlock=50          │
                              │ - DiskBlock=2048, Length=1        │
                              │ - Flags=Allocated                 │
                              ├────────────────────────────────────┤
                              │ Entry 3: RefCountSet              │
                              │ - DiskBlock: 2048, RefCount: 1    │
                              ├────────────────────────────────────┤
                              │ Entry 4: InodeUpdate              │
                              │ - AllocatedBlocks++               │
                              │ - WrittenBlocks++                 │
                              │ - Update generations              │
                              └────────────────────────────────────┘

t8      Journal             WRITE JOURNAL (O_DIRECT), fsync ← COMMIT POINT

t9      Data Write          WRITE DATA BLOCK
                            - Write user data (512KB)
                            - Remaining 512KB: lazy-zero (reads return zeros)

t10     Extent Update       ADD EXTENT TO INODE
                            - Add to InlineExtents[] or extent B-tree
                            - Extent: FileBlock=50, DiskBlock=2048, Length=1

t11     Inode Update        UPDATE INODE
                            - inode.AllocatedBlocks++
                            - inode.WrittenBlocks++
                            - Update generations, timestamps
                            - Write inode, fsync

t12-t13 (Same: Checkpoint, cache update, lock release)
```

### I/O Summary

| Operation | Count | Type |
|-----------|-------|------|
| Lock slot read/write | 2 | O_DIRECT |
| Inode read | 1 | O_DIRECT |
| Bitmap read/write | 2 | O_DIRECT |
| RefCount write | 1 | O_DIRECT |
| Journal write + fsync | 1 | O_DIRECT + fsync |
| Data write | 1 | O_DIRECT |
| Inode write + fsync | 1 | O_DIRECT + fsync |
| **Total** | **9 I/Os** | **2 fsyncs** |

---

## 4. Scenario 3: CoW Write (File Has Snapshot)

**Context**: Writing 512KB to offset 2MB in a file with snapshot. Block is shared (refcount=2).

### Timeline

```
Time    Component           Action
─────────────────────────────────────────────────────────────────────────────────
t0-t4   (Same: Lock, cache invalidate, inode load, extent lookup)
                            - Found extent: DiskBlock=1047

t5      RefCount Check      CHECK REFCOUNT
                            - RefCount[1047] = 2 (shared with snapshot!)
                            - inode.HasSnapshot = true
                            - CoW REQUIRED!

t6      Block Allocator     ALLOCATE NEW BLOCK FOR COW
                            - Lock bitmap, find free block
                            - New block allocated: DiskBlock=2049

t7      Data Copy           COPY EXISTING DATA (for partial write)
                            - Read block 1047 (old data)
                            - Copy to buffer

t8      Data Merge          MERGE USER DATA
                            - Overwrite 512KB at offset 0 in buffer
                            - User data merged with existing

t9      Journal             BEGIN TRANSACTION (CoW)
                              ┌────────────────────────────────────┐
                              │ TxnHeader (EntryCount: 6)         │
                              ├────────────────────────────────────┤
                              │ Entry 1: BlockAlloc (2049)        │
                              ├────────────────────────────────────┤
                              │ Entry 2: RefCountSet              │
                              │ - Block 2049: RefCount=1          │
                              ├────────────────────────────────────┤
                              │ Entry 3: RefCountDecrement        │
                              │ - Block 1047: 2→1                 │
                              ├────────────────────────────────────┤
                              │ Entry 4: ExtentUpdate             │
                              │ - FileBlock=2: 1047→2049          │
                              │ - Clear ExtentFlagShared          │
                              ├────────────────────────────────────┤
                              │ Entry 5: InodeUpdate              │
                              │ - CoWGeneration++                 │
                              │ - Update generations              │
                              ├────────────────────────────────────┤
                              │ Entry 6: DataWrite intent         │
                              └────────────────────────────────────┘

t10     Journal             WRITE JOURNAL (O_DIRECT), fsync ← COMMIT POINT

t11     RefCount Update     UPDATE REFCOUNTS
                            - Lock refcount region
                            - RefCount[1047]-- (2→1, snapshot still has ref)
                            - RefCount[2049] = 1 (new block)
                            - Write refcount entries
                            - Unlock refcount region

t12     Data Write          Write merged data to block 2049 (O_DIRECT)

t13     Extent Update       UPDATE EXTENT MAPPING
                            - Change extent: DiskBlock 1047→2049
                            - Clear ExtentFlagShared

t14     Inode Update        UPDATE INODE
                            - inode.CoWGeneration++
                            - Update timestamps and generations
                            - Write inode, fsync

t15-t16 (Checkpoint, cache update, lock release)
```

### CoW Result

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           After CoW Write                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│ Original file (inode 42):                                                    │
│   Extent[2]: DiskBlock=2049 (NEW), RefCount=1                               │
│   (File now has exclusive access to this block)                             │
│                                                                              │
│ Snapshot:                                                                    │
│   Extent[2]: DiskBlock=1047 (OLD), RefCount=1                               │
│   (Snapshot still points to original data)                                  │
│                                                                              │
│ Block 1047: Still exists, owned by snapshot only                            │
│ Block 2049: New block, owned by original file only                          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### I/O Summary

| Operation | Count | Type |
|-----------|-------|------|
| Lock slot read/write | 2 | O_DIRECT |
| Inode read | 1 | O_DIRECT |
| RefCount read | 1 | O_DIRECT |
| Bitmap read/write | 2 | O_DIRECT |
| Old block read (for copy) | 1 | O_DIRECT |
| Journal write + fsync | 1 | O_DIRECT + fsync |
| RefCount write | 1 | O_DIRECT |
| Data write (new block) | 1 | O_DIRECT |
| Inode write + fsync | 1 | O_DIRECT + fsync |
| **Total** | **11 I/Os** | **2 fsyncs** |

---

## 5. Performance Comparison

| Scenario | Total I/Os | Journal Entries | Fsyncs | Typical Latency |
|----------|------------|-----------------|--------|-----------------|
| Simple write | 6 | 2 | 2 | ~2ms |
| Write to hole | 9 | 4 | 2 | ~3ms |
| CoW write | 11 | 6 | 2 | ~5ms |

### Latency Breakdown

```
Simple Write (~2ms):
├─ Lock acquire: 0.3ms
├─ Inode load: 0.2ms
├─ Journal write + fsync: 0.8ms  ← Bottleneck
├─ Data write: 0.3ms
├─ Inode update + fsync: 0.3ms
└─ Lock release: 0.1ms

CoW Write (~5ms):
├─ Lock acquire: 0.3ms
├─ Inode load: 0.2ms
├─ RefCount check: 0.1ms
├─ Block allocate: 0.4ms
├─ Old block read: 0.8ms  ← Extra for CoW
├─ Journal write + fsync: 0.8ms
├─ RefCount updates: 0.3ms
├─ Data write: 0.3ms
├─ Inode update + fsync: 0.3ms
└─ Lock release: 0.1ms
```

---

## 6. Optimization Opportunities

### Fast Path Optimizations

1. **RefCount == 1 fast path**: Skip CoW entirely when block not shared
2. **Full block writes**: Skip read-modify-write for full 1MB writes
3. **Sequential writes**: Batch multiple blocks in single transaction
4. **Extent coalescing**: Merge adjacent extents to reduce extent count

### Write-Back Optimizations

1. **Delayed allocation**: Batch multiple small writes before allocating
2. **Journal batching**: Combine multiple small transactions
3. **Async checkpoint**: Don't wait for checkpoint on every write

### Future Considerations

1. **Write-back cache**: Cache dirty blocks, flush on fsync
2. **Parallel journal**: Multiple journal streams for high concurrency
3. **NVME optimizations**: Leverage NVMe atomic writes

---

## 7. Error Handling

### Write Failures

```
If any step fails BEFORE journal commit:
├─ Rollback any partial changes
├─ Release locks
└─ Return error to FUSE

If any step fails AFTER journal commit:
├─ Transaction is in journal
├─ Recovery will replay on next mount
├─ No data loss
└─ Return error to FUSE (operation may complete on recovery)
```

### Lock Timeout

```
If lock acquisition times out:
├─ Check if owning node is still alive (heartbeat)
├─ If node dead: steal lock, recover its journal
├─ If node alive: retry with exponential backoff
└─ After max retries: return EBUSY
```

---

## 8. Multi-Node Concurrency

When multiple nodes access the same file concurrently, GoClusterFS uses an **optimistic retry pattern** to handle conflicts without excessive locking overhead.

### Design Philosophy: Pay for What You Use

The concurrency handling is designed to minimize overhead for the common case (single-node access) while safely handling multi-node conflicts:

```
Write Request
    │
    ├─► Block already allocated? ──YES──► Write directly (NO LOCK)
    │                                      │
    │                                      └─► Fast path: ~0.3ms overhead
    │
    └─► Need new block? ──YES──► Acquire inode lock
                                      │
                                      ├─► Check MetaGeneration changed?
                                      │       │
                                      │       ├─► No change: proceed with allocation
                                      │       │
                                      │       └─► Changed: retry (another node modified)
                                      │
                                      └─► Allocate block, write, release lock
```

### Performance Impact by Scenario

| Scenario | Lock Overhead | Retries | Impact |
|----------|---------------|---------|--------|
| Single-node, existing block | None | 0 | **Negligible** |
| Single-node, extending file | One lock per new block | 0 | **Minimal** |
| Multi-node, different files | One lock per new block | 0 | **Minimal** |
| Multi-node, same file, same region | One lock per attempt | 0-3 | **Low** |
| Multi-node, same file, extending | One lock per attempt | 0-3 | **Low** |

### When Locking Occurs

**No inode lock required:**
- Writing to already-allocated blocks
- Reading file data
- Stat operations on cached inodes

**Inode lock required:**
- Allocating new blocks (file extension)
- Updating file size beyond allocated extents
- Modifying extent mappings

### Conflict Detection with MetaGeneration

The `MetaGeneration` field in the inode tracks metadata changes:

```
Timeline: Two nodes extending the same file

Node A                                   Node B
──────                                   ──────
1. Read inode (MetaGen=5)
2. Need block 10, acquire lock
3. Check MetaGen still 5? YES
4. Allocate block 10                     4. Read inode (MetaGen=5)
5. Write extent, MetaGen=6               5. Need block 10, wait for lock...
6. Release lock
                                         6. Acquire lock
                                         7. Re-read inode, MetaGen=6
                                         8. Check block 10: already allocated!
                                         9. Use existing block, release lock
```

### Retry Behavior

When a conflict is detected, the operation retries with fresh inode state:

```go
const MaxWriteRetries = 3

func (f *File) Write(...) error {
    for attempt := 0; attempt < MaxWriteRetries; attempt++ {
        bytesWritten, err := f.writeInternal(ctx, req)
        if err == errRetryWrite {
            continue  // Conflict detected, retry with fresh state
        }
        return err
    }
    return syscall.EAGAIN  // Exceeded retries (very rare)
}
```

**Retry triggers:**
- `MetaGeneration` changed between inode reads
- Target block position already occupied by another node
- Extent conflict detected during allocation

**What does NOT trigger retry:**
- Writes to existing blocks (no conflict possible)
- Sequential single-node writes (no concurrent modifications)

### Typical Performance

For single-node workloads (most common):

```
Sequential file write (single node):
├─ First block: Lock + allocate (~0.5ms extra)
├─ Subsequent writes to same block: No lock (~0ms extra)
├─ Next block: Lock + allocate (~0.5ms extra)
└─ Total overhead: O(blocks_allocated), not O(writes)

Example: Writing 100MB file in 64KB chunks
├─ 1,600 write calls
├─ 100 block allocations (1MB blocks)
├─ 100 lock acquisitions
└─ Lock overhead: ~50ms total (0.03% of typical I/O time)
```

For multi-node concurrent writes to same file:

```
Two nodes writing to different regions:
├─ No conflicts (different blocks)
├─ Each node proceeds independently
└─ Same performance as single-node

Two nodes extending same file simultaneously:
├─ First node succeeds immediately
├─ Second node detects conflict, retries
├─ Retry uses existing block allocated by first node
└─ Additional overhead: ~0.5ms per conflict
```

---

## 9. Cross-References

- **Locking**: See `docs/DESIGN.md` section on CAS locking protocol
- **Journaling**: See `docs/DESIGN-journal.md` for transaction details
- **Cache Coherency**: See `docs/DESIGN-cache.md` for generation counters
- **Thin Provisioning**: See `docs/DESIGN-thin.md` for extent flags
- **Snapshots**: See `docs/DESIGN-snapshot.md` for CoW and refcounting
