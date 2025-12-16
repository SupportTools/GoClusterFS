# GoClusterFS Journaling and Crash Consistency Design

## Overview

This document details the metadata journaling and crash consistency system for GoClusterFS. The design uses **per-node intent logging** with **write-ahead semantics** to ensure crash consistency while minimizing contention between cluster nodes.

---

## 1. Design Principles

| Principle | Rationale |
|-----------|-----------|
| Metadata-only journaling | Data journaling is too expensive for VM workloads |
| Per-node journal areas | Eliminates cross-node contention on journal writes |
| Intent/logical logging | More compact than physical logging; idempotent replay |
| Write-ahead protocol | Journal commit before metadata write ensures recoverability |
| Integrated failure detection | Leverages existing heartbeat mechanism |

---

## 2. On-Disk Layout

### Journal Region Location

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Block Device Layout                                 │
├─────────┬───────────┬───────────┬───────────────────────────────────────────────┤
│ Super   │ Node      │ JOURNAL   │  Lock Region, Inodes, Bitmap, Data...        │
│ Block   │ Registry  │ REGION    │                                               │
│ 4KB     │ 1MB       │ 256MB     │                                               │
└─────────┴───────────┴───────────┴───────────────────────────────────────────────┘

Journal Region = MaxNodes × JournalSlotSize
Default: 64 nodes × 4MB = 256MB
```

### Superblock Extensions

```go
type Superblock struct {
    // ... existing fields ...

    // Journal Region
    JournalRegionStart  uint64     // Byte offset to journal region
    JournalRegionSize   uint64     // Total size in bytes
    JournalSlotSize     uint64     // Size per node (default: 4MB)
    JournalVersion      uint32     // Journal format version
}
```

---

## 3. Journal Data Structures

### Per-Node Journal Superblock

Each node has a dedicated journal area starting with a superblock:

```go
// JournalSuperblock: 512 bytes (sector-aligned for O_DIRECT)
type JournalSuperblock struct {
    // Identity
    Magic               [12]byte   // "GCLSJOURNAL\0"
    Version             uint32     // Journal format version (1)

    // Ownership
    NodeUUID            [16]byte   // UUID of owning node
    NodeGeneration      uint64     // Generation when journal was initialized

    // Journal State
    State               uint32     // JournalState enum
    Flags               uint32     // JournalFlags

    // Sequence Tracking
    FirstSeq            uint64     // Sequence of first valid transaction
    LastSeq             uint64     // Sequence of last committed transaction
    NextSeq             uint64     // Next sequence to assign

    // Ring Buffer Pointers
    HeadOffset          uint32     // Offset of first valid record
    TailOffset          uint32     // Offset after last valid record
    WrapCount           uint64     // Number of times journal has wrapped

    // Geometry
    DataAreaOffset      uint32     // Where transaction data starts
    DataAreaSize        uint32     // Size of transaction data area

    // Recovery Info
    LastCheckpointSeq   uint64     // Last fully checkpointed sequence
    LastCheckpointTime  int64      // Timestamp of last checkpoint
    RecoveryNodeUUID    [16]byte   // If being recovered, by which node

    // Checksum
    Checksum            uint32     // CRC32C of superblock

    // Padding to 512 bytes
    Reserved            [348]byte
}

type JournalState uint32
const (
    JournalStateClean      JournalState = 0  // No uncommitted transactions
    JournalStateDirty      JournalState = 1  // Has uncommitted transactions
    JournalStateRecovering JournalState = 2  // Being recovered by another node
    JournalStateCorrupt    JournalState = 3  // Needs fsck
)
```

### Transaction Record Header

```go
// TxnHeader: 64 bytes (aligned)
type TxnHeader struct {
    Magic           uint32     // TXN_MAGIC (0x54584E52 = "TXNR")
    Type            uint32     // TxnType enum
    Seq             uint64     // Transaction sequence number

    TotalLength     uint32     // Total bytes including header and entries
    EntryCount      uint32     // Number of TxnEntry records following

    Timestamp       int64      // When transaction started

    OwnerNodeUUID   [16]byte   // Which node created this transaction
    OwnerGeneration uint64     // Node generation at creation time

    Checksum        uint32     // CRC32C of entire transaction
    Reserved        [4]byte
}

type TxnType uint32
const (
    TxnTypeBegin      TxnType = 1  // Start of transaction
    TxnTypeCommit     TxnType = 2  // Transaction committed
    TxnTypeAbort      TxnType = 3  // Transaction aborted
    TxnTypeCheckpoint TxnType = 4  // Checkpoint marker
)
```

### Transaction Entry Types

```go
type TxnEntry struct {
    OpType      uint32     // TxnOpType enum
    Length      uint32     // Length of this entry including payload
    // Followed by operation-specific payload
}

type TxnOpType uint32
const (
    // Inode Operations
    TxnOpInodeCreate    TxnOpType = 0x0100
    TxnOpInodeUpdate    TxnOpType = 0x0101
    TxnOpInodeDelete    TxnOpType = 0x0102

    // Directory Operations
    TxnOpDirAddEntry    TxnOpType = 0x0200
    TxnOpDirRemoveEntry TxnOpType = 0x0201
    TxnOpDirRename      TxnOpType = 0x0202

    // Block Allocation
    TxnOpBlockAlloc     TxnOpType = 0x0300
    TxnOpBlockFree      TxnOpType = 0x0301

    // Sub-Block Allocation
    TxnOpSFBAlloc       TxnOpType = 0x0400
    TxnOpSFBFree        TxnOpType = 0x0401
    TxnOpSFBMigrate     TxnOpType = 0x0402

    // Extent Operations
    TxnOpExtentAdd      TxnOpType = 0x0500
    TxnOpExtentRemove   TxnOpType = 0x0501
    TxnOpExtentUpdate   TxnOpType = 0x0502

    // Snapshot Operations
    TxnOpSnapshotCreate TxnOpType = 0x0600
    TxnOpSnapshotDelete TxnOpType = 0x0601
    TxnOpRefCountUpdate TxnOpType = 0x0602
)
```

### Operation Payloads

```go
// TxnInodeCreate payload
type TxnInodeCreate struct {
    InodeNum        uint64
    Mode            uint32
    UID             uint32
    GID             uint32
    ParentInodeNum  uint64
    NameHash        uint64     // Hash of filename (for verification)
}

// TxnInodeUpdate payload
type TxnInodeUpdate struct {
    InodeNum        uint64
    UpdateMask      uint64     // Which fields are being updated
    // Followed by: old values (for undo), then new values
}

const (
    InodeUpdateSize    uint64 = 1 << 0
    InodeUpdateMode    uint64 = 1 << 1
    InodeUpdateUID     uint64 = 1 << 2
    InodeUpdateGID     uint64 = 1 << 3
    InodeUpdateAtime   uint64 = 1 << 4
    InodeUpdateMtime   uint64 = 1 << 5
    InodeUpdateCtime   uint64 = 1 << 6
    InodeUpdateExtents uint64 = 1 << 7
    InodeUpdateSFB     uint64 = 1 << 8
)

// TxnDirAddEntry payload
type TxnDirAddEntry struct {
    ParentInodeNum  uint64
    ChildInodeNum   uint64
    EntryType       uint8
    NameLen         uint8
    Reserved        [2]byte
    // Followed by: Name (NameLen bytes, null-padded to 4-byte boundary)
}

// TxnBlockAlloc payload
type TxnBlockAlloc struct {
    InodeNum        uint64
    StartBlock      uint64
    Count           uint32
    Purpose         uint32
}

// TxnSFBAlloc payload
type TxnSFBAlloc struct {
    InodeNum        uint64
    ContainerBlock  uint64
    SubBlockOffset  uint32
    SubBlockCount   uint32
    SubBlockSize    uint32
    Reserved        uint32
}

// TxnExtentAdd payload
type TxnExtentAdd struct {
    InodeNum        uint64
    FileBlock       uint64
    DiskBlock       uint64
    Length          uint32
    Flags           uint32
}
```

---

## 4. Transaction Protocol

### Write-Ahead Logging Sequence

```
┌─────────────────────────────────────────────────────────────────┐
│                    Metadata Write Protocol                       │
└─────────────────────────────────────────────────────────────────┘

  Application Request (e.g., create file)
            │
            ▼
  ┌───────────────────┐
  │ 1. ACQUIRE LOCKS  │  Following lock hierarchy
  └───────────────────┘
            │
            ▼
  ┌───────────────────┐
  │ 2. BEGIN TXN      │  Allocate sequence number
  └───────────────────┘
            │
            ▼
  ┌───────────────────┐
  │ 3. LOG INTENTS    │  Add TxnEntry for each change
  └───────────────────┘
            │
            ▼
  ┌───────────────────┐
  │ 4. WRITE JOURNAL  │  O_DIRECT write
  └───────────────────┘
            │
            ▼
  ┌───────────────────┐
  │ 5. FSYNC JOURNAL  │  ← COMMIT POINT
  └───────────────────┘
            │
            ▼
  ┌───────────────────┐
  │ 6. EXECUTE OPS    │  Write actual metadata
  └───────────────────┘
            │
            ▼
  ┌───────────────────┐
  │ 7. FSYNC METADATA │
  └───────────────────┘
            │
            ▼
  ┌───────────────────┐
  │ 8. ADVANCE HEAD   │  Checkpoint transaction
  └───────────────────┘
            │
            ▼
  ┌───────────────────┐
  │ 9. RELEASE LOCKS  │
  └───────────────────┘
```

### Transaction State Machine

```
    ┌──────────┐
    │  UNUSED  │
    └──────────┘
          │
          │ begin_transaction()
          ▼
    ┌──────────┐
    │ BUILDING │◄────────┐
    └──────────┘         │
          │              │ add_entry()
          │              │
          │ write_journal()
          ▼
    ┌──────────┐
    │ WRITTEN  │
    └──────────┘
          │
          │ fsync_journal()
          ▼
    ┌──────────┐
    │ COMMITTED│─────────┐ (if crash, replay needed)
    └──────────┘         │
          │              │
          │ execute_ops() + advance_head()
          ▼              ▼
    ┌────────────┐
    │CHECKPOINTED│
    └────────────┘
          │
          │ (reclaim journal space)
          ▼
    ┌──────────┐
    │  UNUSED  │
    └──────────┘
```

### Example: File Create Transaction

```go
func (fs *FS) CreateFile(parent uint64, name string, mode uint32) (*Inode, error) {
    // 1. Acquire locks (in order per hierarchy)
    parentLock := fs.lockMgr.AcquireInode(parent, LockModeExclusive)
    defer parentLock.Release()

    bitmapLock := fs.lockMgr.AcquireBitmap(LockModeExclusive)
    defer bitmapLock.Release()

    // 2. Begin transaction
    txn := fs.journal.Begin()

    // 3. Allocate resources and log intents
    inodeNum := fs.inodeTable.AllocateInodeNum()
    txn.LogInodeCreate(TxnInodeCreate{
        InodeNum:       inodeNum,
        Mode:           mode,
        ParentInodeNum: parent,
        NameHash:       hashName(name),
    })

    block := fs.bitmap.AllocateBlock()
    txn.LogBlockAlloc(TxnBlockAlloc{
        InodeNum:   inodeNum,
        StartBlock: block,
        Count:      1,
    })

    txn.LogDirAddEntry(TxnDirAddEntry{
        ParentInodeNum: parent,
        ChildInodeNum:  inodeNum,
        EntryType:      DT_REG,
        Name:           name,
    })

    // 4-5. Write and sync journal (COMMIT POINT)
    if err := txn.Commit(); err != nil {
        return nil, err
    }

    // 6. Execute actual metadata writes
    inode := &Inode{InodeNum: inodeNum, Mode: mode}
    fs.inodeTable.Write(inode)
    fs.bitmap.MarkAllocated(block)
    fs.directory.AddEntry(parent, name, inodeNum, DT_REG)

    // 7. Sync metadata
    fs.device.Sync()

    // 8. Checkpoint
    fs.journal.Checkpoint(txn.Seq)

    return inode, nil
}
```

---

## 5. Recovery Protocol

### Recovery Triggers

1. **Mount-time**: Node mounts and finds its own journal dirty
2. **Failure detection**: Heartbeat timeout detects failed node (10s)

### Journal Replay Algorithm

```go
func (j *Journal) Replay() error {
    sb, err := j.readSuperblock()
    if err != nil {
        return err
    }

    if sb.State == JournalStateClean {
        return nil
    }

    // Mark as recovering
    sb.State = JournalStateRecovering
    copy(sb.RecoveryNodeUUID[:], j.nodeUUID[:])
    j.writeSuperblock(sb)

    // Scan from head to tail
    offset := sb.HeadOffset
    for offset != sb.TailOffset {
        txn, err := j.readTransaction(offset)
        if err != nil {
            offset = j.nextRecordOffset(offset)
            continue
        }

        // Only replay committed transactions
        if txn.Type == TxnTypeCommit || j.isCommitted(txn.Seq) {
            j.replayTransaction(txn)
        }

        offset = j.nextRecordOffset(offset)
    }

    // Mark recovery complete
    sb.State = JournalStateClean
    sb.HeadOffset = sb.TailOffset
    j.writeSuperblock(sb)

    return nil
}
```

### Idempotent Replay Operations

Each replay operation checks if already applied:

```go
func (j *Journal) replayInodeCreate(entry *TxnEntry) error {
    payload := entry.Payload.(*TxnInodeCreate)

    // Check if inode already exists
    existing, err := j.fs.inodeTable.Read(payload.InodeNum)
    if err == nil && existing != nil {
        return nil  // Already applied
    }

    // Create the inode
    inode := &Inode{
        InodeNum: payload.InodeNum,
        Mode:     payload.Mode,
    }
    return j.fs.inodeTable.Write(inode)
}
```

### Cross-Node Recovery Protocol

When Node B detects Node A has failed:

```
1. Mark Node A as STALE
2. Wait 2x timeout (20s) - ensures consensus
3. Acquire Node A's journal lock (CAS on superblock)
4. Replay Node A's journal
5. Reclaim Node A's resource locks
6. Clear Node A's slot
7. Release journal lock
```

---

## 6. Checkpoint Protocol

### Checkpoint Types

| Type | Trigger | Purpose |
|------|---------|---------|
| Transaction checkpoint | After each txn completes | Advance head pointer |
| Periodic checkpoint | Every 5 seconds | Batch head advancement |
| Forced checkpoint | Journal 75% full | Prevent journal full |
| Unmount checkpoint | Clean shutdown | Ensure clean state |

### Journal Space Management

```go
func (j *Journal) AvailableSpace() uint32 {
    sb := j.cachedSuperblock

    if sb.TailOffset >= sb.HeadOffset {
        return sb.DataAreaSize - (sb.TailOffset - sb.HeadOffset)
    }
    return sb.HeadOffset - sb.TailOffset
}

func (j *Journal) EnsureSpace(needed uint32) error {
    if j.AvailableSpace() >= needed {
        return nil
    }

    // Force checkpoint to free space
    j.tracker.WaitOldest(5 * time.Second)
    j.Checkpoint(j.tracker.OldestCompletedSeq())

    if j.AvailableSpace() < needed {
        return ErrJournalFull
    }
    return nil
}
```

---

## 7. Crash Point Analysis

| Crash Point | Journal State | Metadata State | Recovery Action |
|-------------|---------------|----------------|-----------------|
| Before journal write | No record | Unchanged | Nothing to do |
| During journal write | Corrupt | Unchanged | Skip corrupt record |
| After journal write | Complete (cache) | Unchanged | May lose if not on disk |
| Before journal sync | Complete (cache) | Unchanged | May lose transaction |
| **After journal sync** | **Committed** | **Unchanged** | **Replay transaction** |
| During metadata write | Committed | Partial | Replay completes it |
| After metadata write | Committed | Complete | Replay is no-op |
| Before checkpoint | Committed | Complete | Checkpoint on recovery |

---

## 8. Configuration

```go
type JournalConfig struct {
    SlotSize            uint64        // Per-node size (default: 4MB)
    CommitMode          CommitMode    // Sync/Async (default: Sync)
    CheckpointInterval  time.Duration // Default: 5s
    BatchSize           int           // Max txns per batch (default: 32)
    BatchDelay          time.Duration // Max batch wait (default: 10ms)
    MaxIOErrors         int           // Before self-fence (default: 10)
}

type CommitMode int
const (
    CommitSync      CommitMode = iota  // fsync after every txn (safest)
    CommitAsync                        // fsync periodically (faster)
)
```

---

## 9. Integration with Lock Hierarchy

```
Level 0: Volume Lock
    │
    └─▶ Level 0.5: Journal Lock (per-node)
            │
            └─▶ Level 1: Lock Region Lock
                    │
                    └─▶ ... remaining hierarchy
```

The Journal Lock is per-node and only contends during recovery operations.
