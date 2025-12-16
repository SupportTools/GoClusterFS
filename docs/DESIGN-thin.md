# GoClusterFS Thin Provisioning Design

## Overview

This document specifies the thin provisioning system for GoClusterFS, including allocate-on-write, lazy zeroing, sparse files, and TRIM/UNMAP support.

---

## 1. Design Goals

| Goal | Description |
|------|-------------|
| Allocate-on-write | Physical blocks allocated only when data is written |
| Sparse files | Holes consume no physical storage |
| Lazy zeroing | Unwritten extents read as zeros without physical zeroing |
| TRIM/UNMAP | Reclaim deleted space, notify underlying storage |
| Accurate reporting | Distinguish logical size from physical allocation |

---

## 2. Extent Flags

```go
type ExtentFlags uint32

const (
    ExtentFlagAllocated  ExtentFlags = 1 << 0  // Physical block allocated
    ExtentFlagUnwritten  ExtentFlags = 1 << 1  // Allocated but not written (lazy zero)
    ExtentFlagShared     ExtentFlags = 1 << 2  // CoW shared with snapshot
    ExtentFlagCompressed ExtentFlags = 1 << 3  // Future: compressed
    ExtentFlagEncrypted  ExtentFlags = 1 << 4  // Future: encrypted
)

type Extent struct {
    FileBlock   uint64       // Logical block offset in file
    DiskBlock   uint64       // Physical block (0 if hole)
    Length      uint32       // Number of contiguous blocks
    Flags       ExtentFlags
}
```

### Extent States

| State | Extent Record | DiskBlock | Read Behavior | Write Behavior |
|-------|---------------|-----------|---------------|----------------|
| HOLE | None | N/A | Return zeros | Allocate block |
| UNWRITTEN | Exists + Unwritten flag | Valid | Return zeros | Clear flag, write |
| WRITTEN | Exists, no flag | Valid | Read from disk | Write to disk |

---

## 3. Inode Extensions

```go
type Inode struct {
    // ... existing fields ...

    // Size tracking
    Size              uint64  // Logical file size
    AllocatedBlocks   uint64  // Physical blocks (including unwritten)
    WrittenBlocks     uint64  // Actually written blocks

    // Provisioning mode
    ProvisioningMode  ProvisioningMode
}

type ProvisioningMode uint8
const (
    ProvisioningThin           ProvisioningMode = 0  // Allocate on demand
    ProvisioningThickLazy      ProvisioningMode = 1  // Pre-allocate, lazy zero
    ProvisioningThickEager     ProvisioningMode = 2  // Pre-allocate, immediate zero
)
```

---

## 4. Read Operation

```go
func (f *File) Read(ctx context.Context, offset, length uint64) ([]byte, error) {
    f.lockManager.AcquireShared(LockTypeInode, f.inodeNum)
    defer f.lockManager.Release(LockTypeInode, f.inodeNum)

    result := make([]byte, length)
    fileOffset := offset
    bufOffset := uint64(0)

    for bufOffset < length {
        extent := f.findExtent(fileOffset)

        if extent == nil {
            // HOLE: Return zeros (already zero-initialized)
            zeroLen := min(length-bufOffset, nextExtentStart-fileOffset)
            fileOffset += zeroLen
            bufOffset += zeroLen
            continue
        }

        if extent.Flags&ExtentFlagUnwritten != 0 {
            // UNWRITTEN: Return zeros without reading disk
            zeroLen := min(length-bufOffset, extentEnd-fileOffset)
            fileOffset += zeroLen
            bufOffset += zeroLen
            continue
        }

        // WRITTEN: Read from physical block
        physOffset := extent.DiskBlock*blockSize + (fileOffset - extent.FileBlock*blockSize)
        readLen := min(length-bufOffset, extentEnd-fileOffset)
        f.device.ReadAt(result[bufOffset:bufOffset+readLen], int64(physOffset))

        fileOffset += readLen
        bufOffset += readLen
    }

    return result, nil
}
```

---

## 5. Write Operation

```go
func (f *File) Write(ctx context.Context, offset uint64, data []byte) (int, error) {
    f.lockManager.AcquireExclusive(LockTypeInode, f.inodeNum)
    defer f.lockManager.Release(LockTypeInode, f.inodeNum)

    for logicalBlock := startBlock; logicalBlock <= endBlock; logicalBlock++ {
        extent := f.findExtent(logicalBlock)

        if extent == nil {
            // HOLE: Allocate new block
            extent = f.allocateBlock(logicalBlock)
        }

        if extent.Flags&ExtentFlagUnwritten != 0 {
            // UNWRITTEN: Convert to written
            f.convertUnwrittenExtent(extent, logicalBlock, data)
            continue
        }

        // WRITTEN: Direct write
        f.writeToBlock(extent.DiskBlock, blockOffset, data)
    }

    // Update size if extending
    if offset+uint64(len(data)) > f.inode.Size {
        f.inode.Size = offset + uint64(len(data))
    }

    return len(data), nil
}
```

---

## 6. Lazy Zeroing (fallocate)

```go
func (f *File) Fallocate(ctx context.Context, mode uint32, offset, length uint64) error {
    f.lockManager.AcquireExclusive(LockTypeInode, f.inodeNum)
    defer f.lockManager.Release(LockTypeInode, f.inodeNum)

    switch {
    case mode == 0 || mode&FALLOC_FL_KEEP_SIZE == 0:
        // Allocate unwritten extents
        return f.allocateUnwritten(offset, length, mode&FALLOC_FL_KEEP_SIZE != 0)

    case mode&FALLOC_FL_PUNCH_HOLE != 0:
        // Create hole (deallocate)
        return f.punchHole(offset, length)

    case mode&FALLOC_FL_ZERO_RANGE != 0:
        // Zero range (convert to unwritten)
        return f.zeroRange(offset, length)
    }

    return syscall.EOPNOTSUPP
}

func (f *File) allocateUnwritten(offset, length uint64, keepSize bool) error {
    startBlock := offset / blockSize
    endBlock := (offset + length - 1) / blockSize

    f.lockManager.AcquireExclusive(LockTypeBitmap, 0)
    defer f.lockManager.Release(LockTypeBitmap, 0)

    for block := startBlock; block <= endBlock; block++ {
        if f.hasExtent(block) {
            continue
        }

        diskBlock := f.bitmap.AllocateBlock()

        extent := &Extent{
            FileBlock: block,
            DiskBlock: diskBlock,
            Length:    1,
            Flags:     ExtentFlagAllocated | ExtentFlagUnwritten,
        }

        f.addExtent(extent)
        f.inode.AllocatedBlocks++
        // WrittenBlocks NOT incremented (unwritten)
    }

    if !keepSize && offset+length > f.inode.Size {
        f.inode.Size = offset + length
    }

    return nil
}
```

---

## 7. Unwritten to Written Conversion

```go
func (f *File) convertUnwrittenExtent(extent *Extent, writeOffset uint64, data []byte) error {
    extentStart := extent.FileBlock * blockSize
    extentEnd := extentStart + uint64(extent.Length)*blockSize
    writeEnd := writeOffset + uint64(len(data))

    // Case 1: Write covers entire extent
    if writeOffset <= extentStart && writeEnd >= extentEnd {
        extent.Flags &^= ExtentFlagUnwritten
        f.inode.WrittenBlocks += uint64(extent.Length)
        return nil
    }

    // Case 2: Partial write - split extent
    // [UNWRITTEN] -> [UNWRITTEN | WRITTEN | UNWRITTEN]
    return f.splitExtent(extent, writeOffset, writeEnd)
}
```

---

## 8. TRIM Manager

```go
type TRIMManager struct {
    mu           sync.Mutex
    device       *os.File

    pending      []TRIMRequest
    maxBatchSize int           // Default: 256
    maxBatchAge  time.Duration // Default: 5s
    lastFlush    time.Time

    totalTrimmed uint64
    trimCount    uint64
}

type TRIMRequest struct {
    DiskBlock   uint64
    Length      uint32
    RequestTime time.Time
}

func (tm *TRIMManager) QueueTRIM(diskBlock uint64, length uint32) {
    tm.mu.Lock()
    defer tm.mu.Unlock()

    tm.pending = append(tm.pending, TRIMRequest{
        DiskBlock:   diskBlock,
        Length:      length,
        RequestTime: time.Now(),
    })

    if len(tm.pending) >= tm.maxBatchSize {
        tm.flushLocked()
    }
}

func (tm *TRIMManager) flushLocked() error {
    if len(tm.pending) == 0 {
        return nil
    }

    // Sort by disk block
    sort.Slice(tm.pending, func(i, j int) bool {
        return tm.pending[i].DiskBlock < tm.pending[j].DiskBlock
    })

    // Coalesce adjacent blocks
    coalesced := tm.coalesceRequests(tm.pending)

    // Issue TRIM commands
    for _, req := range coalesced {
        tm.issueTRIM(req.DiskBlock, req.Length)
    }

    tm.pending = tm.pending[:0]
    tm.lastFlush = time.Now()
    return nil
}

func (tm *TRIMManager) issueTRIM(diskBlock uint64, length uint32) error {
    // Linux: ioctl(fd, BLKDISCARD, [start, len])
    range_ := [2]uint64{
        diskBlock * blockSize,
        uint64(length) * blockSize,
    }

    _, _, errno := syscall.Syscall(
        syscall.SYS_IOCTL,
        tm.device.Fd(),
        BLKDISCARD,  // 0x1277
        uintptr(unsafe.Pointer(&range_)),
    )

    if errno != 0 {
        return errno
    }
    return nil
}
```

---

## 9. TRIM Integration Points

```go
// On file deletion
func (f *File) Delete() error {
    for _, extent := range f.inode.getAllExtents() {
        f.bitmap.FreeBlocks(extent.DiskBlock, extent.Length)
        f.trimManager.QueueTRIM(extent.DiskBlock, extent.Length)
    }
    f.inodeTable.FreeInode(f.inodeNum)
    return nil
}

// On truncation
func (f *File) Truncate(newSize uint64) error {
    if newSize < f.inode.Size {
        // Free extents beyond new size
        truncBlock := (newSize + blockSize - 1) / blockSize
        for _, extent := range f.inode.getExtentsBeyond(truncBlock) {
            f.bitmap.FreeBlocks(extent.DiskBlock, extent.Length)
            f.trimManager.QueueTRIM(extent.DiskBlock, extent.Length)
        }
    }
    f.inode.Size = newSize
    return nil
}

// On hole punch
func (f *File) punchHole(offset, length uint64) error {
    startBlock := offset / blockSize
    endBlock := (offset + length - 1) / blockSize

    for block := startBlock; block <= endBlock; block++ {
        extent := f.findExtent(block)
        if extent == nil {
            continue
        }

        f.bitmap.FreeBlock(extent.DiskBlock)
        f.trimManager.QueueTRIM(extent.DiskBlock, 1)
        f.removeExtent(extent)
        f.inode.AllocatedBlocks--
    }

    return nil
}
```

---

## 10. Space Reporting

```go
func (f *File) Attr(ctx context.Context, attr *fuse.Attr) error {
    attr.Size = f.inode.Size  // Logical size

    // Physical blocks in 512-byte units
    attr.Blocks = f.inode.AllocatedBlocks * (blockSize / 512)

    // This allows:
    // - ls -l shows logical size
    // - du -h shows physical usage
    // - Sparse files: Size >> Blocks * 512

    return nil
}

func (fs *FS) Statfs(resp *fuse.StatfsResponse) error {
    stats := fs.getSpaceStats()

    resp.Blocks = stats.TotalBlocks
    resp.Bfree = stats.FreeBlocks
    resp.Bavail = stats.FreeBlocks
    resp.Bsize = blockSize

    return nil
}

type SpaceStats struct {
    TotalBlocks     uint64
    FreeBlocks      uint64
    AllocatedBlocks uint64
    WrittenBlocks   uint64

    LogicalUsed     uint64  // Sum of file sizes
    PhysicalUsed    uint64  // Allocated * blockSize
    OvercommitRatio float64
}
```

---

## 11. Configuration

```go
type ThinProvisioningConfig struct {
    DefaultMode       ProvisioningMode // Default: Thin

    TRIMEnabled       bool             // Default: true
    TRIMBatchSize     int              // Default: 256
    TRIMBatchAge      time.Duration    // Default: 5s

    OvercommitWarn    float64          // Default: 0.8
    OvercommitCrit    float64          // Default: 0.95
}
```
