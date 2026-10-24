// Package inode implements inode table management for GoClusterFS.
//
// Each inode is 256 bytes, stored in 4096-byte sectors (16 inodes per sector).
// Inodes use CRC32C checksums for integrity verification.
//
// Reserved inodes:
//   - Inode 0: Invalid/unused marker
//   - Inode 1: Bad block tracking
//   - Inode 2: Root directory
package inode

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// InodeSize is the size of each inode in bytes.
	InodeSize = 256

	// SectorSize is the disk sector size (matches storage.SectorSize).
	SectorSize = 4096

	// InodesPerSector is the number of inodes per disk sector.
	InodesPerSector = SectorSize / InodeSize // 16

	// InodeMagic identifies a valid inode.
	InodeMagic uint32 = 0x494E4F44 // "INOD"

	// Reserved inodes
	InodeInvalid     uint32 = 0 // Invalid/marker
	InodeBadBlocks   uint32 = 1 // Bad block list
	InodeRoot        uint32 = 2 // Root directory
	InodeFirstUsable uint32 = 3 // First allocatable inode

	// MaxInlineExtents is the number of extents stored directly in the inode.
	MaxInlineExtents = 2
)

// File types (match Linux S_IF* values)
const (
	S_IFREG  uint16 = 0o100000 // Regular file
	S_IFDIR  uint16 = 0o040000 // Directory
	S_IFLNK  uint16 = 0o120000 // Symbolic link
	S_IFBLK  uint16 = 0o060000 // Block device
	S_IFCHR  uint16 = 0o020000 // Character device
	S_IFIFO  uint16 = 0o010000 // FIFO
	S_IFSOCK uint16 = 0o140000 // Socket
	S_IFMT   uint16 = 0o170000 // Mask for file type
)

// Inode flags
const (
	FlagHasExtentTree uint16 = 1 << 0 // Uses overflow extent tree
	FlagHasSnapshot   uint16 = 1 << 1 // Part of a snapshot
	FlagDeleted       uint16 = 1 << 2 // Marked for deletion
	FlagSync          uint16 = 1 << 3 // Synchronous updates
	FlagImmutable     uint16 = 1 << 4 // Cannot be modified
	FlagAppend        uint16 = 1 << 5 // Append-only
	FlagSnapshotData  uint16 = 1 << 6 // Holds a snapshot's frozen extents; in no directory
)

// Errors
var (
	ErrInvalidInode     = errors.New("invalid inode")
	ErrInodeOutOfRange  = errors.New("inode number out of range")
	ErrInodeFull        = errors.New("no free inodes available")
	ErrInodeInUse       = errors.New("inode already in use")
	ErrInodeNotInUse    = errors.New("inode not in use")
	ErrChecksumMismatch = errors.New("inode checksum mismatch")
	ErrNoTreeFlusher    = errors.New("inode has pending extent tree changes but no tree flusher is set")
	ErrTreeConflict     = errors.New("extent tree changed concurrently")
)

// TreeFlusher writes an inode's pending extent-tree changes before the inode
// itself is written. It runs under the inode lock; onDisk is the inode as it is
// currently stored. The returned func, if any, runs after the inode write
// succeeds; Write runs it after releasing the inode lock.
type TreeFlusher interface {
	FlushTree(ino, onDisk *Inode) (committed func(), err error)
}

// InlineExtent represents an extent stored directly in the inode.
// Size: 32 bytes.
type InlineExtent struct {
	LogicalBlock  uint64 // File offset in blocks (1MB each)
	PhysicalBlock uint64 // Disk block number
	Length        uint64 // Number of blocks
	Flags         uint32 // Extent flags
	Reserved      uint32
}

// Inode represents an on-disk inode (256 bytes).
type Inode struct {
	Magic            uint32          // 0x494E4F44 "INOD"
	InodeNum         uint32          // Inode number
	Mode             uint16          // File type + permissions
	Flags            uint16          // Inode flags
	UID              uint32          // Owner user ID
	GID              uint32          // Owner group ID
	LinkCount        uint32          // Hard link count
	Size             uint64          // File size in bytes
	AllocatedBlocks  uint64          // Physical blocks allocated
	WrittenBlocks    uint64          // Actually written blocks (excluding unwritten)
	Atime            int64           // Access time (Unix timestamp)
	Mtime            int64           // Modification time
	Ctime            int64           // Change time (metadata)
	DataGeneration   uint64          // Data modification counter
	MetaGeneration   uint64          // Metadata modification counter
	LastModifierUUID [16]byte        // UUID of last modifying node
	InlineExtents    [2]InlineExtent // Inline extent storage (64 bytes)
	ExtentTreeRoot   uint64          // Block number for overflow tree
	ExtentCount      uint64          // Total extent count
	ExtentDepth      uint32          // Extent tree depth (0 = inline only)
	ExtentTreeGen    uint64          // Commit generation of the tree root; its low bit picks the root's active half
	Reserved         [56]byte        // Padding to 252 bytes
	Checksum         uint32          // CRC32C checksum

	// PendingTree holds extent-tree changes owned by package extent that are
	// not on disk yet. It is never serialized; Table writes it via its TreeFlusher.
	PendingTree any

	committedInline [MaxInlineExtents]InlineExtent // inline extents as last read or written
}

// CommittedInlineExtents returns the inline extents as this copy last read or
// wrote them, before any uncommitted edits.
func (inode *Inode) CommittedInlineExtents() [MaxInlineExtents]InlineExtent {
	return inode.committedInline
}

// Table manages the inode table on disk.
type Table struct {
	mu          sync.RWMutex
	dev         *storage.Device
	offset      uint64        // Inode table start offset on device
	size        uint64        // Inode table size in bytes
	totalInodes uint64        // Total inode count
	lockMgr     *lock.Manager // For distributed locking (optional)
	nodeUUID    [16]byte      // This node's UUID
	treeFlusher TreeFlusher
	syncDevice  func() error
}

// SetTreeFlusher sets the writer for pending extent-tree changes.
// It must be set before any inode with an extent tree is modified.
func (t *Table) SetTreeFlusher(f TreeFlusher) {
	t.treeFlusher = f
}

// NewTable creates a new inode table manager.
//
// Parameters:
//   - dev: The storage device
//   - offset: Byte offset where inode table starts on device
//   - size: Total size of inode table region
//   - lockMgr: Lock manager for distributed locking (can be nil for single-node)
//   - nodeUUID: This node's UUID for modification tracking
func NewTable(dev *storage.Device, offset, size uint64, lockMgr *lock.Manager, nodeUUID [16]byte) *Table {
	totalInodes := size / InodeSize
	return &Table{
		dev:         dev,
		offset:      offset,
		size:        size,
		totalInodes: totalInodes,
		lockMgr:     lockMgr,
		nodeUUID:    nodeUUID,
		syncDevice:  dev.Sync,
	}
}

// Read reads an inode from disk.
func (t *Table) Read(inodeNum uint32) (*Inode, error) {
	if uint64(inodeNum) >= t.totalInodes {
		return nil, ErrInodeOutOfRange
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.readInodeUnlocked(inodeNum)
}

// Write writes an inode to disk.
// The inode must have a valid InodeNum set.
func (t *Table) Write(ctx context.Context, inode *Inode) error {
	committed, err := t.lockedWrite(ctx, inode)
	if committed != nil {
		committed()
	}
	return err
}

func (t *Table) lockedWrite(ctx context.Context, inode *Inode) (func(), error) {
	if uint64(inode.InodeNum) >= t.totalInodes {
		return nil, ErrInodeOutOfRange
	}

	// Acquire distributed lock if available
	if t.lockMgr != nil {
		if err := t.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(inode.InodeNum), lock.LockModeExclusive); err != nil {
			return nil, fmt.Errorf("failed to acquire inode lock: %w", err)
		}
		defer t.lockMgr.Release(lock.LockTypeInode, uint64(inode.InodeNum))
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return t.writeWithTree(inode)
}

// WriteWithLockHeld writes an inode to disk without acquiring the distributed lock.
// Use this when the caller already holds the inode lock (e.g., in directory operations).
// This prevents deadlock from trying to acquire the same lock twice.
func (t *Table) WriteWithLockHeld(inode *Inode) error {
	if uint64(inode.InodeNum) >= t.totalInodes {
		return ErrInodeOutOfRange
	}

	t.mu.Lock()
	committed, err := t.writeWithTree(inode)
	t.mu.Unlock()
	if committed != nil {
		committed()
	}
	return err
}

// writeWithTree flushes pending extent-tree changes, then writes the inode.
// The inode sector write is the commit point for the tree. The returned func
// finishes the commit by freeing released blocks; Write runs it after dropping
// the inode lock. Under WriteWithLockHeld the caller still holds that lock, so
// frees that would break the lock order are deferred by the flusher.
// Caller must hold t.mu and the inode lock.
func (t *Table) writeWithTree(inode *Inode) (func(), error) {
	var committed func()
	flushed := inode.PendingTree != nil
	if flushed {
		if t.treeFlusher == nil {
			return nil, ErrNoTreeFlusher
		}
		onDisk, err := t.readInodeUnlocked(inode.InodeNum)
		if err != nil {
			return nil, err
		}
		if committed, err = t.treeFlusher.FlushTree(inode, onDisk); err != nil {
			return nil, err
		}
	}

	inode.LastModifierUUID = t.nodeUUID
	inode.MetaGeneration++

	if err := t.writeInodeSector(inode, !flushed); err != nil {
		return nil, err
	}
	inode.PendingTree = nil
	inode.committedInline = inode.InlineExtents
	if flushed {
		// The next commit overwrites the half the previous inode used, and
		// released blocks may be reused, so this inode must be durable first.
		if err := t.syncDevice(); err != nil {
			return nil, err
		}
	}
	return committed, nil
}

// staleExtents reports whether writing inode would replace extents another
// write changed since inode was read, such as a copy-on-write whose old block
// was already released.
func staleExtents(inode, onDisk *Inode) bool {
	if inode.HasExtentTree() || onDisk.HasExtentTree() {
		return !sameExtentTree(inode, onDisk)
	}
	return onDisk.InlineExtents != inode.committedInline
}

func sameExtentTree(a, b *Inode) bool {
	return a.HasExtentTree() == b.HasExtentTree() && a.ExtentTreeRoot == b.ExtentTreeRoot &&
		a.ExtentTreeGen == b.ExtentTreeGen && a.ExtentDepth == b.ExtentDepth
}

// FindFreeSlot scans for a free inode slot without acquiring any distributed locks.
// This is the first phase of a two-phase allocation pattern used to respect lock ordering.
// Returns the inode number of a free slot, or ErrInodeFull if none available.
//
// Important: The slot may be claimed by another node between FindFreeSlot() and ClaimSlot().
// Callers should be prepared to retry with a different slot if ClaimSlot() returns ErrInodeInUse.
func (t *Table) FindFreeSlot() (uint32, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for num := InodeFirstUsable; uint64(num) < t.totalInodes; num++ {
		inode, err := t.readInodeUnlocked(num)
		if err != nil {
			continue // Skip unreadable inodes
		}
		if inode.Magic == 0 {
			return num, nil
		}
	}
	return 0, ErrInodeFull
}

// ClaimSlot attempts to claim a specific inode slot found by FindFreeSlot().
// This is the second phase of a two-phase allocation pattern.
// Returns the initialized inode, or ErrInodeInUse if the slot was claimed by another node.
//
// This function acquires the distributed inode lock (Level 8), so callers must ensure
// any BlockBitmap locks (Level 6) are acquired BEFORE calling this function.
func (t *Table) ClaimSlot(ctx context.Context, num uint32) (*Inode, error) {
	if uint64(num) >= t.totalInodes {
		return nil, ErrInodeOutOfRange
	}

	// Re-read to verify slot is still free (another node may have claimed it)
	t.mu.RLock()
	existing, err := t.readInodeUnlocked(num)
	t.mu.RUnlock()
	if err != nil {
		return nil, fmt.Errorf("failed to read inode slot: %w", err)
	}
	if existing.Magic != 0 {
		return nil, ErrInodeInUse
	}

	// Acquire distributed lock if available
	if t.lockMgr != nil {
		if err := t.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(num), lock.LockModeExclusive); err != nil {
			return nil, fmt.Errorf("failed to acquire inode lock: %w", err)
		}
		defer t.lockMgr.Release(lock.LockTypeInode, uint64(num))
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Double-check after acquiring lock (another node could have won the race)
	existing, err = t.readInodeUnlocked(num)
	if err != nil {
		return nil, fmt.Errorf("failed to re-read inode slot: %w", err)
	}
	if existing.Magic != 0 {
		return nil, ErrInodeInUse
	}

	newInode := t.newInodeInSlot(num, existing)
	if err := t.writeInodeUnlocked(newInode); err != nil {
		return nil, fmt.Errorf("failed to write new inode: %w", err)
	}

	return newInode, nil
}

// newInodeInSlot initializes an inode for a free slot. Its generations are higher
// than any previous occupant's, so caches on other nodes can detect the reuse.
func (t *Table) newInodeInSlot(num uint32, free *Inode) *Inode {
	return &Inode{
		Magic:            InodeMagic,
		InodeNum:         num,
		LinkCount:        1,
		DataGeneration:   free.DataGeneration + 1,
		MetaGeneration:   free.MetaGeneration + 1,
		LastModifierUUID: t.nodeUUID,
	}
}

// Alloc allocates a new inode.
// Returns the allocated inode with InodeNum set, ready for initialization.
//
// Deprecated: Use FindFreeSlot() + ClaimSlot() for operations that need to allocate
// blocks before claiming the inode (to respect lock ordering: BlockBitmap before Inode).
func (t *Table) Alloc(ctx context.Context) (*Inode, error) {
	for {
		num, err := t.FindFreeSlot()
		if err != nil {
			return nil, err
		}
		ino, err := t.ClaimSlot(ctx, num)
		if !errors.Is(err, ErrInodeInUse) {
			return ino, err
		}
	}
}

// Free marks an inode as free.
func (t *Table) Free(ctx context.Context, inodeNum uint32) error {
	if uint64(inodeNum) >= t.totalInodes {
		return ErrInodeOutOfRange
	}

	if inodeNum < InodeFirstUsable {
		return fmt.Errorf("cannot free reserved inode %d", inodeNum)
	}

	// Acquire distributed lock if available
	if t.lockMgr != nil {
		if err := t.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(inodeNum), lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire inode lock: %w", err)
		}
		defer t.lockMgr.Release(lock.LockTypeInode, uint64(inodeNum))
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Read current inode
	inode, err := t.readInodeUnlocked(inodeNum)
	if err != nil {
		return err
	}

	// Verify it's in use
	if inode.Magic == 0 {
		return ErrInodeNotInUse
	}

	// Keep the generations so the slot's next occupant gets higher ones.
	emptyInode := &Inode{
		InodeNum:       inodeNum,
		DataGeneration: inode.DataGeneration,
		MetaGeneration: inode.MetaGeneration,
	}

	return t.writeInodeUnlocked(emptyInode)
}

// TotalInodes returns the total number of inodes in the table.
func (t *Table) TotalInodes() uint64 {
	return t.totalInodes
}

// FreeCount returns an estimate of free inodes.
// For simplicity in Phase 2, this scans the table to count free slots.
func (t *Table) FreeCount() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	freeCount := uint64(0)
	for num := InodeFirstUsable; uint64(num) < t.totalInodes; num++ {
		inode, err := t.readInodeUnlocked(num)
		if err != nil {
			continue
		}
		if inode.Magic == 0 {
			freeCount++
		}
	}
	return freeCount
}

// readInodeUnlocked reads an inode without acquiring the mutex.
// Caller must hold at least t.mu.RLock().
func (t *Table) readInodeUnlocked(inodeNum uint32) (*Inode, error) {
	// Calculate sector containing this inode
	sectorIdx := uint64(inodeNum) / InodesPerSector
	inodeOffset := uint64(inodeNum) % InodesPerSector

	// Read the entire sector
	sectorOffset := int64(t.offset) + int64(sectorIdx)*SectorSize

	buf := storage.GetBuffer(SectorSize)
	defer storage.PutBuffer(buf)

	if _, err := t.dev.ReadAt(buf.Bytes(), sectorOffset); err != nil {
		return nil, err
	}

	// Extract this inode's data
	start := inodeOffset * InodeSize
	inodeData := buf.Bytes()[start : start+InodeSize]

	return deserializeInode(inodeData)
}

// writeInodeUnlocked writes an inode without acquiring the mutex.
// Caller must hold t.mu.Lock().
func (t *Table) writeInodeUnlocked(inode *Inode) error {
	return t.writeInodeSector(inode, false)
}

// writeInodeSector writes an inode into its sector. With checkTree, a copy
// whose extent tree differs from the stored one fails with ErrTreeConflict:
// it has no extent edits, so writing it would roll back or wipe a tree that
// another node committed, and the tree it read may already be freed.
func (t *Table) writeInodeSector(inode *Inode, checkTree bool) error {
	// Calculate sector containing this inode
	sectorIdx := uint64(inode.InodeNum) / InodesPerSector
	inodeOffset := uint64(inode.InodeNum) % InodesPerSector

	// Read the entire sector first (we need to preserve other inodes)
	sectorOffset := int64(t.offset) + int64(sectorIdx)*SectorSize

	buf := storage.GetBuffer(SectorSize)
	defer storage.PutBuffer(buf)

	if _, err := t.dev.ReadAt(buf.Bytes(), sectorOffset); err != nil {
		// For new sectors, initialize with zeros
		if err == syscall.EINVAL {
			for i := range buf.Bytes() {
				buf.Bytes()[i] = 0
			}
		} else {
			return err
		}
	}

	start := inodeOffset * InodeSize
	if checkTree {
		onDisk, err := deserializeInode(buf.Bytes()[start : start+InodeSize])
		if err == nil && staleExtents(inode, onDisk) {
			return fmt.Errorf("%w: inode %d", ErrTreeConflict, inode.InodeNum)
		}
	}

	// Serialize the inode
	inodeData := serializeInode(inode)

	// Copy into the sector at the correct offset
	copy(buf.Bytes()[start:start+InodeSize], inodeData)

	// Write the sector back
	_, err := t.dev.WriteAt(buf.Bytes(), sectorOffset)
	return err
}

// InitRootInode initializes the root directory inode if it doesn't exist.
func (t *Table) InitRootInode(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if root already exists
	root, err := t.readInodeUnlocked(InodeRoot)
	if err == nil && root.Magic == InodeMagic {
		return nil // Already initialized
	}

	// Create root directory inode
	rootInode := &Inode{
		Magic:            InodeMagic,
		InodeNum:         InodeRoot,
		Mode:             S_IFDIR | 0o755,
		UID:              0,
		GID:              0,
		LinkCount:        2, // . and parent (for root, parent is itself)
		Size:             0,
		LastModifierUUID: t.nodeUUID,
	}

	return t.writeInodeUnlocked(rootInode)
}

// IsDir returns true if the inode is a directory.
func (inode *Inode) IsDir() bool {
	return (inode.Mode & S_IFMT) == S_IFDIR
}

// IsFile returns true if the inode is a regular file.
func (inode *Inode) IsFile() bool {
	return (inode.Mode & S_IFMT) == S_IFREG
}

// IsSymlink returns true if the inode is a symbolic link.
func (inode *Inode) IsSymlink() bool {
	return (inode.Mode & S_IFMT) == S_IFLNK
}

// HasExtentTree returns true if the inode uses an overflow extent tree.
func (inode *Inode) HasExtentTree() bool {
	return (inode.Flags & FlagHasExtentTree) != 0
}

// Extent flag for SFB detection (must match extent.FlagSFB)
const extentFlagSFB = 1 << 3

// IsSFBBacked returns true if the inode's first extent is in an SFB container.
// SFB (Sub-File Block) containers store small files (<64KB) efficiently.
func (inode *Inode) IsSFBBacked() bool {
	if inode.ExtentCount == 0 {
		return false
	}
	return (inode.InlineExtents[0].Flags & extentFlagSFB) != 0
}

// Permissions returns the permission bits (lower 12 bits of Mode).
func (inode *Inode) Permissions() uint16 {
	return inode.Mode & 0o7777
}

// serializeInode converts an Inode to bytes.
func serializeInode(inode *Inode) []byte {
	buf := make([]byte, InodeSize)

	// Header fields
	binary.LittleEndian.PutUint32(buf[0:4], inode.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], inode.InodeNum)
	binary.LittleEndian.PutUint16(buf[8:10], inode.Mode)
	binary.LittleEndian.PutUint16(buf[10:12], inode.Flags)
	binary.LittleEndian.PutUint32(buf[12:16], inode.UID)
	binary.LittleEndian.PutUint32(buf[16:20], inode.GID)
	binary.LittleEndian.PutUint32(buf[20:24], inode.LinkCount)

	// Size and allocation
	binary.LittleEndian.PutUint64(buf[24:32], inode.Size)
	binary.LittleEndian.PutUint64(buf[32:40], inode.AllocatedBlocks)
	binary.LittleEndian.PutUint64(buf[40:48], inode.WrittenBlocks)

	// Timestamps
	binary.LittleEndian.PutUint64(buf[48:56], uint64(inode.Atime))
	binary.LittleEndian.PutUint64(buf[56:64], uint64(inode.Mtime))
	binary.LittleEndian.PutUint64(buf[64:72], uint64(inode.Ctime))

	// Generation counters
	binary.LittleEndian.PutUint64(buf[72:80], inode.DataGeneration)
	binary.LittleEndian.PutUint64(buf[80:88], inode.MetaGeneration)

	// Last modifier UUID
	copy(buf[88:104], inode.LastModifierUUID[:])

	// Inline extents (2 × 32 bytes = 64 bytes)
	for i, ext := range inode.InlineExtents {
		offset := 104 + i*32
		binary.LittleEndian.PutUint64(buf[offset:offset+8], ext.LogicalBlock)
		binary.LittleEndian.PutUint64(buf[offset+8:offset+16], ext.PhysicalBlock)
		binary.LittleEndian.PutUint64(buf[offset+16:offset+24], ext.Length)
		binary.LittleEndian.PutUint32(buf[offset+24:offset+28], ext.Flags)
		binary.LittleEndian.PutUint32(buf[offset+28:offset+32], ext.Reserved)
	}

	// Extent tree info
	binary.LittleEndian.PutUint64(buf[168:176], inode.ExtentTreeRoot)
	binary.LittleEndian.PutUint64(buf[176:184], inode.ExtentCount)
	binary.LittleEndian.PutUint32(buf[184:188], inode.ExtentDepth)
	binary.LittleEndian.PutUint64(buf[188:196], inode.ExtentTreeGen)

	// Reserved bytes 196-251 (56 bytes) - already zero

	// Compute checksum (bytes 0-251)
	checksum := storage.CRC32C(buf[:InodeSize-4])
	binary.LittleEndian.PutUint32(buf[InodeSize-4:], checksum)

	return buf
}

// deserializeInode parses an Inode from bytes.
func deserializeInode(buf []byte) (*Inode, error) {
	if len(buf) < InodeSize {
		return nil, ErrInvalidInode
	}

	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic == 0 {
		return &Inode{
			DataGeneration: binary.LittleEndian.Uint64(buf[72:80]),
			MetaGeneration: binary.LittleEndian.Uint64(buf[80:88]),
		}, nil
	}

	// Verify checksum
	stored := binary.LittleEndian.Uint32(buf[InodeSize-4:])
	computed := storage.CRC32C(buf[:InodeSize-4])
	if stored != computed {
		return nil, ErrChecksumMismatch
	}

	inode := &Inode{
		Magic:           magic,
		InodeNum:        binary.LittleEndian.Uint32(buf[4:8]),
		Mode:            binary.LittleEndian.Uint16(buf[8:10]),
		Flags:           binary.LittleEndian.Uint16(buf[10:12]),
		UID:             binary.LittleEndian.Uint32(buf[12:16]),
		GID:             binary.LittleEndian.Uint32(buf[16:20]),
		LinkCount:       binary.LittleEndian.Uint32(buf[20:24]),
		Size:            binary.LittleEndian.Uint64(buf[24:32]),
		AllocatedBlocks: binary.LittleEndian.Uint64(buf[32:40]),
		WrittenBlocks:   binary.LittleEndian.Uint64(buf[40:48]),
		Atime:           int64(binary.LittleEndian.Uint64(buf[48:56])),
		Mtime:           int64(binary.LittleEndian.Uint64(buf[56:64])),
		Ctime:           int64(binary.LittleEndian.Uint64(buf[64:72])),
		DataGeneration:  binary.LittleEndian.Uint64(buf[72:80]),
		MetaGeneration:  binary.LittleEndian.Uint64(buf[80:88]),
		ExtentTreeRoot:  binary.LittleEndian.Uint64(buf[168:176]),
		ExtentCount:     binary.LittleEndian.Uint64(buf[176:184]),
		ExtentDepth:     binary.LittleEndian.Uint32(buf[184:188]),
		ExtentTreeGen:   binary.LittleEndian.Uint64(buf[188:196]),
		Checksum:        stored,
	}

	// Copy UUID
	copy(inode.LastModifierUUID[:], buf[88:104])

	// Deserialize inline extents
	for i := range inode.InlineExtents {
		offset := 104 + i*32
		inode.InlineExtents[i] = InlineExtent{
			LogicalBlock:  binary.LittleEndian.Uint64(buf[offset : offset+8]),
			PhysicalBlock: binary.LittleEndian.Uint64(buf[offset+8 : offset+16]),
			Length:        binary.LittleEndian.Uint64(buf[offset+16 : offset+24]),
			Flags:         binary.LittleEndian.Uint32(buf[offset+24 : offset+28]),
			Reserved:      binary.LittleEndian.Uint32(buf[offset+28 : offset+32]),
		}
	}
	inode.committedInline = inode.InlineExtents

	return inode, nil
}

// SizeForInodes calculates the table size needed for a given number of inodes.
// Rounds up to sector boundary.
func SizeForInodes(count uint64) uint64 {
	numSectors := (count + InodesPerSector - 1) / InodesPerSector
	return numSectors * SectorSize
}
