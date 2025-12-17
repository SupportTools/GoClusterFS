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
)

// Errors
var (
	ErrInvalidInode     = errors.New("invalid inode")
	ErrInodeOutOfRange  = errors.New("inode number out of range")
	ErrInodeFull        = errors.New("no free inodes available")
	ErrInodeInUse       = errors.New("inode already in use")
	ErrInodeNotInUse    = errors.New("inode not in use")
	ErrChecksumMismatch = errors.New("inode checksum mismatch")
)

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
	Reserved         [60]byte        // Padding to 252 bytes
	Checksum         uint32          // CRC32C checksum
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
	if uint64(inode.InodeNum) >= t.totalInodes {
		return ErrInodeOutOfRange
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Acquire distributed lock if available
	if t.lockMgr != nil {
		if err := t.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(inode.InodeNum), lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire inode lock: %w", err)
		}
		defer t.lockMgr.Release(lock.LockTypeInode, uint64(inode.InodeNum))
	}

	// Update modification tracking
	inode.LastModifierUUID = t.nodeUUID
	inode.MetaGeneration++

	return t.writeInodeUnlocked(inode)
}

// WriteWithLockHeld writes an inode to disk without acquiring the distributed lock.
// Use this when the caller already holds the inode lock (e.g., in directory operations).
// This prevents deadlock from trying to acquire the same lock twice.
func (t *Table) WriteWithLockHeld(inode *Inode) error {
	if uint64(inode.InodeNum) >= t.totalInodes {
		return ErrInodeOutOfRange
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Update modification tracking
	inode.LastModifierUUID = t.nodeUUID
	inode.MetaGeneration++

	return t.writeInodeUnlocked(inode)
}

// Alloc allocates a new inode.
// Returns the allocated inode with InodeNum set, ready for initialization.
func (t *Table) Alloc(ctx context.Context) (*Inode, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Scan for a free inode starting from InodeFirstUsable
	for num := InodeFirstUsable; uint64(num) < t.totalInodes; num++ {
		inode, err := t.readInodeUnlocked(num)
		if err != nil {
			continue // Skip unreadable inodes
		}

		// Check if inode is free (Magic == 0)
		if inode.Magic == 0 {
			// Found a free slot - initialize it
			newInode := &Inode{
				Magic:            InodeMagic,
				InodeNum:         num,
				LinkCount:        1,
				LastModifierUUID: t.nodeUUID,
			}

			// Acquire distributed lock if available
			if t.lockMgr != nil {
				if err := t.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(num), lock.LockModeExclusive); err != nil {
					return nil, fmt.Errorf("failed to acquire inode lock: %w", err)
				}
				defer t.lockMgr.Release(lock.LockTypeInode, uint64(num))
			}

			if err := t.writeInodeUnlocked(newInode); err != nil {
				return nil, fmt.Errorf("failed to write new inode: %w", err)
			}

			return newInode, nil
		}
	}

	return nil, ErrInodeFull
}

// Free marks an inode as free.
func (t *Table) Free(ctx context.Context, inodeNum uint32) error {
	if uint64(inodeNum) >= t.totalInodes {
		return ErrInodeOutOfRange
	}

	if inodeNum < InodeFirstUsable {
		return fmt.Errorf("cannot free reserved inode %d", inodeNum)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Acquire distributed lock if available
	if t.lockMgr != nil {
		if err := t.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(inodeNum), lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire inode lock: %w", err)
		}
		defer t.lockMgr.Release(lock.LockTypeInode, uint64(inodeNum))
	}

	// Read current inode
	inode, err := t.readInodeUnlocked(inodeNum)
	if err != nil {
		return err
	}

	// Verify it's in use
	if inode.Magic == 0 {
		return ErrInodeNotInUse
	}

	// Clear the inode (set Magic to 0)
	emptyInode := &Inode{
		InodeNum: inodeNum,
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

	// Serialize the inode
	inodeData := serializeInode(inode)

	// Copy into the sector at the correct offset
	start := inodeOffset * InodeSize
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

	// Timestamps
	binary.LittleEndian.PutUint64(buf[40:48], uint64(inode.Atime))
	binary.LittleEndian.PutUint64(buf[48:56], uint64(inode.Mtime))
	binary.LittleEndian.PutUint64(buf[56:64], uint64(inode.Ctime))

	// Generation counters
	binary.LittleEndian.PutUint64(buf[64:72], inode.DataGeneration)
	binary.LittleEndian.PutUint64(buf[72:80], inode.MetaGeneration)

	// Last modifier UUID
	copy(buf[80:96], inode.LastModifierUUID[:])

	// Inline extents (2 × 32 bytes = 64 bytes)
	for i, ext := range inode.InlineExtents {
		offset := 96 + i*32
		binary.LittleEndian.PutUint64(buf[offset:offset+8], ext.LogicalBlock)
		binary.LittleEndian.PutUint64(buf[offset+8:offset+16], ext.PhysicalBlock)
		binary.LittleEndian.PutUint64(buf[offset+16:offset+24], ext.Length)
		binary.LittleEndian.PutUint32(buf[offset+24:offset+28], ext.Flags)
		binary.LittleEndian.PutUint32(buf[offset+28:offset+32], ext.Reserved)
	}

	// Extent tree info
	binary.LittleEndian.PutUint64(buf[160:168], inode.ExtentTreeRoot)
	binary.LittleEndian.PutUint64(buf[168:176], inode.ExtentCount)
	binary.LittleEndian.PutUint32(buf[176:180], inode.ExtentDepth)

	// Reserved bytes 180-247 (68 bytes) - already zero

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

	// Check for empty inode (all zeros = free)
	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic == 0 {
		// Return empty inode with just the position
		return &Inode{}, nil
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
		Atime:           int64(binary.LittleEndian.Uint64(buf[40:48])),
		Mtime:           int64(binary.LittleEndian.Uint64(buf[48:56])),
		Ctime:           int64(binary.LittleEndian.Uint64(buf[56:64])),
		DataGeneration:  binary.LittleEndian.Uint64(buf[64:72]),
		MetaGeneration:  binary.LittleEndian.Uint64(buf[72:80]),
		ExtentTreeRoot:  binary.LittleEndian.Uint64(buf[160:168]),
		ExtentCount:     binary.LittleEndian.Uint64(buf[168:176]),
		ExtentDepth:     binary.LittleEndian.Uint32(buf[176:180]),
		Checksum:        stored,
	}

	// Copy UUID
	copy(inode.LastModifierUUID[:], buf[80:96])

	// Deserialize inline extents
	for i := range inode.InlineExtents {
		offset := 96 + i*32
		inode.InlineExtents[i] = InlineExtent{
			LogicalBlock:  binary.LittleEndian.Uint64(buf[offset : offset+8]),
			PhysicalBlock: binary.LittleEndian.Uint64(buf[offset+8 : offset+16]),
			Length:        binary.LittleEndian.Uint64(buf[offset+16 : offset+24]),
			Flags:         binary.LittleEndian.Uint32(buf[offset+24 : offset+28]),
			Reserved:      binary.LittleEndian.Uint32(buf[offset+28 : offset+32]),
		}
	}

	return inode, nil
}

// SizeForInodes calculates the table size needed for a given number of inodes.
// Rounds up to sector boundary.
func SizeForInodes(count uint64) uint64 {
	numSectors := (count + InodesPerSector - 1) / InodesPerSector
	return numSectors * SectorSize
}
