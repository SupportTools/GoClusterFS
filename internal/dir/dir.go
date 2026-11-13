// Package dir implements directory operations for GoClusterFS.
//
// Directories are stored as linear lists of directory entries in data blocks.
// Each entry contains an inode number, file type, and name.
//
// Entry format (variable size):
//
//	0-7:   InodeNum (uint64)
//	8-9:   RecLen   (uint16, total record length including padding)
//	10:    NameLen  (uint8)
//	11:    FileType (uint8)
//	12+:   Name     (NameLen bytes, padded to 4-byte boundary)
//
// The RecLen field allows for variable-length entries and facilitates
// entry removal by expanding adjacent entries.
package dir

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"

	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// DirEntryHeaderSize is the fixed portion of a directory entry.
	DirEntryHeaderSize = 12

	// MinDirEntrySize is the minimum size of a directory entry (header + 1 char name + padding).
	MinDirEntrySize = 16

	// MaxNameLen is the maximum length of a filename.
	MaxNameLen = 255

	// BlockSize is the size of data blocks used by directories.
	BlockSize = extent.BlockSize

	// DirChunkSize is the size of directory chunks within a block.
	// Directories are organized in 4KB chunks to keep RecLen within uint16 range.
	DirChunkSize = 4096

	// MaxRecLen is the maximum record length (fits in uint16).
	MaxRecLen = 65535
)

// File types for directory entries (matches POSIX DT_* values)
const (
	DT_UNKNOWN = 0
	DT_REG     = 1  // Regular file
	DT_DIR     = 2  // Directory
	DT_LNK     = 7  // Symbolic link
	DT_BLK     = 6  // Block device
	DT_CHR     = 3  // Character device
	DT_FIFO    = 5  // Named pipe (FIFO)
	DT_SOCK    = 12 // Unix domain socket
)

// Errors
var (
	ErrNameTooLong  = errors.New("filename too long")
	ErrNameEmpty    = errors.New("filename empty")
	ErrNotDirectory = errors.New("not a directory")
	ErrNotFound     = errors.New("entry not found")
	ErrExists       = errors.New("entry already exists")
	ErrNotEmpty     = errors.New("directory not empty")
	ErrInvalidEntry = errors.New("invalid directory entry")
	ErrNoSpace      = errors.New("no space in directory block")
	ErrCorrupted    = errors.New("directory corrupted")
	ErrRemoved      = errors.New("directory was removed")

	// errRetryAdd is an internal sentinel error indicating that a concurrent
	// modification was detected and the Add operation should be retried.
	errRetryAdd = errors.New("retry add operation")
)

// MaxAddRetries is the maximum number of retry attempts for Add operations
// when concurrent modifications are detected.
const MaxAddRetries = 3

// DirEntry represents a directory entry.
type DirEntry struct {
	InodeNum uint64 // Inode number (0 = deleted entry)
	RecLen   uint16 // Total record length including padding
	NameLen  uint8  // Length of name
	FileType uint8  // File type (DT_*)
	Name     string // Entry name
}

// Manager handles directory operations.
type Manager struct {
	dev        *storage.Device
	inodeTbl   *inode.Table
	extentMgr  *extent.Manager
	lockMgr    *lock.Manager
	dataOffset uint64 // Starting disk offset for data blocks
}

// NewManager creates a new directory manager.
// dataOffset is the starting disk offset where data blocks are stored.
func NewManager(dev *storage.Device, inodeTbl *inode.Table, extentMgr *extent.Manager, lockMgr *lock.Manager, dataOffset uint64) *Manager {
	return &Manager{
		dev:        dev,
		inodeTbl:   inodeTbl,
		extentMgr:  extentMgr,
		lockMgr:    lockMgr,
		dataOffset: dataOffset,
	}
}

// Lookup finds an entry by name in a directory.
// Returns the inode number and file type, or ErrNotFound.
func (m *Manager) Lookup(ctx context.Context, dirIno *inode.Inode, name string) (uint64, uint8, error) {
	if !dirIno.IsDir() {
		return 0, 0, ErrNotDirectory
	}

	if name == "" {
		return 0, 0, ErrNameEmpty
	}

	if len(name) > MaxNameLen {
		return 0, 0, ErrNameTooLong
	}

	// Empty directory
	if dirIno.Size == 0 {
		return 0, 0, ErrNotFound
	}

	// Acquire shared lock for read operation
	if m.lockMgr != nil {
		if err := m.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(dirIno.InodeNum), lock.LockModeShared); err != nil {
			return 0, 0, fmt.Errorf("failed to acquire directory lock: %w", err)
		}
		defer m.lockMgr.Release(lock.LockTypeInode, uint64(dirIno.InodeNum))
	}

	// Read directory data and scan for entry
	entries, err := m.readEntries(ctx, dirIno)
	if err != nil {
		return 0, 0, err
	}

	for _, entry := range entries {
		if entry.InodeNum != 0 && entry.Name == name {
			return entry.InodeNum, entry.FileType, nil
		}
	}

	return 0, 0, ErrNotFound
}

// lookupUnlocked finds an entry without acquiring locks.
// Used internally by methods that already hold the directory lock.
func (m *Manager) lookupUnlocked(ctx context.Context, dirIno *inode.Inode, name string) (uint64, uint8, error) {
	if dirIno.Size == 0 {
		return 0, 0, ErrNotFound
	}

	entries, err := m.readEntries(ctx, dirIno)
	if err != nil {
		return 0, 0, err
	}

	for _, entry := range entries {
		if entry.InodeNum != 0 && entry.Name == name {
			return entry.InodeNum, entry.FileType, nil
		}
	}

	return 0, 0, ErrNotFound
}

// Add adds a new entry to a directory.
// Uses an optimistic retry pattern to handle concurrent modifications from other nodes.
func (m *Manager) Add(ctx context.Context, dirIno *inode.Inode, name string, childInode uint64, fileType uint8) error {
	if !dirIno.IsDir() {
		return ErrNotDirectory
	}

	if name == "" {
		return ErrNameEmpty
	}

	if len(name) > MaxNameLen {
		return ErrNameTooLong
	}

	// Create the new entry
	entry := &DirEntry{
		InodeNum: childInode,
		NameLen:  uint8(len(name)),
		FileType: fileType,
		Name:     name,
	}
	entry.RecLen = entrySize(entry)

	// Retry loop to handle concurrent modifications from other nodes.
	// When multiple nodes try to add entries simultaneously, they may race
	// to allocate the same logical block position. If a conflict is detected,
	// errRetryAdd is returned and we retry the operation.
	for attempt := 0; attempt < MaxAddRetries; attempt++ {
		err := m.addInternal(ctx, dirIno, entry)
		if err == errRetryAdd {
			log.Printf("[DEBUG] dir.Add: retry attempt %d for entry %q in inode %d",
				attempt+1, name, dirIno.InodeNum)
			continue
		}
		return err
	}

	return fmt.Errorf("concurrent modification detected after %d retries for entry %q", MaxAddRetries, name)
}

// addInternal performs the actual add operation.
// Returns errRetryAdd if a concurrent modification is detected and the caller should retry.
func (m *Manager) addInternal(ctx context.Context, dirIno *inode.Inode, entry *DirEntry) error {
	// Acquire exclusive lock for write operation
	// NOTE: We do NOT use defer here because we may need to release the lock
	// before calling addToNewBlock (which acquires its own lock). Using defer
	// would cause the lock to be released twice, potentially releasing a lock
	// held by a different goroutine.
	lockHeld := false
	if m.lockMgr != nil {
		if err := m.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(dirIno.InodeNum), lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire directory lock: %w", err)
		}
		lockHeld = true
	}

	freshIno, err := m.liveDirLocked(dirIno.InodeNum)
	if err != nil {
		if lockHeld {
			m.lockMgr.Release(lock.LockTypeInode, uint64(dirIno.InodeNum))
		}
		return err
	}

	// Check if entry already exists (done under lock to avoid TOCTOU)
	_, _, err = m.lookupUnlocked(ctx, freshIno, entry.Name)
	if err == nil {
		if lockHeld {
			m.lockMgr.Release(lock.LockTypeInode, uint64(dirIno.InodeNum))
		}
		return ErrExists
	}
	if err != ErrNotFound {
		if lockHeld {
			m.lockMgr.Release(lock.LockTypeInode, uint64(dirIno.InodeNum))
		}
		return err
	}

	// Try to find space in existing blocks (doesn't need bitmap lock)
	if freshIno.Size > 0 {
		added, err := m.addToExistingBlock(ctx, freshIno, entry)
		if err != nil {
			if lockHeld {
				m.lockMgr.Release(lock.LockTypeInode, uint64(dirIno.InodeNum))
			}
			return err
		}
		if added {
			// Increment MetaGeneration to signal modification
			freshIno.MetaGeneration++
			if err := m.inodeTbl.WriteWithLockHeld(freshIno); err != nil {
				if lockHeld {
					m.lockMgr.Release(lock.LockTypeInode, uint64(dirIno.InodeNum))
				}
				if errors.Is(err, extent.ErrTreeConflict) {
					return errRetryAdd
				}
				return fmt.Errorf("failed to write directory inode: %w", err)
			}
			if lockHeld {
				m.lockMgr.Release(lock.LockTypeInode, uint64(dirIno.InodeNum))
			}
			// Copy the fresh state back to caller's inode
			dirIno.Size = freshIno.Size
			return nil
		}
	}

	// No space in existing blocks - need to allocate a new one
	// Release inode lock before bitmap allocation to respect lock ordering
	// (bitmap lock level 6 < inode lock level 8)
	if lockHeld {
		m.lockMgr.Release(lock.LockTypeInode, uint64(dirIno.InodeNum))
	}

	// Allocate block without holding inode lock
	// CRITICAL: Pass freshIno (not dirIno) so addToNewBlock has the latest size
	// for calculating which block to allocate. addToNewBlock will re-read anyway,
	// but starting with a fresher estimate reduces wasted allocations.
	// NOTE: addToNewBlock handles its own lock acquisition and release.
	// It may return errRetryAdd if a concurrent modification is detected.
	err = m.addToNewBlock(ctx, freshIno, entry)

	// Copy updated state back to caller's inode
	if err == nil {
		dirIno.Size = freshIno.Size
	}

	return err
}

// Remove removes an entry from a directory.
func (m *Manager) Remove(ctx context.Context, dirIno *inode.Inode, name string) error {
	_, err := m.RemoveIf(ctx, dirIno, name, func(uint64) bool { return true })
	return err
}

// RemoveIf removes name if ok accepts the inode it names; ok runs under the directory lock.
func (m *Manager) RemoveIf(ctx context.Context, dirIno *inode.Inode, name string, ok func(child uint64) bool) (bool, error) {
	if !dirIno.IsDir() {
		return false, ErrNotDirectory
	}

	if name == "" {
		return false, ErrNameEmpty
	}

	if name == "." || name == ".." {
		return false, fmt.Errorf("cannot remove %q", name)
	}

	if m.lockMgr != nil {
		if err := m.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(dirIno.InodeNum), lock.LockModeExclusive); err != nil {
			return false, fmt.Errorf("failed to acquire directory lock: %w", err)
		}
		defer m.lockMgr.Release(lock.LockTypeInode, uint64(dirIno.InodeNum))
	}

	dirIno, err := m.liveDirLocked(dirIno.InodeNum)
	if err != nil {
		return false, err
	}

	numBlocks := extent.BlocksForSize(dirIno.Size)
	for block := uint64(0); block < numBlocks; block++ {
		ext, extOffset, err := m.extentMgr.Lookup(dirIno, block)
		if err == extent.ErrExtentNotFound {
			continue
		}
		if err != nil {
			return false, err
		}
		physBlock := ext.PhysicalBlock + extOffset

		data, err := m.readBlock(physBlock)
		if err != nil {
			return false, err
		}
		for start := 0; start < len(data); start += DirChunkSize {
			chunk := data[start : start+DirChunkSize]
			child, found := childInChunk(chunk, name)
			if !found {
				continue
			}
			if !ok(child) {
				return false, nil
			}
			removeFromChunk(chunk, name)
			if err := m.writeChunk(physBlock, start, chunk); err != nil {
				return false, err
			}
			return true, nil
		}
	}

	return false, ErrNotFound
}

// liveDirLocked re-reads directory num under its lock: another node may have changed or removed it.
func (m *Manager) liveDirLocked(num uint32) (*inode.Inode, error) {
	ino, err := m.inodeTbl.Read(num)
	if err != nil {
		return nil, fmt.Errorf("failed to re-read directory inode: %w", err)
	}
	if ino.Magic != inode.InodeMagic || !ino.IsDir() {
		return nil, ErrRemoved
	}
	return ino, nil
}

// List returns all entries in a directory.
func (m *Manager) List(ctx context.Context, dirIno *inode.Inode) ([]DirEntry, error) {
	if !dirIno.IsDir() {
		return nil, ErrNotDirectory
	}

	// Acquire shared lock for read operation
	if m.lockMgr != nil {
		if err := m.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(dirIno.InodeNum), lock.LockModeShared); err != nil {
			return nil, fmt.Errorf("failed to acquire directory lock: %w", err)
		}
		defer m.lockMgr.Release(lock.LockTypeInode, uint64(dirIno.InodeNum))
	}

	// Re-read inode from disk to get current state (extents may have changed)
	freshIno, err := m.inodeTbl.Read(dirIno.InodeNum)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory inode: %w", err)
	}

	return m.readEntries(ctx, freshIno)
}

// IsEmpty returns true if the directory contains only "." and ".." entries.
func (m *Manager) IsEmpty(ctx context.Context, dirIno *inode.Inode) (bool, error) {
	entries, err := m.List(ctx, dirIno)
	if err != nil {
		return false, err
	}

	for _, entry := range entries {
		if entry.InodeNum != 0 && entry.Name != "." && entry.Name != ".." {
			return false, nil
		}
	}

	return true, nil
}

// InitDirectory initializes a new directory with "." and ".." entries.
func (m *Manager) InitDirectory(ctx context.Context, dirIno *inode.Inode, parentInodeNum uint64) error {
	log.Printf("[DEBUG] dir.InitDirectory: inode=%d parent=%d", dirIno.InodeNum, parentInodeNum)
	if dirIno.Size > 0 {
		return fmt.Errorf("directory already has content")
	}

	// IMPORTANT: Block allocation must happen BEFORE acquiring inode lock
	// to respect lock ordering (bitmap lock level 6 < inode lock level 8)
	log.Printf("[DEBUG] dir.InitDirectory: allocating block for inode %d", dirIno.InodeNum)
	ext, err := m.extentMgr.AllocateBlocks(ctx, dirIno, 0, 1)
	if err != nil {
		return fmt.Errorf("failed to allocate directory block: %w", err)
	}
	log.Printf("[DEBUG] dir.InitDirectory: allocated physBlock=%d for inode %d", ext.PhysicalBlock, dirIno.InodeNum)

	// Now acquire exclusive lock for writing directory contents
	if m.lockMgr != nil {
		log.Printf("[DEBUG] dir.InitDirectory: acquiring inode lock for inode %d", dirIno.InodeNum)
		if err := m.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(dirIno.InodeNum), lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire directory lock: %w", err)
		}
		log.Printf("[DEBUG] dir.InitDirectory: acquired inode lock for inode %d", dirIno.InodeNum)
		defer m.lockMgr.Release(lock.LockTypeInode, uint64(dirIno.InodeNum))
	}

	// Create "." entry
	dotEntry := &DirEntry{
		InodeNum: uint64(dirIno.InodeNum),
		NameLen:  1,
		FileType: DT_DIR,
		Name:     ".",
	}
	dotEntry.RecLen = entrySize(dotEntry)

	// Create ".." entry
	dotdotEntry := &DirEntry{
		InodeNum: parentInodeNum,
		NameLen:  2,
		FileType: DT_DIR,
		Name:     "..",
	}
	// ".." entry takes rest of first chunk to allow room for more entries
	// We use DirChunkSize to keep RecLen within uint16 range
	dotdotEntry.RecLen = uint16(DirChunkSize - int(dotEntry.RecLen))

	// Build the block
	data := make([]byte, BlockSize)
	offset := 0

	// Write "."
	serializeEntry(dotEntry, data[offset:])
	offset += int(dotEntry.RecLen)

	// Write ".."
	serializeEntry(dotdotEntry, data[offset:])

	// Write to disk
	log.Printf("[DEBUG] dir.InitDirectory: writing block %d for inode %d", ext.PhysicalBlock, dirIno.InodeNum)
	if err := m.writeBlock(ext.PhysicalBlock, data); err != nil {
		return err
	}
	log.Printf("[DEBUG] dir.InitDirectory: block write complete for inode %d", dirIno.InodeNum)

	// Update inode size and allocated blocks
	dirIno.Size = BlockSize
	dirIno.AllocatedBlocks = 1

	// CRITICAL: Write the inode to disk so subsequent operations
	// (which re-read from disk for distributed consistency) see the current state.
	// Use WriteWithLockHeld since we already hold the inode lock.
	log.Printf("[DEBUG] dir.InitDirectory: writing inode %d to disk", dirIno.InodeNum)
	if err := m.inodeTbl.WriteWithLockHeld(dirIno); err != nil {
		return fmt.Errorf("failed to write directory inode: %w", err)
	}
	log.Printf("[DEBUG] dir.InitDirectory: complete for inode %d", dirIno.InodeNum)

	return nil
}

// readEntries reads all entries from a directory.
func (m *Manager) readEntries(ctx context.Context, dirIno *inode.Inode) ([]DirEntry, error) {
	entries, err := m.readEntriesOf(ctx, dirIno)
	if errors.Is(err, extent.ErrStaleTree) {
		// A cached inode can point at tree nodes another node has rewritten.
		fresh, readErr := m.inodeTbl.Read(dirIno.InodeNum)
		if readErr != nil {
			return nil, err
		}
		return m.readEntriesOf(ctx, fresh)
	}
	return entries, err
}

func (m *Manager) readEntriesOf(ctx context.Context, dirIno *inode.Inode) ([]DirEntry, error) {
	if dirIno.Size == 0 {
		return nil, nil
	}

	var entries []DirEntry
	numBlocks := extent.BlocksForSize(dirIno.Size)

	for block := uint64(0); block < numBlocks; block++ {
		// Look up the extent for this logical block
		ext, offset, err := m.extentMgr.Lookup(dirIno, block)
		if err == extent.ErrExtentNotFound {
			// No extent for this block - it's a hole (shouldn't happen for directories)
			continue
		}
		if err != nil {
			return nil, err
		}
		physBlock := ext.PhysicalBlock + offset

		// Read the block
		data, err := m.readBlock(physBlock)
		if err != nil {
			return nil, err
		}

		// Parse entries from this block
		blockEntries, err := m.parseBlockEntries(data)
		if err != nil {
			return nil, err
		}

		entries = append(entries, blockEntries...)
	}

	return entries, nil
}

// parseBlockEntries parses the live entries of every chunk in a block.
func (m *Manager) parseBlockEntries(data []byte) ([]DirEntry, error) {
	var entries []DirEntry
	for start := 0; start < len(data); start += DirChunkSize {
		walkChunk(data[start:start+DirChunkSize], func(_ int, entry *DirEntry) bool {
			if entry.InodeNum != 0 {
				entries = append(entries, *entry)
			}
			return false
		})
	}
	return entries, nil
}

// addToExistingBlock tries to add an entry to an existing directory block.
func (m *Manager) addToExistingBlock(ctx context.Context, dirIno *inode.Inode, newEntry *DirEntry) (bool, error) {
	numBlocks := extent.BlocksForSize(dirIno.Size)

	for block := uint64(0); block < numBlocks; block++ {
		ext, extOffset, err := m.extentMgr.Lookup(dirIno, block)
		if err == extent.ErrExtentNotFound {
			continue
		}
		if err != nil {
			return false, err
		}
		physBlock := ext.PhysicalBlock + extOffset

		data, err := m.readBlock(physBlock)
		if err != nil {
			return false, err
		}
		for start := 0; start < len(data); start += DirChunkSize {
			chunk := data[start : start+DirChunkSize]
			if addToChunk(chunk, newEntry) {
				return true, m.writeChunk(physBlock, start, chunk)
			}
		}
	}

	return false, nil
}

// walkChunk calls fn with each entry of a 4 KB chunk and its offset until fn
// returns true. An unused chunk starts with RecLen 0 and has no entries.
func walkChunk(chunk []byte, fn func(offset int, entry *DirEntry) bool) {
	for offset := 0; offset < len(chunk); {
		entry, err := parseEntry(chunk[offset:])
		if err != nil || offset+int(entry.RecLen) > len(chunk) {
			return
		}
		if fn(offset, entry) {
			return
		}
		offset += int(entry.RecLen)
	}
}

func chunkUnused(chunk []byte) bool {
	return binary.LittleEndian.Uint16(chunk[8:10]) == 0
}

// addToChunk puts newEntry in a deleted entry, in the space after a live one,
// or at the start of an unused chunk. Records never cross a chunk boundary.
func addToChunk(chunk []byte, newEntry *DirEntry) bool {
	need := entrySize(newEntry)
	if chunkUnused(chunk) {
		newEntry.RecLen = DirChunkSize
		serializeEntry(newEntry, chunk)
		return true
	}
	added := false
	walkChunk(chunk, func(offset int, entry *DirEntry) bool {
		used := entrySize(entry)
		switch {
		case entry.InodeNum == 0 && entry.RecLen >= need:
			newEntry.RecLen = entry.RecLen
			serializeEntry(newEntry, chunk[offset:])
			added = true
		case entry.InodeNum != 0 && entry.RecLen >= used+need:
			newEntry.RecLen = entry.RecLen - used
			entry.RecLen = used
			serializeEntry(entry, chunk[offset:])
			serializeEntry(newEntry, chunk[offset+int(used):])
			added = true
		}
		return added
	})
	return added
}

func childInChunk(chunk []byte, name string) (child uint64, found bool) {
	walkChunk(chunk, func(_ int, entry *DirEntry) bool {
		found = entry.InodeNum != 0 && entry.Name == name
		child = entry.InodeNum
		return found
	})
	return child, found
}

// removeFromChunk deletes the entry called name and gives its space to the
// entry before it in the chunk.
func removeFromChunk(chunk []byte, name string) bool {
	prev := -1
	removed := false
	walkChunk(chunk, func(offset int, entry *DirEntry) bool {
		if entry.InodeNum == 0 || entry.Name != name {
			prev = offset
			return false
		}
		binary.LittleEndian.PutUint64(chunk[offset:], 0)
		if prev >= 0 {
			prevRecLen := binary.LittleEndian.Uint16(chunk[prev+8:])
			binary.LittleEndian.PutUint16(chunk[prev+8:], prevRecLen+entry.RecLen)
		}
		removed = true
		return true
	})
	return removed
}

// addToNewBlock allocates a new block and adds the entry.
// IMPORTANT: This function expects to be called WITHOUT holding the inode lock,
// as it needs to allocate blocks (bitmap lock) before acquiring the inode lock.
// Returns errRetryAdd if a concurrent modification is detected and the caller should retry.
func (m *Manager) addToNewBlock(ctx context.Context, dirIno *inode.Inode, entry *DirEntry) error {
	// Calculate next block number based on current (possibly stale) size
	nextBlock := extent.BlocksForSize(dirIno.Size)

	// Allocate a new block (this acquires bitmap lock - level 6)
	physBlock, err := m.extentMgr.AllocatePhysicalBlock(ctx)
	if err != nil {
		return fmt.Errorf("failed to allocate directory block: %w", err)
	}

	// Track whether we use the allocated block. If we don't, we must free it.
	blockUsed := false
	defer func() {
		if !blockUsed {
			// Free the allocated block since we didn't use it.
			// Ignore errors - best effort cleanup.
			log.Printf("[DEBUG] dir.addToNewBlock: freeing unused block %d for inode %d",
				physBlock, dirIno.InodeNum)
			m.extentMgr.FreePhysicalBlock(ctx, physBlock)
		}
	}()

	// Extent tree blocks also come from the bitmap, so reserve them before the inode lock.
	ctx, releaseTreeBlocks, err := m.extentMgr.ReserveTreeBlocks(ctx, dirIno)
	if errors.Is(err, extent.ErrStaleTree) {
		return errRetryAdd
	}
	if err != nil {
		return fmt.Errorf("failed to reserve extent tree blocks: %w", err)
	}
	defer releaseTreeBlocks()

	// Set once the extent is inserted; an uncommitted tree change is dropped
	// after the inode lock is released, since freeing takes the bitmap lock.
	var pending *inode.Inode
	defer func() {
		if pending != nil && !blockUsed {
			m.extentMgr.DiscardPending(ctx, pending)
		}
	}()

	// Now acquire inode lock for writing (level 8 - after bitmap lock)
	if m.lockMgr != nil {
		if err := m.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(dirIno.InodeNum), lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire directory lock: %w", err)
		}
		defer m.lockMgr.Release(lock.LockTypeInode, uint64(dirIno.InodeNum))
	}

	freshIno, err := m.liveDirLocked(dirIno.InodeNum)
	if err != nil {
		return err
	}

	// CONFLICT DETECTION 1: Check if entry already exists.
	// Another node may have added this entry while we were allocating the block.
	_, _, lookupErr := m.lookupUnlocked(ctx, freshIno, entry.Name)
	if lookupErr == nil {
		// Entry was added by a concurrent operation - return ErrExists
		log.Printf("[DEBUG] dir.addToNewBlock: entry %q already exists in inode %d (added by another node)",
			entry.Name, dirIno.InodeNum)
		return ErrExists
	}
	if lookupErr != ErrNotFound {
		return lookupErr
	}

	// Check if the directory has grown since we released the lock
	freshNextBlock := extent.BlocksForSize(freshIno.Size)
	if freshNextBlock > nextBlock {
		// Another node added blocks while we were waiting.
		// Try to add to existing blocks first (they may have free space now).
		added, err := m.addToExistingBlock(ctx, freshIno, entry)
		if err != nil {
			return err
		}
		if added {
			// Successfully added to existing block.
			// Increment MetaGeneration and write inode.
			freshIno.MetaGeneration++
			if err := m.inodeTbl.WriteWithLockHeld(freshIno); err != nil {
				if errors.Is(err, extent.ErrTreeConflict) {
					return errRetryAdd
				}
				return fmt.Errorf("failed to write directory inode: %w", err)
			}
			// The defer will free the block we allocated since blockUsed is false.
			dirIno.Size = freshIno.Size
			return nil
		}
		// No space in existing blocks, use our newly allocated block
		// but at the correct position based on fresh state
		nextBlock = freshNextBlock
	}

	// CONFLICT DETECTION 2: Check if our target logical block is already occupied.
	// Another node may have inserted an extent at this position while we were waiting.
	_, _, extErr := m.extentMgr.Lookup(freshIno, nextBlock)
	if extErr == nil {
		// Block position is already taken - signal caller to retry.
		// The defer will free our allocated block.
		log.Printf("[DEBUG] dir.addToNewBlock: logical block %d already occupied in inode %d, retrying",
			nextBlock, dirIno.InodeNum)
		return errRetryAdd
	}
	// If extErr is not "not found", it's an unexpected error
	if extErr != extent.ErrExtentNotFound {
		return fmt.Errorf("failed to check extent: %w", extErr)
	}

	// The entry takes the first chunk (DirChunkSize) to keep RecLen in uint16 range
	entry.RecLen = uint16(DirChunkSize)

	// Write the entry data to the allocated physical block
	data := make([]byte, BlockSize)
	serializeEntry(entry, data)

	if err := m.writeBlock(physBlock, data); err != nil {
		return err
	}

	// Map the block into the fresh inode at the correct logical block.
	newExt := &extent.Extent{
		LogicalBlock:  nextBlock,
		PhysicalBlock: physBlock,
		Length:        1,
		Flags:         extent.FlagAllocated,
	}
	if err := m.extentMgr.Insert(ctx, freshIno, newExt); err != nil {
		// Overlap: another node beat us. Deadlock risk: the reservation was too
		// small for the fresh inode, so retry outside the lock.
		if errors.Is(err, extent.ErrExtentOverlap) || errors.Is(err, extent.ErrStaleTree) || errors.Is(err, lock.ErrDeadlockRisk) {
			log.Printf("[DEBUG] dir.addToNewBlock: extent insert conflict in inode %d: %v, retrying",
				dirIno.InodeNum, err)
			return errRetryAdd
		}
		return fmt.Errorf("failed to insert extent: %w", err)
	}
	pending = freshIno

	// Update directory size and allocated blocks
	freshIno.Size = uint64(nextBlock+1) * BlockSize
	freshIno.AllocatedBlocks++
	freshIno.MetaGeneration++

	// Write the updated inode to disk.
	// Use WriteWithLockHeld since we already hold the inode lock.
	if err := m.inodeTbl.WriteWithLockHeld(freshIno); err != nil {
		if errors.Is(err, extent.ErrTreeConflict) {
			return errRetryAdd
		}
		blockUsed = true // the inode on disk may already map physBlock
		return fmt.Errorf("failed to write directory inode: %w", err)
	}

	// Block is now successfully committed to the filesystem.
	// Mark as used so the defer doesn't free it.
	blockUsed = true

	// Copy the fresh size back to the caller's inode
	dirIno.Size = freshIno.Size

	return nil
}

func (m *Manager) blockOffset(physBlock uint64) int64 {
	return int64(m.dataOffset + physBlock*BlockSize)
}

func (m *Manager) readBlock(physBlock uint64) ([]byte, error) {
	buf := storage.GetBuffer(BlockSize)
	defer storage.PutBuffer(buf)
	if _, err := m.dev.ReadAt(buf.Bytes()[:BlockSize], m.blockOffset(physBlock)); err != nil {
		return nil, fmt.Errorf("read block %d: %w", physBlock, err)
	}
	return append([]byte(nil), buf.Bytes()[:BlockSize]...), nil
}

func (m *Manager) writeBlock(physBlock uint64, data []byte) error {
	buf := storage.GetBuffer(BlockSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), data)
	if _, err := m.dev.WriteAt(buf.Bytes()[:BlockSize], m.blockOffset(physBlock)); err != nil {
		return fmt.Errorf("write block %d: %w", physBlock, err)
	}
	return nil
}

// writeChunk writes only the chunk that changed, so the rest of the block is never rewritten from a stale copy.
func (m *Manager) writeChunk(physBlock uint64, start int, chunk []byte) error {
	buf := storage.GetBuffer(DirChunkSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), chunk)
	if _, err := m.dev.WriteAt(buf.Bytes()[:DirChunkSize], m.blockOffset(physBlock)+int64(start)); err != nil {
		return fmt.Errorf("write block %d chunk %d: %w", physBlock, start/DirChunkSize, err)
	}
	return nil
}

// serializeEntry writes a directory entry to a buffer.
func serializeEntry(entry *DirEntry, buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:8], entry.InodeNum)
	binary.LittleEndian.PutUint16(buf[8:10], entry.RecLen)
	buf[10] = entry.NameLen
	buf[11] = entry.FileType
	// Use int(entry.NameLen) to avoid uint8 overflow (12 + 255 would overflow to 11)
	copy(buf[12:12+int(entry.NameLen)], entry.Name)
}

// parseEntry parses a directory entry from a buffer.
func parseEntry(buf []byte) (*DirEntry, error) {
	if len(buf) < DirEntryHeaderSize {
		return nil, ErrInvalidEntry
	}

	entry := &DirEntry{
		InodeNum: binary.LittleEndian.Uint64(buf[0:8]),
		RecLen:   binary.LittleEndian.Uint16(buf[8:10]),
		NameLen:  buf[10],
		FileType: buf[11],
	}

	if entry.RecLen < DirEntryHeaderSize {
		return nil, ErrInvalidEntry
	}

	if int(entry.NameLen) > len(buf)-DirEntryHeaderSize {
		return nil, ErrInvalidEntry
	}

	if entry.NameLen > 0 {
		// Use int(entry.NameLen) to avoid uint8 overflow
		entry.Name = string(buf[12 : 12+int(entry.NameLen)])
	}

	return entry, nil
}

// entrySize calculates the actual size needed for an entry (with 4-byte alignment).
func entrySize(entry *DirEntry) uint16 {
	// Header (12) + name length, rounded up to 4-byte boundary
	size := DirEntryHeaderSize + int(entry.NameLen)
	return uint16((size + 3) &^ 3) // Round up to multiple of 4
}

// FileTypeFromMode converts an inode mode to a directory file type.
func FileTypeFromMode(mode uint16) uint8 {
	switch mode & inode.S_IFMT {
	case inode.S_IFREG:
		return DT_REG
	case inode.S_IFDIR:
		return DT_DIR
	case inode.S_IFLNK:
		return DT_LNK
	case inode.S_IFBLK:
		return DT_BLK
	case inode.S_IFCHR:
		return DT_CHR
	case inode.S_IFIFO:
		return DT_FIFO
	case inode.S_IFSOCK:
		return DT_SOCK
	default:
		return DT_UNKNOWN
	}
}
