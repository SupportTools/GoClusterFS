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
)

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

// Add adds a new entry to a directory.
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

	// Check if entry already exists
	_, _, err := m.Lookup(ctx, dirIno, name)
	if err == nil {
		return ErrExists
	}
	if err != ErrNotFound {
		return err
	}

	// Create the new entry
	entry := &DirEntry{
		InodeNum: childInode,
		NameLen:  uint8(len(name)),
		FileType: fileType,
		Name:     name,
	}
	entry.RecLen = entrySize(entry)

	// Try to find space in existing blocks
	if dirIno.Size > 0 {
		added, err := m.addToExistingBlock(ctx, dirIno, entry)
		if err != nil {
			return err
		}
		if added {
			return nil
		}
	}

	// No space in existing blocks - allocate a new one
	return m.addToNewBlock(ctx, dirIno, entry)
}

// Remove removes an entry from a directory.
func (m *Manager) Remove(ctx context.Context, dirIno *inode.Inode, name string) error {
	if !dirIno.IsDir() {
		return ErrNotDirectory
	}

	if name == "" {
		return ErrNameEmpty
	}

	if name == "." || name == ".." {
		return fmt.Errorf("cannot remove %q", name)
	}

	// Read all blocks and find the entry
	numBlocks := extent.BlocksForSize(dirIno.Size)
	if numBlocks == 0 {
		return ErrNotFound
	}

	for block := uint64(0); block < numBlocks; block++ {
		// Look up the extent for this logical block
		ext, extOffset, err := m.extentMgr.Lookup(dirIno, block)
		if err != nil {
			// No extent for this block - it's a hole (shouldn't happen for directories)
			continue
		}
		physBlock := ext.PhysicalBlock + extOffset

		// Read the block
		data, err := m.readBlock(physBlock)
		if err != nil {
			return err
		}

		// Scan for the entry
		offset := uint32(0)
		prevOffset := uint32(0)
		found := false

		for offset < uint32(len(data)) {
			entry, err := parseEntry(data[offset:])
			if err != nil {
				break
			}

			if entry.RecLen == 0 {
				break
			}

			if entry.InodeNum != 0 && entry.Name == name {
				// Found it - mark as deleted
				found = true

				// Option 1: Zero the inode number (simple deletion)
				binary.LittleEndian.PutUint64(data[offset:], 0)

				// Option 2: Merge with previous entry if possible
				if prevOffset < offset {
					prevRecLen := binary.LittleEndian.Uint16(data[prevOffset+8:])
					newRecLen := prevRecLen + entry.RecLen
					binary.LittleEndian.PutUint16(data[prevOffset+8:], newRecLen)
				}

				// Write the block back
				if err := m.writeBlock(physBlock, data); err != nil {
					return err
				}

				return nil
			}

			prevOffset = offset
			offset += uint32(entry.RecLen)
		}

		if found {
			break
		}
	}

	return ErrNotFound
}

// List returns all entries in a directory.
func (m *Manager) List(ctx context.Context, dirIno *inode.Inode) ([]DirEntry, error) {
	if !dirIno.IsDir() {
		return nil, ErrNotDirectory
	}

	return m.readEntries(ctx, dirIno)
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
	if dirIno.Size > 0 {
		return fmt.Errorf("directory already has content")
	}

	// Allocate a block for the directory
	ext, err := m.extentMgr.AllocateBlocks(ctx, dirIno, 0, 1)
	if err != nil {
		return fmt.Errorf("failed to allocate directory block: %w", err)
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
	if err := m.writeBlock(ext.PhysicalBlock, data); err != nil {
		return err
	}

	// Update inode size
	dirIno.Size = BlockSize

	return nil
}

// readEntries reads all entries from a directory.
func (m *Manager) readEntries(ctx context.Context, dirIno *inode.Inode) ([]DirEntry, error) {
	if dirIno.Size == 0 {
		return nil, nil
	}

	var entries []DirEntry
	numBlocks := extent.BlocksForSize(dirIno.Size)

	for block := uint64(0); block < numBlocks; block++ {
		// Look up the extent for this logical block
		ext, offset, err := m.extentMgr.Lookup(dirIno, block)
		if err != nil {
			// No extent for this block - it's a hole (shouldn't happen for directories)
			continue
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

// parseBlockEntries parses all directory entries from a block.
func (m *Manager) parseBlockEntries(data []byte) ([]DirEntry, error) {
	var entries []DirEntry
	offset := uint32(0)

	for offset < uint32(len(data)) {
		entry, err := parseEntry(data[offset:])
		if err != nil {
			return entries, nil // End of valid entries
		}

		if entry.RecLen == 0 {
			break
		}

		// Only include non-deleted entries
		if entry.InodeNum != 0 {
			entries = append(entries, *entry)
		}

		offset += uint32(entry.RecLen)
	}

	return entries, nil
}

// addToExistingBlock tries to add an entry to an existing directory block.
func (m *Manager) addToExistingBlock(ctx context.Context, dirIno *inode.Inode, newEntry *DirEntry) (bool, error) {
	numBlocks := extent.BlocksForSize(dirIno.Size)

	for block := uint64(0); block < numBlocks; block++ {
		// Look up the extent for this logical block
		ext, extOffset, err := m.extentMgr.Lookup(dirIno, block)
		if err != nil {
			// No extent for this block - it's a hole
			continue
		}
		physBlock := ext.PhysicalBlock + extOffset

		data, err := m.readBlock(physBlock)
		if err != nil {
			return false, err
		}

		// Try to find space
		offset := uint32(0)
		for offset < uint32(len(data)) {
			entry, err := parseEntry(data[offset:])
			if err != nil {
				break
			}

			if entry.RecLen == 0 {
				break
			}

			// Check if this entry has extra space we can use
			actualSize := entrySize(entry)
			if entry.InodeNum == 0 {
				// Deleted entry - can we reuse it?
				if entry.RecLen >= newEntry.RecLen {
					// Reuse this slot
					newEntry.RecLen = entry.RecLen // Keep the same record length
					serializeEntry(newEntry, data[offset:])
					return true, m.writeBlock(physBlock, data)
				}
			} else if entry.RecLen > actualSize {
				// Entry has trailing space
				freeSpace := entry.RecLen - actualSize
				if freeSpace >= newEntry.RecLen {
					// Shrink existing entry and add new one
					entry.RecLen = actualSize
					serializeEntry(entry, data[offset:])

					// Add new entry after
					newEntry.RecLen = freeSpace
					serializeEntry(newEntry, data[offset+uint32(actualSize):])

					return true, m.writeBlock(physBlock, data)
				}
			}

			offset += uint32(entry.RecLen)
		}
	}

	return false, nil
}

// addToNewBlock allocates a new block and adds the entry.
func (m *Manager) addToNewBlock(ctx context.Context, dirIno *inode.Inode, entry *DirEntry) error {
	// Calculate next block number
	nextBlock := extent.BlocksForSize(dirIno.Size)

	// Allocate a new block
	ext, err := m.extentMgr.AllocateBlocks(ctx, dirIno, nextBlock, 1)
	if err != nil {
		return fmt.Errorf("failed to allocate directory block: %w", err)
	}

	// The entry takes the first chunk (DirChunkSize) to keep RecLen in uint16 range
	entry.RecLen = uint16(DirChunkSize)

	// Write the entry
	data := make([]byte, BlockSize)
	serializeEntry(entry, data)

	if err := m.writeBlock(ext.PhysicalBlock, data); err != nil {
		return err
	}

	// Update directory size
	dirIno.Size += BlockSize

	return nil
}

// readBlock reads a physical block.
func (m *Manager) readBlock(physBlock uint64) ([]byte, error) {
	buf := storage.GetBuffer(storage.SectorSize)
	defer storage.PutBuffer(buf)

	// Read in 4K chunks for O_DIRECT compatibility
	// physBlock is a block index; actual disk offset is dataOffset + physBlock*BlockSize
	data := make([]byte, BlockSize)
	for i := uint64(0); i < BlockSize/storage.SectorSize; i++ {
		offset := m.dataOffset + physBlock*BlockSize + i*storage.SectorSize
		if _, err := m.dev.ReadAt(buf.Bytes(), int64(offset)); err != nil {
			return nil, fmt.Errorf("read block %d: %w", physBlock, err)
		}
		copy(data[i*storage.SectorSize:], buf.Bytes())
	}

	return data, nil
}

// writeBlock writes a physical block.
func (m *Manager) writeBlock(physBlock uint64, data []byte) error {
	buf := storage.GetBuffer(storage.SectorSize)
	defer storage.PutBuffer(buf)

	// Write in 4K chunks for O_DIRECT compatibility
	// physBlock is a block index; actual disk offset is dataOffset + physBlock*BlockSize
	for i := uint64(0); i < BlockSize/storage.SectorSize; i++ {
		copy(buf.Bytes(), data[i*storage.SectorSize:])
		offset := m.dataOffset + physBlock*BlockSize + i*storage.SectorSize
		if _, err := m.dev.WriteAt(buf.Bytes(), int64(offset)); err != nil {
			return fmt.Errorf("write block %d: %w", physBlock, err)
		}
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
