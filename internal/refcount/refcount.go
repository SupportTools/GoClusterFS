// Package refcount implements block reference counting for GoClusterFS snapshots.
//
// Reference counting enables Copy-on-Write (CoW) semantics for snapshots:
//   - RefCount = 1: Block is exclusive to one file, can modify in place
//   - RefCount > 1: Block is shared (via snapshot), must CoW before writing
//
// This allows O(1) snapshot creation - just increment refcounts of all blocks.
// When a write targets a shared block, allocate a new block and copy data.
//
// Layout: Each entry is 8 bytes (4-byte count + 2-byte flags + 2-byte reserved).
// Entries are stored in 4KB sectors with a CRC32C checksum at the end.
// For 1TB with 1MB blocks: 1M entries × 8 bytes = 8MB overhead.
package refcount

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// EntrySize is the size of each reference count entry in bytes.
	// 4 bytes RefCount + 2 bytes Flags + 2 bytes Reserved = 8 bytes.
	EntrySize = 8

	// SectorSize is the size of each disk sector (matches storage.SectorSize).
	SectorSize = 4096

	// ChecksumSize is the size of the CRC32C checksum.
	ChecksumSize = 4

	// EntriesPerSector is the number of refcount entries per sector.
	// (SectorSize - ChecksumSize) / EntrySize = (4096 - 4) / 8 = 511
	EntriesPerSector = (SectorSize - ChecksumSize) / EntrySize

	// BytesPerSector is the usable bytes for refcount data per sector.
	BytesPerSector = EntriesPerSector * EntrySize
)

// Entry flags for reference count entries.
type EntryFlags uint16

const (
	// FlagNone indicates no special flags.
	FlagNone EntryFlags = 0

	// FlagSnapshot indicates the block is part of a snapshot.
	FlagSnapshot EntryFlags = 1 << 0

	// FlagCowPending indicates the block needs CoW on next write.
	FlagCowPending EntryFlags = 1 << 1

	// FlagMetadata indicates the block contains metadata (not user data).
	FlagMetadata EntryFlags = 1 << 2

	// FlagPinned indicates the block should not be freed even at refcount 0.
	// Used during certain operations to prevent premature block reuse.
	FlagPinned EntryFlags = 1 << 3
)

// Errors
var (
	ErrBlockOutOfRange   = errors.New("block number out of range")
	ErrRefCountOverflow  = errors.New("reference count overflow")
	ErrRefCountUnderflow = errors.New("reference count underflow")
	ErrInvalidChecksum   = errors.New("refcount sector checksum mismatch")
	ErrAlreadyZero       = errors.New("reference count already zero")
)

// Entry represents a single reference count entry.
// This is the in-memory representation; on-disk layout is 8 bytes.
type Entry struct {
	RefCount uint32     // Number of references to this block
	Flags    EntryFlags // Entry flags (snapshot, CoW pending, etc.)
}

// IsShared returns true if the block is shared (refcount > 1).
func (e Entry) IsShared() bool {
	return e.RefCount > 1
}

// IsExclusive returns true if the block is exclusive (refcount == 1).
func (e Entry) IsExclusive() bool {
	return e.RefCount == 1
}

// IsFree returns true if the block is free (refcount == 0).
func (e Entry) IsFree() bool {
	return e.RefCount == 0
}

// NeedsCoW returns true if the block needs copy-on-write before modification.
func (e Entry) NeedsCoW() bool {
	return e.RefCount > 1 || (e.Flags&FlagCowPending) != 0
}

// Region manages reference counts for data blocks.
type Region struct {
	mu          sync.RWMutex
	dev         *storage.Device
	offset      uint64        // RefCount region start offset on device
	totalBlocks uint64        // Total number of data blocks tracked
	lockMgr     *lock.Manager // For distributed locking (optional)
}

// NewRegion creates a new reference count region manager.
//
// Parameters:
//   - dev: The storage device
//   - offset: Byte offset where refcount region starts on device
//   - totalBlocks: Total number of data blocks to track
//   - lockMgr: Lock manager for distributed locking (can be nil for single-node)
func NewRegion(dev *storage.Device, offset, totalBlocks uint64, lockMgr *lock.Manager) *Region {
	return &Region{
		dev:         dev,
		offset:      offset,
		totalBlocks: totalBlocks,
		lockMgr:     lockMgr,
	}
}

// Get returns the reference count entry for a block.
func (r *Region) Get(ctx context.Context, blockNum uint64) (Entry, error) {
	if blockNum >= r.totalBlocks {
		return Entry{}, ErrBlockOutOfRange
	}

	// Acquire distributed lock if available (shared mode for read)
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeRefCount, 0, lock.LockModeShared); err != nil {
			return Entry{}, fmt.Errorf("failed to acquire refcount lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeRefCount, 0)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.readEntry(blockNum)
}

// Increment increases the reference count for a block.
// Returns the new count after incrementing.
func (r *Region) Increment(ctx context.Context, blockNum uint64) (uint32, error) {
	if blockNum >= r.totalBlocks {
		return 0, ErrBlockOutOfRange
	}

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeRefCount, 0, lock.LockModeExclusive); err != nil {
			return 0, fmt.Errorf("failed to acquire refcount lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeRefCount, 0)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	entry, err := r.readEntry(blockNum)
	if err != nil {
		return 0, err
	}

	// Check for overflow
	if entry.RefCount == 0xFFFFFFFF {
		return 0, ErrRefCountOverflow
	}

	entry.RefCount++
	if err := r.writeEntry(blockNum, entry); err != nil {
		return 0, err
	}

	return entry.RefCount, nil
}

// Decrement decreases the reference count for a block.
// Returns the new count after decrementing.
// Returns ErrRefCountUnderflow if the count is already zero.
func (r *Region) Decrement(ctx context.Context, blockNum uint64) (uint32, error) {
	if blockNum >= r.totalBlocks {
		return 0, ErrBlockOutOfRange
	}

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeRefCount, 0, lock.LockModeExclusive); err != nil {
			return 0, fmt.Errorf("failed to acquire refcount lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeRefCount, 0)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	entry, err := r.readEntry(blockNum)
	if err != nil {
		return 0, err
	}

	// Check for underflow
	if entry.RefCount == 0 {
		return 0, ErrRefCountUnderflow
	}

	entry.RefCount--
	if err := r.writeEntry(blockNum, entry); err != nil {
		return 0, err
	}

	return entry.RefCount, nil
}

// Set directly sets the reference count for a block.
// Used during initialization or recovery. For normal operations, use Increment/Decrement.
func (r *Region) Set(ctx context.Context, blockNum uint64, count uint32, flags EntryFlags) error {
	if blockNum >= r.totalBlocks {
		return ErrBlockOutOfRange
	}

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeRefCount, 0, lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire refcount lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeRefCount, 0)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	entry := Entry{
		RefCount: count,
		Flags:    flags,
	}
	return r.writeEntry(blockNum, entry)
}

// SetFlags updates the flags for a block without changing the reference count.
func (r *Region) SetFlags(ctx context.Context, blockNum uint64, flags EntryFlags) error {
	if blockNum >= r.totalBlocks {
		return ErrBlockOutOfRange
	}

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeRefCount, 0, lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire refcount lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeRefCount, 0)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	entry, err := r.readEntry(blockNum)
	if err != nil {
		return err
	}

	entry.Flags = flags
	return r.writeEntry(blockNum, entry)
}

// IncrementBatch increments reference counts for multiple blocks atomically.
// This is more efficient than calling Increment individually for each block.
// Returns the new counts after incrementing.
func (r *Region) IncrementBatch(ctx context.Context, blockNums []uint64) ([]uint32, error) {
	if len(blockNums) == 0 {
		return nil, nil
	}

	for _, blockNum := range blockNums {
		if blockNum >= r.totalBlocks {
			return nil, ErrBlockOutOfRange
		}
	}

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeRefCount, 0, lock.LockModeExclusive); err != nil {
			return nil, fmt.Errorf("failed to acquire refcount lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeRefCount, 0)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	results := make([]uint32, len(blockNums))

	// Group blocks by sector for efficient I/O
	sectorCache := make(map[uint64][]byte)

	for i, blockNum := range blockNums {
		sectorIdx := blockNum / EntriesPerSector

		// Load sector if not cached
		if _, ok := sectorCache[sectorIdx]; !ok {
			sectorData, err := r.readSector(sectorIdx)
			if err != nil {
				return nil, err
			}
			sectorCache[sectorIdx] = sectorData
		}

		// Read and increment entry
		entry := decodeEntry(sectorCache[sectorIdx], blockNum%EntriesPerSector)
		if entry.RefCount == 0xFFFFFFFF {
			return nil, ErrRefCountOverflow
		}
		entry.RefCount++
		results[i] = entry.RefCount
		encodeEntry(sectorCache[sectorIdx], blockNum%EntriesPerSector, entry)
	}

	// Write back modified sectors
	for sectorIdx, sectorData := range sectorCache {
		if err := r.writeSector(sectorIdx, sectorData); err != nil {
			return nil, err
		}
	}

	return results, nil
}

// DecrementBatch decrements reference counts for multiple blocks atomically.
// Returns the new counts after decrementing.
// Returns ErrRefCountUnderflow if any block's count is already zero.
func (r *Region) DecrementBatch(ctx context.Context, blockNums []uint64) ([]uint32, error) {
	if len(blockNums) == 0 {
		return nil, nil
	}

	for _, blockNum := range blockNums {
		if blockNum >= r.totalBlocks {
			return nil, ErrBlockOutOfRange
		}
	}

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeRefCount, 0, lock.LockModeExclusive); err != nil {
			return nil, fmt.Errorf("failed to acquire refcount lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeRefCount, 0)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	results := make([]uint32, len(blockNums))

	// Group blocks by sector for efficient I/O
	sectorCache := make(map[uint64][]byte)

	for i, blockNum := range blockNums {
		sectorIdx := blockNum / EntriesPerSector

		// Load sector if not cached
		if _, ok := sectorCache[sectorIdx]; !ok {
			sectorData, err := r.readSector(sectorIdx)
			if err != nil {
				return nil, err
			}
			sectorCache[sectorIdx] = sectorData
		}

		// Read and decrement entry
		entry := decodeEntry(sectorCache[sectorIdx], blockNum%EntriesPerSector)
		if entry.RefCount == 0 {
			return nil, ErrRefCountUnderflow
		}
		entry.RefCount--
		results[i] = entry.RefCount
		encodeEntry(sectorCache[sectorIdx], blockNum%EntriesPerSector, entry)
	}

	// Write back modified sectors
	for sectorIdx, sectorData := range sectorCache {
		if err := r.writeSector(sectorIdx, sectorData); err != nil {
			return nil, err
		}
	}

	return results, nil
}

// Scan reads all reference counts and returns statistics.
// This also validates checksums for all sectors.
func (r *Region) Scan(ctx context.Context) (stats ScanStats, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	numSectors := (r.totalBlocks + EntriesPerSector - 1) / EntriesPerSector

	for sectorIdx := uint64(0); sectorIdx < numSectors; sectorIdx++ {
		sectorData, err := r.readSector(sectorIdx)
		if err != nil {
			return stats, fmt.Errorf("failed to read sector %d: %w", sectorIdx, err)
		}

		baseBlock := sectorIdx * EntriesPerSector
		for entryIdx := uint64(0); entryIdx < EntriesPerSector; entryIdx++ {
			blockNum := baseBlock + entryIdx
			if blockNum >= r.totalBlocks {
				break
			}

			entry := decodeEntry(sectorData, entryIdx)
			switch entry.RefCount {
			case 0:
				stats.FreeBlocks++
			case 1:
				stats.ExclusiveBlocks++
			default:
				stats.SharedBlocks++
				stats.TotalSharedRefs += uint64(entry.RefCount)
			}

			if entry.Flags&FlagSnapshot != 0 {
				stats.SnapshotBlocks++
			}
		}
	}

	stats.TotalBlocks = r.totalBlocks
	return stats, nil
}

// ScanStats contains statistics from a refcount scan.
type ScanStats struct {
	TotalBlocks     uint64 // Total blocks tracked
	FreeBlocks      uint64 // Blocks with refcount == 0
	ExclusiveBlocks uint64 // Blocks with refcount == 1
	SharedBlocks    uint64 // Blocks with refcount > 1
	TotalSharedRefs uint64 // Sum of refcounts for shared blocks
	SnapshotBlocks  uint64 // Blocks with FlagSnapshot set
}

// TotalBlocks returns the total number of blocks tracked.
func (r *Region) TotalBlocks() uint64 {
	return r.totalBlocks
}

// readEntry reads a single entry from disk.
func (r *Region) readEntry(blockNum uint64) (Entry, error) {
	sectorIdx := blockNum / EntriesPerSector
	entryIdx := blockNum % EntriesPerSector

	sectorData, err := r.readSector(sectorIdx)
	if err != nil {
		return Entry{}, err
	}

	return decodeEntry(sectorData, entryIdx), nil
}

// writeEntry writes a single entry to disk.
func (r *Region) writeEntry(blockNum uint64, entry Entry) error {
	sectorIdx := blockNum / EntriesPerSector
	entryIdx := blockNum % EntriesPerSector

	sectorData, err := r.readSector(sectorIdx)
	if err != nil {
		return err
	}

	encodeEntry(sectorData, entryIdx, entry)
	return r.writeSector(sectorIdx, sectorData)
}

// readSector reads a refcount sector from disk.
// Returns the sector data (without checksum) or an error.
func (r *Region) readSector(sectorIdx uint64) ([]byte, error) {
	offset := int64(r.offset) + int64(sectorIdx)*SectorSize

	buf := storage.GetBuffer(SectorSize)
	defer storage.PutBuffer(buf)

	if _, err := r.dev.ReadAt(buf.Bytes(), offset); err != nil {
		return nil, err
	}

	data := buf.Bytes()

	// Verify checksum (skip for all-zero sectors which are valid initial state)
	storedChecksum := binary.LittleEndian.Uint32(data[SectorSize-ChecksumSize:])
	computedChecksum := storage.CRC32C(data[:SectorSize-ChecksumSize])

	// All-zero sector is valid (initial state - all refcounts are 0)
	if storedChecksum == 0 {
		allZero := true
		for i := 0; i < SectorSize-ChecksumSize; i++ {
			if data[i] != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			// Return a copy of the zero data
			result := make([]byte, BytesPerSector)
			return result, nil
		}
	}

	if storedChecksum != computedChecksum {
		return nil, ErrInvalidChecksum
	}

	// Return a copy of the data portion
	result := make([]byte, BytesPerSector)
	copy(result, data[:BytesPerSector])
	return result, nil
}

// writeSector writes a refcount sector to disk with checksum.
func (r *Region) writeSector(sectorIdx uint64, data []byte) error {
	if len(data) != BytesPerSector {
		return fmt.Errorf("invalid sector data size: %d", len(data))
	}

	offset := int64(r.offset) + int64(sectorIdx)*SectorSize

	buf := storage.GetBuffer(SectorSize)
	defer storage.PutBuffer(buf)

	// Copy data and compute checksum
	copy(buf.Bytes()[:BytesPerSector], data)
	// Zero the padding between BytesPerSector and SectorSize-ChecksumSize
	for i := BytesPerSector; i < SectorSize-ChecksumSize; i++ {
		buf.Bytes()[i] = 0
	}
	checksum := storage.CRC32C(buf.Bytes()[:SectorSize-ChecksumSize])
	binary.LittleEndian.PutUint32(buf.Bytes()[SectorSize-ChecksumSize:], checksum)

	_, err := r.dev.WriteAt(buf.Bytes(), offset)
	return err
}

// decodeEntry decodes an entry from sector data at the given index.
func decodeEntry(sectorData []byte, entryIdx uint64) Entry {
	offset := entryIdx * EntrySize
	return Entry{
		RefCount: binary.LittleEndian.Uint32(sectorData[offset : offset+4]),
		Flags:    EntryFlags(binary.LittleEndian.Uint16(sectorData[offset+4 : offset+6])),
		// Reserved bytes at offset+6:offset+8 are ignored
	}
}

// encodeEntry encodes an entry into sector data at the given index.
func encodeEntry(sectorData []byte, entryIdx uint64, entry Entry) {
	offset := entryIdx * EntrySize
	binary.LittleEndian.PutUint32(sectorData[offset:offset+4], entry.RefCount)
	binary.LittleEndian.PutUint16(sectorData[offset+4:offset+6], uint16(entry.Flags))
	// Reserved bytes at offset+6:offset+8 are zeroed
	sectorData[offset+6] = 0
	sectorData[offset+7] = 0
}

// SizeForBlocks calculates the refcount region size needed for a given number of blocks.
func SizeForBlocks(totalBlocks uint64) uint64 {
	numSectors := (totalBlocks + EntriesPerSector - 1) / EntriesPerSector
	return numSectors * SectorSize
}

// Initialize initializes the refcount region with all counts set to 0.
// This should be called during filesystem format.
func (r *Region) Initialize(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	numSectors := (r.totalBlocks + EntriesPerSector - 1) / EntriesPerSector

	// Create an empty sector (all zeros)
	emptyData := make([]byte, BytesPerSector)

	for sectorIdx := uint64(0); sectorIdx < numSectors; sectorIdx++ {
		if err := r.writeSector(sectorIdx, emptyData); err != nil {
			return fmt.Errorf("failed to initialize sector %d: %w", sectorIdx, err)
		}
	}

	return nil
}
