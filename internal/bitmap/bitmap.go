// Package bitmap implements block allocation tracking for GoClusterFS.
//
// The bitmap uses one bit per data block (1MB each). Bits are stored in
// 4096-byte sectors with a CRC32C checksum in the last 4 bytes.
// Bit value 0 = free, 1 = allocated.
package bitmap

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// SectorSize is the size of each bitmap sector (matches storage.SectorSize).
	SectorSize = 4096

	// ChecksumSize is the size of the CRC32C checksum.
	ChecksumSize = 4

	// BitsPerSector is the number of bits stored per sector.
	// (SectorSize - ChecksumSize) * 8 = 4092 * 8 = 32736 bits
	BitsPerSector = (SectorSize - ChecksumSize) * 8

	// BytesPerSector is the usable bytes for bitmap data per sector.
	BytesPerSector = SectorSize - ChecksumSize
)

// Errors
var (
	ErrNoFreeBlocks     = errors.New("no free blocks available")
	ErrBlockOutOfRange  = errors.New("block number out of range")
	ErrInvalidChecksum  = errors.New("bitmap sector checksum mismatch")
	ErrAlreadyAllocated = errors.New("block already allocated")
	ErrAlreadyFree      = errors.New("block already free")
)

// TrimQueuer is the interface for queueing TRIM/discard operations.
// This allows the bitmap to notify a TRIM manager when blocks are freed,
// without creating a circular dependency between packages.
type TrimQueuer interface {
	// Queue adds a single freed block to the TRIM queue.
	Queue(blockNum uint64)
	// QueueRange adds multiple contiguous freed blocks to the TRIM queue.
	QueueRange(startBlock, count uint64)
}

// Region manages block allocation using a bitmap stored on disk.
type Region struct {
	mu          sync.RWMutex
	dev         *storage.Device
	offset      uint64        // Bitmap region start offset on device
	totalBlocks uint64        // Total number of data blocks tracked
	freeBlocks  uint64        // Cached count of free blocks
	lockMgr     *lock.Manager // For distributed locking (optional)
	nodeUUID    [16]byte      // This node's UUID for locking
	trimMgr     TrimQueuer    // Optional TRIM manager for batched discards
}

// NewRegion creates a new bitmap region manager.
//
// Parameters:
//   - dev: The storage device
//   - offset: Byte offset where bitmap region starts on device
//   - totalBlocks: Total number of data blocks to track
//   - lockMgr: Lock manager for distributed locking (can be nil for single-node)
//   - nodeUUID: This node's UUID for lock ownership
func NewRegion(dev *storage.Device, offset, totalBlocks uint64, lockMgr *lock.Manager, nodeUUID [16]byte) *Region {
	return &Region{
		dev:         dev,
		offset:      offset,
		totalBlocks: totalBlocks,
		freeBlocks:  totalBlocks, // Assume all free initially; will be corrected by scan
		lockMgr:     lockMgr,
		nodeUUID:    nodeUUID,
	}
}

// Alloc allocates the requested number of blocks.
// Returns the allocated block numbers, or an error if not enough free blocks.
func (r *Region) Alloc(ctx context.Context, count int) ([]uint64, error) {
	if count <= 0 {
		return nil, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeBlockBitmap, 0, lock.LockModeExclusive); err != nil {
			return nil, fmt.Errorf("failed to acquire bitmap lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeBlockBitmap, 0)
	}

	allocated := make([]uint64, 0, count)

	// Scan through sectors looking for free blocks
	numSectors := (r.totalBlocks + BitsPerSector - 1) / BitsPerSector

	for sectorIdx := uint64(0); sectorIdx < numSectors && len(allocated) < count; sectorIdx++ {
		sectorData, err := r.readSector(sectorIdx)
		if err != nil {
			// Rollback allocations made so far
			for _, blockNum := range allocated {
				r.freeBlockInSector(blockNum)
			}
			return nil, fmt.Errorf("failed to read bitmap sector %d: %w", sectorIdx, err)
		}

		modified := false
		baseBlock := sectorIdx * BitsPerSector

		// Scan bits in this sector
		for byteIdx := 0; byteIdx < BytesPerSector && len(allocated) < count; byteIdx++ {
			b := sectorData[byteIdx]
			if b == 0xFF {
				continue // All bits set, no free blocks in this byte
			}

			for bitIdx := 0; bitIdx < 8 && len(allocated) < count; bitIdx++ {
				blockNum := baseBlock + uint64(byteIdx*8+bitIdx)
				if blockNum >= r.totalBlocks {
					break // Past the last valid block
				}

				if (b & (1 << bitIdx)) == 0 {
					// Bit is 0, block is free - allocate it
					sectorData[byteIdx] |= (1 << bitIdx)
					allocated = append(allocated, blockNum)
					modified = true
				}
			}
		}

		if modified {
			if err := r.writeSector(sectorIdx, sectorData); err != nil {
				// Rollback allocations made so far
				for _, blockNum := range allocated {
					r.freeBlockInSector(blockNum)
				}
				return nil, fmt.Errorf("failed to write bitmap sector %d: %w", sectorIdx, err)
			}
		}
	}

	if len(allocated) < count {
		// Not enough free blocks - rollback
		for _, blockNum := range allocated {
			r.freeBlockInSector(blockNum)
		}
		return nil, ErrNoFreeBlocks
	}

	r.freeBlocks -= uint64(len(allocated))
	log.Printf("[DEBUG] bitmap.Alloc: allocated blocks %v, freeBlocks=%d", allocated, r.freeBlocks)
	return allocated, nil
}

// Free releases a single block back to the free pool.
func (r *Region) Free(ctx context.Context, blockNum uint64) error {
	if blockNum >= r.totalBlocks {
		return ErrBlockOutOfRange
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeBlockBitmap, 0, lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire bitmap lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeBlockBitmap, 0)
	}

	sectorIdx := blockNum / BitsPerSector
	bitOffset := blockNum % BitsPerSector
	byteIdx := bitOffset / 8
	bitIdx := bitOffset % 8

	sectorData, err := r.readSector(sectorIdx)
	if err != nil {
		return fmt.Errorf("failed to read bitmap sector: %w", err)
	}

	// Check if already free
	if (sectorData[byteIdx] & (1 << bitIdx)) == 0 {
		log.Printf("[DEBUG] bitmap.Free: block %d already free!", blockNum)
		return ErrAlreadyFree
	}

	// Clear the bit
	sectorData[byteIdx] &^= (1 << bitIdx)

	if err := r.writeSector(sectorIdx, sectorData); err != nil {
		return fmt.Errorf("failed to write bitmap sector: %w", err)
	}

	r.freeBlocks++
	log.Printf("[DEBUG] bitmap.Free: freed block %d, freeBlocks=%d", blockNum, r.freeBlocks)

	// Queue block for TRIM if manager is configured
	if r.trimMgr != nil {
		r.trimMgr.Queue(blockNum)
	}

	return nil
}

// IsFree checks if a block is free (not allocated).
func (r *Region) IsFree(blockNum uint64) (bool, error) {
	if blockNum >= r.totalBlocks {
		return false, ErrBlockOutOfRange
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	sectorIdx := blockNum / BitsPerSector
	bitOffset := blockNum % BitsPerSector
	byteIdx := bitOffset / 8
	bitIdx := bitOffset % 8

	sectorData, err := r.readSector(sectorIdx)
	if err != nil {
		return false, fmt.Errorf("failed to read bitmap sector: %w", err)
	}

	// Bit 0 = free, bit 1 = allocated
	return (sectorData[byteIdx] & (1 << bitIdx)) == 0, nil
}

// IsAllocated checks if a block is allocated.
func (r *Region) IsAllocated(blockNum uint64) (bool, error) {
	free, err := r.IsFree(blockNum)
	return !free, err
}

// FreeCount returns the current count of free blocks.
// Note: This returns the cached value; use Scan() to get an accurate count.
func (r *Region) FreeCount() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.freeBlocks
}

// TotalBlocks returns the total number of blocks tracked by this bitmap.
func (r *Region) TotalBlocks() uint64 {
	return r.totalBlocks
}

// AllocatedCount returns the number of allocated blocks.
func (r *Region) AllocatedCount() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.totalBlocks - r.freeBlocks
}

// SetTrimManager sets the TRIM manager for batched block discards.
// When set, freed blocks will be queued for TRIM rather than immediately discarded.
func (r *Region) SetTrimManager(tm TrimQueuer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.trimMgr = tm
}

// Scan reads the entire bitmap and returns accurate free/allocated counts.
// This also updates the cached freeBlocks count.
func (r *Region) Scan(ctx context.Context) (free, allocated uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	numSectors := (r.totalBlocks + BitsPerSector - 1) / BitsPerSector
	var totalFree, totalAllocated uint64

	for sectorIdx := uint64(0); sectorIdx < numSectors; sectorIdx++ {
		sectorData, err := r.readSector(sectorIdx)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to read bitmap sector %d: %w", sectorIdx, err)
		}

		baseBlock := sectorIdx * BitsPerSector

		for byteIdx := 0; byteIdx < BytesPerSector; byteIdx++ {
			for bitIdx := 0; bitIdx < 8; bitIdx++ {
				blockNum := baseBlock + uint64(byteIdx*8+bitIdx)
				if blockNum >= r.totalBlocks {
					break
				}

				if (sectorData[byteIdx] & (1 << bitIdx)) == 0 {
					totalFree++
				} else {
					totalAllocated++
				}
			}
		}
	}

	r.freeBlocks = totalFree
	return totalFree, totalAllocated, nil
}

// MarkAllocated marks a specific block as allocated.
// Used during filesystem initialization or recovery.
func (r *Region) MarkAllocated(ctx context.Context, blockNum uint64) error {
	if blockNum >= r.totalBlocks {
		return ErrBlockOutOfRange
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	sectorIdx := blockNum / BitsPerSector
	bitOffset := blockNum % BitsPerSector
	byteIdx := bitOffset / 8
	bitIdx := bitOffset % 8

	sectorData, err := r.readSector(sectorIdx)
	if err != nil {
		return fmt.Errorf("failed to read bitmap sector: %w", err)
	}

	// Check if already allocated
	if (sectorData[byteIdx] & (1 << bitIdx)) != 0 {
		return ErrAlreadyAllocated
	}

	// Set the bit
	sectorData[byteIdx] |= (1 << bitIdx)

	if err := r.writeSector(sectorIdx, sectorData); err != nil {
		return fmt.Errorf("failed to write bitmap sector: %w", err)
	}

	r.freeBlocks--
	return nil
}

// readSector reads a bitmap sector from disk.
// Returns the sector data (without checksum) or an error.
func (r *Region) readSector(sectorIdx uint64) ([]byte, error) {
	offset := int64(r.offset) + int64(sectorIdx)*SectorSize

	buf := storage.GetBuffer(SectorSize)
	defer storage.PutBuffer(buf)

	if _, err := r.dev.ReadAt(buf.Bytes(), offset); err != nil {
		return nil, err
	}

	data := buf.Bytes()

	// Verify checksum (skip for all-zero sectors which are valid empty sectors)
	storedChecksum := binary.LittleEndian.Uint32(data[SectorSize-ChecksumSize:])
	computedChecksum := storage.CRC32C(data[:SectorSize-ChecksumSize])

	// All-zero sector is valid (initial state)
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

// writeSector writes a bitmap sector to disk with checksum.
func (r *Region) writeSector(sectorIdx uint64, data []byte) error {
	if len(data) != BytesPerSector {
		return fmt.Errorf("invalid sector data size: %d", len(data))
	}

	offset := int64(r.offset) + int64(sectorIdx)*SectorSize

	buf := storage.GetBuffer(SectorSize)
	defer storage.PutBuffer(buf)

	// Copy data and compute checksum
	copy(buf.Bytes()[:BytesPerSector], data)
	checksum := storage.CRC32C(buf.Bytes()[:SectorSize-ChecksumSize])
	binary.LittleEndian.PutUint32(buf.Bytes()[SectorSize-ChecksumSize:], checksum)

	_, err := r.dev.WriteAt(buf.Bytes(), offset)
	return err
}

// freeBlockInSector is an internal helper to free a block without locking.
// Used during allocation rollback.
func (r *Region) freeBlockInSector(blockNum uint64) error {
	sectorIdx := blockNum / BitsPerSector
	bitOffset := blockNum % BitsPerSector
	byteIdx := bitOffset / 8
	bitIdx := bitOffset % 8

	sectorData, err := r.readSector(sectorIdx)
	if err != nil {
		return err
	}

	sectorData[byteIdx] &^= (1 << bitIdx)

	return r.writeSector(sectorIdx, sectorData)
}

// SizeForBlocks calculates the bitmap region size needed for a given number of blocks.
func SizeForBlocks(totalBlocks uint64) uint64 {
	numSectors := (totalBlocks + BitsPerSector - 1) / BitsPerSector
	return numSectors * SectorSize
}

// BlocksPerSector returns the number of blocks tracked per bitmap sector.
func BlocksPerSector() uint64 {
	return BitsPerSector
}
