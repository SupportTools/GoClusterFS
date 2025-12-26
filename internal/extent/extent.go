// Package extent implements extent-based block mapping for GoClusterFS.
//
// Files store their block mappings as extents - contiguous ranges of blocks.
// Each extent maps a logical file offset to a physical disk location.
//
// Storage modes:
//   - Inline: Up to 2 extents stored directly in the inode (64 bytes)
//   - Tree: Overflow extents stored in a B-tree (future implementation)
//
// Extent flags support:
//   - Allocated: Physical blocks are allocated
//   - Unwritten: Allocated but not yet written (lazy zeroing)
//   - Shared: Block is shared with snapshots (CoW required)
package extent

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sort"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// ExtentSize is the size of each extent record in bytes.
	ExtentSize = 32

	// BlockSize is the size of each data block (1MB).
	BlockSize = 1 << 20 // 1MB

	// MaxInlineExtents is the number of extents that fit in an inode.
	MaxInlineExtents = 2
)

// Extent flags
const (
	FlagAllocated = 1 << 0 // Physical block is allocated
	FlagUnwritten = 1 << 1 // Allocated but lazy-zero (not yet written)
	FlagShared    = 1 << 2 // Shared with snapshot (requires CoW)
	FlagSFB       = 1 << 3 // Stored in SFB container (not full block)
)

// Errors
var (
	ErrExtentNotFound    = errors.New("extent not found")
	ErrNoSpaceForExtent  = errors.New("no space for new extent (tree required)")
	ErrExtentOverlap     = errors.New("extent overlap detected")
	ErrInvalidExtent     = errors.New("invalid extent")
	ErrBlockNotAllocated = errors.New("block not allocated")
	ErrCoWRequired       = errors.New("copy-on-write required for shared block")
	ErrCoWMultiBlock     = errors.New("CoW on multi-block extents not yet supported")
)

// CoWResult contains the result of a copy-on-write operation.
type CoWResult struct {
	OldPhysBlock uint64 // Original physical block (for refcount decrement)
	NewPhysBlock uint64 // New physical block after CoW
	WasShared    bool   // True if CoW was actually performed
}

// Extent represents a contiguous range of blocks.
type Extent struct {
	LogicalBlock  uint64 // Logical file offset in blocks
	PhysicalBlock uint64 // Physical disk block number
	Length        uint64 // Number of blocks
	Flags         uint32 // Extent flags
}

// Manager manages extent operations for the filesystem.
type Manager struct {
	dev     *storage.Device
	bitmap  *bitmap.Region
	lockMgr *lock.Manager
}

// NewManager creates a new extent manager.
func NewManager(dev *storage.Device, bm *bitmap.Region, lockMgr *lock.Manager) *Manager {
	return &Manager{
		dev:     dev,
		bitmap:  bm,
		lockMgr: lockMgr,
	}
}

// Lookup finds the extent containing the given logical block.
// Returns the extent and the offset within that extent, or ErrExtentNotFound.
func (m *Manager) Lookup(ino *inode.Inode, logicalBlock uint64) (*Extent, uint64, error) {
	// Check inline extents first
	for _, inline := range ino.InlineExtents {
		if inline.Length == 0 {
			continue // Empty slot
		}

		ext := &Extent{
			LogicalBlock:  inline.LogicalBlock,
			PhysicalBlock: inline.PhysicalBlock,
			Length:        inline.Length,
			Flags:         inline.Flags,
		}

		if logicalBlock >= ext.LogicalBlock && logicalBlock < ext.LogicalBlock+ext.Length {
			offset := logicalBlock - ext.LogicalBlock
			return ext, offset, nil
		}
	}

	// TODO: Check extent tree if FlagHasExtentTree is set
	if ino.HasExtentTree() {
		return nil, 0, fmt.Errorf("extent tree not yet implemented")
	}

	return nil, 0, ErrExtentNotFound
}

// LookupPhysical returns the physical block number for a logical block.
// Returns 0 if the block is a hole (not allocated).
func (m *Manager) LookupPhysical(ino *inode.Inode, logicalBlock uint64) (uint64, error) {
	ext, offset, err := m.Lookup(ino, logicalBlock)
	if err == ErrExtentNotFound {
		return 0, nil // Hole - return 0
	}
	if err != nil {
		return 0, err
	}

	// Unwritten extents return zeros like holes for read operations.
	// The extent exists (space is allocated) but data hasn't been written yet.
	if ext.Flags&FlagUnwritten != 0 {
		return 0, nil // Lazy-zero: treat as hole for reads
	}

	return ext.PhysicalBlock + offset, nil
}

// Insert adds a new extent to the inode.
// For Phase 2, only supports inline extents (max 2).
func (m *Manager) Insert(ctx context.Context, ino *inode.Inode, ext *Extent) error {
	// Validate extent
	if ext.Length == 0 {
		return ErrInvalidExtent
	}

	// Check for overlaps with existing extents
	for i, inline := range ino.InlineExtents {
		if inline.Length == 0 {
			continue
		}

		// Check if ranges overlap
		if extentsOverlap(ext, &Extent{
			LogicalBlock: inline.LogicalBlock,
			Length:       inline.Length,
		}) {
			// Allow merge if adjacent and same physical region
			if canMerge(ext, &Extent{
				LogicalBlock:  inline.LogicalBlock,
				PhysicalBlock: inline.PhysicalBlock,
				Length:        inline.Length,
				Flags:         inline.Flags,
			}) {
				// Merge the extents
				merged := mergeExtents(ext, &Extent{
					LogicalBlock:  inline.LogicalBlock,
					PhysicalBlock: inline.PhysicalBlock,
					Length:        inline.Length,
					Flags:         inline.Flags,
				})
				ino.InlineExtents[i] = inode.InlineExtent{
					LogicalBlock:  merged.LogicalBlock,
					PhysicalBlock: merged.PhysicalBlock,
					Length:        merged.Length,
					Flags:         merged.Flags,
				}
				return nil
			}
			return ErrExtentOverlap
		}
	}

	// Find empty slot for new extent
	for i, inline := range ino.InlineExtents {
		if inline.Length == 0 {
			ino.InlineExtents[i] = inode.InlineExtent{
				LogicalBlock:  ext.LogicalBlock,
				PhysicalBlock: ext.PhysicalBlock,
				Length:        ext.Length,
				Flags:         ext.Flags,
			}
			ino.ExtentCount++
			return nil
		}
	}

	// TODO: Create extent tree if no inline slots available
	if !ino.HasExtentTree() {
		return ErrNoSpaceForExtent
	}

	return fmt.Errorf("extent tree insertion not yet implemented")
}

// AllocateBlocks allocates physical blocks for a logical range.
// Returns the extent representing the allocated blocks.
func (m *Manager) AllocateBlocks(ctx context.Context, ino *inode.Inode, logicalBlock, count uint64) (*Extent, error) {
	// Check if already allocated
	existing, _, err := m.Lookup(ino, logicalBlock)
	if err == nil && existing != nil {
		// Already has an extent - check if it covers the full range
		if logicalBlock >= existing.LogicalBlock &&
			logicalBlock+count <= existing.LogicalBlock+existing.Length {
			return existing, nil
		}
		// Partial coverage - need to extend or add new extent
		// For now, return what we have
		return existing, nil
	}

	// Allocate physical blocks
	blocks, err := m.bitmap.Alloc(ctx, int(count))
	if err != nil {
		return nil, fmt.Errorf("failed to allocate blocks: %w", err)
	}

	// Check if blocks are contiguous
	if !areContiguous(blocks) {
		// For simplicity in Phase 2, require contiguous allocation
		// Free the non-contiguous blocks and try again with smaller count
		for _, b := range blocks {
			m.bitmap.Free(ctx, b)
		}
		return nil, fmt.Errorf("non-contiguous allocation not yet supported")
	}

	// Create extent
	ext := &Extent{
		LogicalBlock:  logicalBlock,
		PhysicalBlock: blocks[0],
		Length:        uint64(len(blocks)),
		Flags:         FlagAllocated,
	}

	// Insert into inode
	if err := m.Insert(ctx, ino, ext); err != nil {
		// Rollback allocation
		for _, b := range blocks {
			m.bitmap.Free(ctx, b)
		}
		return nil, err
	}

	// Update inode stats
	ino.AllocatedBlocks += uint64(len(blocks))
	// Blocks allocated via AllocateBlocks are for immediate write, so also count as written.
	// Unwritten extents (from fallocate) would use FlagUnwritten and not increment WrittenBlocks.
	ino.WrittenBlocks += uint64(len(blocks))

	return ext, nil
}

// AllocateUnwritten allocates physical blocks marked as unwritten (lazy-zero).
// Unlike AllocateBlocks, this does NOT increment WrittenBlocks since no data is written.
// Reads from unwritten blocks return zeros without disk I/O.
// The first write to an unwritten block clears the Unwritten flag.
func (m *Manager) AllocateUnwritten(ctx context.Context, ino *inode.Inode, logicalBlock, count uint64) (*Extent, error) {
	// Check if already allocated
	existing, _, err := m.Lookup(ino, logicalBlock)
	if err == nil && existing != nil {
		// Already has extent - return it (no-op for already allocated blocks)
		return existing, nil
	}

	// Allocate physical blocks
	blocks, err := m.bitmap.Alloc(ctx, int(count))
	if err != nil {
		return nil, fmt.Errorf("failed to allocate blocks: %w", err)
	}

	// Check if blocks are contiguous
	if !areContiguous(blocks) {
		for _, b := range blocks {
			m.bitmap.Free(ctx, b)
		}
		return nil, fmt.Errorf("non-contiguous allocation not yet supported")
	}

	// Create extent with BOTH FlagAllocated and FlagUnwritten
	ext := &Extent{
		LogicalBlock:  logicalBlock,
		PhysicalBlock: blocks[0],
		Length:        uint64(len(blocks)),
		Flags:         FlagAllocated | FlagUnwritten,
	}

	// Insert into inode
	if err := m.Insert(ctx, ino, ext); err != nil {
		for _, b := range blocks {
			m.bitmap.Free(ctx, b)
		}
		return nil, err
	}

	// Update inode stats - only AllocatedBlocks, NOT WrittenBlocks
	// Unwritten extents don't count toward written blocks until first write
	ino.AllocatedBlocks += uint64(len(blocks))

	return ext, nil
}

// ClearUnwritten clears the Unwritten flag for an extent and increments WrittenBlocks.
// Called on first write to an unwritten block.
// Returns true if the flag was cleared (first write), false if already written.
func (m *Manager) ClearUnwritten(ino *inode.Inode, ext *Extent) bool {
	if ext.Flags&FlagUnwritten == 0 {
		return false // Already written, nothing to do
	}

	// Clear the flag
	ext.Flags &^= FlagUnwritten

	// Update the inline extent in inode
	for i, inline := range ino.InlineExtents {
		if inline.LogicalBlock == ext.LogicalBlock && inline.Length == ext.Length {
			ino.InlineExtents[i].Flags = ext.Flags
			break
		}
	}

	// Increment WrittenBlocks
	ino.WrittenBlocks += ext.Length

	return true
}

// FreeExtent frees the physical blocks for an extent.
func (m *Manager) FreeExtent(ctx context.Context, ino *inode.Inode, logicalBlock uint64) error {
	// Find the extent
	ext, _, err := m.Lookup(ino, logicalBlock)
	if err != nil {
		return err
	}

	// Free all blocks in the extent
	for i := uint64(0); i < ext.Length; i++ {
		if err := m.bitmap.Free(ctx, ext.PhysicalBlock+i); err != nil {
			// Continue freeing other blocks even if one fails
			continue
		}
	}

	// Remove extent from inode
	for i, inline := range ino.InlineExtents {
		if inline.LogicalBlock == ext.LogicalBlock && inline.Length == ext.Length {
			ino.InlineExtents[i] = inode.InlineExtent{} // Clear
			ino.ExtentCount--
			break
		}
	}

	// Update inode stats
	if ino.AllocatedBlocks >= ext.Length {
		ino.AllocatedBlocks -= ext.Length
	}
	// Decrement WrittenBlocks for actually written extents (not unwritten)
	if ext.Flags&FlagUnwritten == 0 && ino.WrittenBlocks >= ext.Length {
		ino.WrittenBlocks -= ext.Length
	}

	return nil
}

// FreePhysicalBlock frees a single physical block back to the bitmap.
// This is used when a block was allocated but not used (e.g., due to race conditions).
// It does NOT modify any inode - caller is responsible for inode consistency.
func (m *Manager) FreePhysicalBlock(ctx context.Context, physBlock uint64) error {
	return m.bitmap.Free(ctx, physBlock)
}

// Truncate removes all extents beyond the given size.
func (m *Manager) Truncate(ctx context.Context, ino *inode.Inode, newSize uint64) error {
	log.Printf("[DEBUG] extent.Truncate: inode %d, newSize=%d, extentCount=%d", ino.InodeNum, newSize, ino.ExtentCount)
	// Special case: truncate to 0 frees everything
	if newSize == 0 {
		for i := range ino.InlineExtents {
			ext := &ino.InlineExtents[i]
			if ext.Length == 0 {
				continue
			}

			log.Printf("[DEBUG] extent.Truncate: freeing extent[%d] physBlock=%d len=%d", i, ext.PhysicalBlock, ext.Length)
			// Free all blocks
			for j := uint64(0); j < ext.Length; j++ {
				if err := m.bitmap.Free(ctx, ext.PhysicalBlock+j); err != nil {
					log.Printf("[DEBUG] extent.Truncate: bitmap.Free(%d) failed: %v", ext.PhysicalBlock+j, err)
				}
			}
			ino.AllocatedBlocks -= ext.Length
			// Decrement WrittenBlocks for actually written extents (not unwritten)
			if ext.Flags&FlagUnwritten == 0 && ino.WrittenBlocks >= ext.Length {
				ino.WrittenBlocks -= ext.Length
			}
			*ext = inode.InlineExtent{} // Clear
			ino.ExtentCount--
		}
		ino.Size = 0
		return nil
	}

	// Calculate the last block to keep
	lastBlock := (newSize - 1) / BlockSize

	// Process inline extents
	for i := range ino.InlineExtents {
		ext := &ino.InlineExtents[i]
		if ext.Length == 0 {
			continue
		}

		extEnd := ext.LogicalBlock + ext.Length - 1

		if ext.LogicalBlock > lastBlock {
			// Entire extent is beyond truncation point - free all blocks
			for j := uint64(0); j < ext.Length; j++ {
				m.bitmap.Free(ctx, ext.PhysicalBlock+j)
			}
			ino.AllocatedBlocks -= ext.Length
			// Decrement WrittenBlocks for actually written extents (not unwritten)
			if ext.Flags&FlagUnwritten == 0 && ino.WrittenBlocks >= ext.Length {
				ino.WrittenBlocks -= ext.Length
			}
			*ext = inode.InlineExtent{} // Clear
			ino.ExtentCount--
		} else if extEnd > lastBlock {
			// Partial truncation - free blocks beyond lastBlock
			keepBlocks := lastBlock - ext.LogicalBlock + 1
			freeBlocks := ext.Length - keepBlocks

			for j := uint64(0); j < freeBlocks; j++ {
				m.bitmap.Free(ctx, ext.PhysicalBlock+keepBlocks+j)
			}
			ino.AllocatedBlocks -= freeBlocks
			// Decrement WrittenBlocks for freed portion if extent was written
			if ext.Flags&FlagUnwritten == 0 && ino.WrittenBlocks >= freeBlocks {
				ino.WrittenBlocks -= freeBlocks
			}
			ext.Length = keepBlocks
		}
	}

	// Update inode size
	ino.Size = newSize

	return nil
}

// MaxAllocatedOffset returns the highest byte offset covered by allocated extents.
// This is used to derive file size from extents, which eliminates race conditions
// when multiple nodes write to the same file at different offsets - each node's
// write creates/extends different extents, and the true size is always the max.
func (m *Manager) MaxAllocatedOffset(ino *inode.Inode) uint64 {
	max := uint64(0)
	for _, ext := range ino.InlineExtents {
		if ext.Length == 0 {
			continue
		}
		// Calculate the end byte offset of this extent
		end := (ext.LogicalBlock + ext.Length) * BlockSize
		if end > max {
			max = end
		}
	}
	return max
}

// ListExtents returns all extents for an inode.
func (m *Manager) ListExtents(ino *inode.Inode) []*Extent {
	var extents []*Extent

	for _, inline := range ino.InlineExtents {
		if inline.Length == 0 {
			continue
		}
		extents = append(extents, &Extent{
			LogicalBlock:  inline.LogicalBlock,
			PhysicalBlock: inline.PhysicalBlock,
			Length:        inline.Length,
			Flags:         inline.Flags,
		})
	}

	// Sort by logical block
	sort.Slice(extents, func(i, j int) bool {
		return extents[i].LogicalBlock < extents[j].LogicalBlock
	})

	return extents
}

// extentsOverlap checks if two extents overlap.
func extentsOverlap(a, b *Extent) bool {
	aEnd := a.LogicalBlock + a.Length
	bEnd := b.LogicalBlock + b.Length
	return a.LogicalBlock < bEnd && aEnd > b.LogicalBlock
}

// canMerge checks if two extents can be merged.
func canMerge(a, b *Extent) bool {
	// Must be adjacent (logically)
	if a.LogicalBlock+a.Length != b.LogicalBlock &&
		b.LogicalBlock+b.Length != a.LogicalBlock {
		return false
	}

	// Must be adjacent (physically)
	if a.LogicalBlock < b.LogicalBlock {
		// a comes first
		return a.PhysicalBlock+a.Length == b.PhysicalBlock
	}
	// b comes first
	return b.PhysicalBlock+b.Length == a.PhysicalBlock
}

// mergeExtents merges two adjacent extents.
func mergeExtents(a, b *Extent) *Extent {
	if a.LogicalBlock < b.LogicalBlock {
		return &Extent{
			LogicalBlock:  a.LogicalBlock,
			PhysicalBlock: a.PhysicalBlock,
			Length:        a.Length + b.Length,
			Flags:         a.Flags, // Keep first extent's flags
		}
	}
	return &Extent{
		LogicalBlock:  b.LogicalBlock,
		PhysicalBlock: b.PhysicalBlock,
		Length:        a.Length + b.Length,
		Flags:         b.Flags,
	}
}

// areContiguous checks if block numbers are contiguous.
func areContiguous(blocks []uint64) bool {
	if len(blocks) <= 1 {
		return true
	}
	for i := 1; i < len(blocks); i++ {
		if blocks[i] != blocks[i-1]+1 {
			return false
		}
	}
	return true
}

// SerializeExtent converts an Extent to bytes.
func SerializeExtent(ext *Extent) []byte {
	buf := make([]byte, ExtentSize)

	binary.LittleEndian.PutUint64(buf[0:8], ext.LogicalBlock)
	binary.LittleEndian.PutUint64(buf[8:16], ext.PhysicalBlock)
	binary.LittleEndian.PutUint64(buf[16:24], ext.Length)
	binary.LittleEndian.PutUint32(buf[24:28], ext.Flags)
	// Bytes 28-31 reserved

	return buf
}

// DeserializeExtent parses an Extent from bytes.
func DeserializeExtent(buf []byte) (*Extent, error) {
	if len(buf) < ExtentSize {
		return nil, ErrInvalidExtent
	}

	return &Extent{
		LogicalBlock:  binary.LittleEndian.Uint64(buf[0:8]),
		PhysicalBlock: binary.LittleEndian.Uint64(buf[8:16]),
		Length:        binary.LittleEndian.Uint64(buf[16:24]),
		Flags:         binary.LittleEndian.Uint32(buf[24:28]),
	}, nil
}

// IsAllocated returns true if the extent has allocated blocks.
func (e *Extent) IsAllocated() bool {
	return (e.Flags & FlagAllocated) != 0
}

// IsUnwritten returns true if the extent is allocated but not yet written.
func (e *Extent) IsUnwritten() bool {
	return (e.Flags & FlagUnwritten) != 0
}

// IsShared returns true if the extent is shared with snapshots.
func (e *Extent) IsShared() bool {
	return (e.Flags & FlagShared) != 0
}

// IsSFB returns true if the extent is stored in an SFB container.
func (e *Extent) IsSFB() bool {
	return (e.Flags & FlagSFB) != 0
}

// ContainerBlock extracts the SFB container block number from PhysicalBlock.
// Only valid when FlagSFB is set. High 48 bits contain the container block.
func (e *Extent) ContainerBlock() uint64 {
	return e.PhysicalBlock >> 16
}

// SlotIndex extracts the SFB slot index from PhysicalBlock.
// Only valid when FlagSFB is set. Low 16 bits contain the slot index.
func (e *Extent) SlotIndex() uint16 {
	return uint16(e.PhysicalBlock & 0xFFFF)
}

// SetSFBLocation packs container block and slot index into PhysicalBlock
// and sets the FlagSFB flag.
func (e *Extent) SetSFBLocation(containerBlock uint64, slotIndex uint16) {
	e.PhysicalBlock = (containerBlock << 16) | uint64(slotIndex)
	e.Flags |= FlagSFB | FlagAllocated
}

// BlocksForSize calculates the number of blocks needed for a given byte size.
func BlocksForSize(size uint64) uint64 {
	if size == 0 {
		return 0
	}
	return (size + BlockSize - 1) / BlockSize
}

// OffsetToBlock converts a byte offset to a block number.
func OffsetToBlock(offset uint64) uint64 {
	return offset / BlockSize
}

// BlockToOffset converts a block number to the starting byte offset.
func BlockToOffset(block uint64) uint64 {
	return block * BlockSize
}

// AddExtentForRecovery adds an extent during recovery.
// This is similar to Insert but designed for idempotent recovery.
// It uses Insert internally but handles the case where the extent
// may already exist due to concurrent recovery.
func (m *Manager) AddExtentForRecovery(ctx context.Context, ino *inode.Inode, ext *Extent) error {
	return m.Insert(ctx, ino, ext)
}

// RemoveExtentForRecovery removes extents during recovery.
// This removes extents starting at fileBlock for the given length.
func (m *Manager) RemoveExtentForRecovery(ctx context.Context, ino *inode.Inode, fileBlock, length uint32) error {
	// Find and remove extents that overlap with the specified range
	for i := range ino.InlineExtents {
		ext := &ino.InlineExtents[i]
		if ext.Length == 0 {
			continue
		}

		// Check if this extent overlaps with the range to remove
		rangeEnd := uint64(fileBlock) + uint64(length)
		extEnd := ext.LogicalBlock + ext.Length

		if ext.LogicalBlock >= uint64(fileBlock) && ext.LogicalBlock < rangeEnd {
			// This extent starts within the removal range
			// Free all blocks in this extent
			for j := uint64(0); j < ext.Length; j++ {
				if err := m.bitmap.Free(ctx, ext.PhysicalBlock+j); err != nil {
					log.Printf("[Recovery] Failed to free block %d: %v", ext.PhysicalBlock+j, err)
				}
			}
			if ino.AllocatedBlocks >= ext.Length {
				ino.AllocatedBlocks -= ext.Length
			}
			*ext = inode.InlineExtent{} // Clear
			if ino.ExtentCount > 0 {
				ino.ExtentCount--
			}
		} else if uint64(fileBlock) > ext.LogicalBlock && uint64(fileBlock) < extEnd {
			// Removal starts in the middle of this extent - partial removal
			keepBlocks := uint64(fileBlock) - ext.LogicalBlock
			freeBlocks := ext.Length - keepBlocks

			for j := uint64(0); j < freeBlocks; j++ {
				if err := m.bitmap.Free(ctx, ext.PhysicalBlock+keepBlocks+j); err != nil {
					log.Printf("[Recovery] Failed to free block %d: %v", ext.PhysicalBlock+keepBlocks+j, err)
				}
			}
			if ino.AllocatedBlocks >= freeBlocks {
				ino.AllocatedBlocks -= freeBlocks
			}
			ext.Length = keepBlocks
		}
	}

	return nil
}

// NeedsCoW checks if a write to the given extent requires copy-on-write.
// Returns true if the block is shared (refcount > 1) and must be copied.
func (m *Manager) NeedsCoW(ctx context.Context, ext *Extent, refcountReg *refcount.Region) (bool, error) {
	if refcountReg == nil {
		return false, nil
	}
	if ext.Flags&FlagShared == 0 {
		return false, nil
	}
	entry, err := refcountReg.Get(ctx, ext.PhysicalBlock)
	if err != nil {
		return false, fmt.Errorf("refcount read failed for block %d: %w", ext.PhysicalBlock, err)
	}
	return entry.NeedsCoW(), nil
}

// CopyOnWrite performs copy-on-write for a shared block.
// Allocates a new block, copies data, and updates the extent.
// The caller must decrement the old block's refcount after this returns.
func (m *Manager) CopyOnWrite(ctx context.Context, ino *inode.Inode, ext *Extent,
	logicalBlock uint64, dataOffset uint64, refcountReg *refcount.Region) (*CoWResult, error) {

	offsetInExtent := logicalBlock - ext.LogicalBlock
	result := &CoWResult{
		OldPhysBlock: ext.PhysicalBlock + offsetInExtent,
		WasShared:    false,
	}

	needsCoW, err := m.NeedsCoW(ctx, ext, refcountReg)
	if err != nil {
		return nil, err
	}
	if !needsCoW {
		result.NewPhysBlock = result.OldPhysBlock
		return result, nil
	}

	// Only support single-block extents for now
	if ext.Length != 1 {
		return nil, ErrCoWMultiBlock
	}

	log.Printf("[CoW] Block %d is shared, performing copy-on-write", result.OldPhysBlock)

	// Allocate new block
	newBlocks, err := m.bitmap.Alloc(ctx, 1)
	if err != nil {
		return nil, fmt.Errorf("CoW alloc failed: %w", err)
	}
	newPhysBlock := newBlocks[0]

	// Copy data from old to new block
	buf := storage.GetBuffer(BlockSize)
	defer storage.PutBuffer(buf)

	oldOffset := int64(dataOffset + result.OldPhysBlock*BlockSize)
	newOffset := int64(dataOffset + newPhysBlock*BlockSize)

	if _, err := m.dev.ReadAt(buf.Bytes(), oldOffset); err != nil {
		m.bitmap.Free(ctx, newPhysBlock)
		return nil, fmt.Errorf("CoW read failed: %w", err)
	}
	if _, err := m.dev.WriteAt(buf.Bytes(), newOffset); err != nil {
		m.bitmap.Free(ctx, newPhysBlock)
		return nil, fmt.Errorf("CoW write failed: %w", err)
	}

	// Update extent to point to new block
	ext.PhysicalBlock = newPhysBlock
	ext.Flags &^= FlagShared

	// Update inline extent in inode
	for i := range ino.InlineExtents {
		inline := &ino.InlineExtents[i]
		if inline.LogicalBlock == ext.LogicalBlock {
			inline.PhysicalBlock = newPhysBlock
			inline.Flags &^= FlagShared
			break
		}
	}

	result.NewPhysBlock = newPhysBlock
	result.WasShared = true
	log.Printf("[CoW] Copied block %d -> %d", result.OldPhysBlock, newPhysBlock)
	return result, nil
}

// MarkAllShared sets FlagShared on all extents of an inode.
func (m *Manager) MarkAllShared(ino *inode.Inode) {
	for i := range ino.InlineExtents {
		if ino.InlineExtents[i].Length > 0 {
			ino.InlineExtents[i].Flags |= FlagShared
		}
	}
}
