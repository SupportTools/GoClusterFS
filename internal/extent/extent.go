// Package extent implements extent-based block mapping for GoClusterFS.
//
// Files store their block mappings as extents - contiguous ranges of blocks.
// Each extent maps a logical file offset to a physical disk location.
//
// Storage modes:
//   - Inline: Up to 2 extents stored directly in the inode (64 bytes)
//   - Tree: More extents stored in an overflow tree of 1MB nodes (see tree.go)
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
	"math"
	"sync"

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
	// FlagCounted marks sharing that refcounts track. Older versions set
	// FlagShared alone and never counted the owners.
	FlagCounted = 1 << 6
)

// Hole is what LookupPhysical returns for a logical block with no data on disk.
// Physical block 0 is a valid data block.
const Hole = math.MaxUint64

// Errors
var (
	ErrExtentNotFound    = errors.New("extent not found")
	ErrExtentOverlap     = errors.New("extent overlap detected")
	ErrInvalidExtent     = errors.New("invalid extent")
	ErrBlockNotAllocated = errors.New("block not allocated")
	ErrCoWRequired       = errors.New("copy-on-write required for shared block")
)

// CoWResult contains the result of a copy-on-write operation.
type CoWResult struct {
	OldPhysBlock uint64 // Original physical block (for refcount decrement)
	NewPhysBlock uint64 // New physical block after CoW
	WasShared    bool   // True if CoW was actually performed
	// Uncounted means an older version shared the old block without counting
	// its owners, so it must never be released.
	Uncounted bool
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
	dev        *storage.Device
	bitmap     *bitmap.Region
	lockMgr    *lock.Manager
	dataOffset uint64
	nodes      *nodeCache
	leafCap    int // extents per leaf before it splits
	indexCap   int // leaves per tree

	refs *refcount.Region // nil when snapshots are disabled

	deferredMu sync.Mutex
	deferred   []uint64 // frees postponed by lock order
}

// SetRefcounts makes frees respect block sharing: a block shared by a
// snapshot or clone goes back to the bitmap only after its last owner frees it.
func (m *Manager) SetRefcounts(refs *refcount.Region) {
	m.refs = refs
}

// NewManager creates a new extent manager. dataOffset is the device offset of
// data block 0; extent tree nodes are stored in data blocks.
func NewManager(dev *storage.Device, bm *bitmap.Region, lockMgr *lock.Manager, dataOffset uint64) *Manager {
	return &Manager{
		dev:        dev,
		bitmap:     bm,
		lockMgr:    lockMgr,
		dataOffset: dataOffset,
		nodes:      newNodeCache(nodeCacheEntries),
		leafCap:    defaultLeafCap,
		indexCap:   MaxNodeEntries,
	}
}

// Lookup finds the extent containing the given logical block.
// Returns the extent and the offset within that extent, or ErrExtentNotFound.
func (m *Manager) Lookup(ino *inode.Inode, logicalBlock uint64) (*Extent, uint64, error) {
	exts, err := m.lookupLeaf(ino, logicalBlock)
	if err != nil {
		return nil, 0, err
	}
	if exts == nil {
		for _, inline := range ino.InlineExtents {
			if inline.Length > 0 && logicalBlock >= inline.LogicalBlock && logicalBlock < inline.LogicalBlock+inline.Length {
				ext := &Extent{inline.LogicalBlock, inline.PhysicalBlock, inline.Length, inline.Flags}
				return ext, logicalBlock - ext.LogicalBlock, nil
			}
		}
		return nil, 0, ErrExtentNotFound
	}

	i, ok := find(exts, logicalBlock)
	if !ok {
		return nil, 0, ErrExtentNotFound
	}
	ext := exts[i]
	return &ext, logicalBlock - ext.LogicalBlock, nil
}

// LookupPhysical returns the physical block number for a logical block, or Hole
// if the block is not allocated.
func (m *Manager) LookupPhysical(ino *inode.Inode, logicalBlock uint64) (uint64, error) {
	ext, offset, err := m.Lookup(ino, logicalBlock)
	if err == ErrExtentNotFound {
		return Hole, nil
	}
	if err != nil {
		return 0, err
	}

	// Unwritten extents return zeros like holes for read operations.
	// The extent exists (space is allocated) but data hasn't been written yet.
	if ext.Flags&FlagUnwritten != 0 {
		return Hole, nil
	}

	return ext.PhysicalBlock + offset, nil
}

// Insert adds a new extent to the inode, merging it with adjacent extents.
// The extent map moves into an overflow tree when it outgrows the inode.
// Changes reach disk when the inode is written.
func (m *Manager) Insert(ctx context.Context, ino *inode.Inode, ext *Extent) error {
	if ext.Length == 0 {
		return ErrInvalidExtent
	}
	e, err := m.edit(ctx, ino)
	if err != nil {
		return err
	}
	if err := e.insert(*ext); err != nil {
		return err
	}
	return e.commit()
}

// AllocateBlocks allocates physical blocks for a logical range.
// Returns the extent representing the first contiguous run of allocated blocks.
func (m *Manager) AllocateBlocks(ctx context.Context, ino *inode.Inode, logicalBlock, count uint64) (*Extent, error) {
	return m.allocate(ctx, ino, logicalBlock, count, FlagAllocated)
}

// AllocateUnwritten allocates physical blocks marked as unwritten (lazy-zero).
// Unlike AllocateBlocks, this does NOT increment WrittenBlocks since no data is written.
// Reads from unwritten blocks return zeros without disk I/O.
// The first write to an unwritten block clears the Unwritten flag.
func (m *Manager) AllocateUnwritten(ctx context.Context, ino *inode.Inode, logicalBlock, count uint64) (*Extent, error) {
	return m.allocate(ctx, ino, logicalBlock, count, FlagAllocated|FlagUnwritten)
}

func (m *Manager) allocate(ctx context.Context, ino *inode.Inode, logicalBlock, count uint64, flags uint32) (*Extent, error) {
	existing, _, err := m.Lookup(ino, logicalBlock)
	if err == nil {
		return existing, nil
	}
	if err != ErrExtentNotFound {
		return nil, err
	}

	blocks, err := m.bitmap.Alloc(ctx, int(count))
	if err != nil {
		return nil, fmt.Errorf("failed to allocate blocks: %w", err)
	}

	runs := contiguousRuns(logicalBlock, blocks, flags)
	e, err := m.edit(ctx, ino)
	for i := 0; err == nil && i < len(runs); i++ {
		err = e.insert(runs[i])
	}
	if err == nil {
		err = e.commit()
	}
	if err != nil {
		for _, b := range blocks {
			m.bitmap.Free(ctx, b)
		}
		return nil, err
	}

	ino.AllocatedBlocks += count
	if flags&FlagUnwritten == 0 {
		ino.WrittenBlocks += count
	}
	return &runs[0], nil
}

// contiguousRuns maps blocks onto consecutive logical blocks starting at
// logicalBlock, one extent per physically contiguous run.
func contiguousRuns(logicalBlock uint64, blocks []uint64, flags uint32) []Extent {
	var runs []Extent
	for i, b := range blocks {
		if n := len(runs); n > 0 && runs[n-1].PhysicalBlock+runs[n-1].Length == b {
			runs[n-1].Length++
			continue
		}
		runs = append(runs, Extent{logicalBlock + uint64(i), b, 1, flags})
	}
	return runs
}

// ClearUnwritten marks one block of an unwritten extent as written, splitting
// the extent so its other blocks still read as zeros. Called on first write to
// the block. Returns true if the block was unwritten, false if already written.
func (m *Manager) ClearUnwritten(ctx context.Context, ino *inode.Inode, logicalBlock uint64) (bool, error) {
	ext, _, err := m.Lookup(ino, logicalBlock)
	if err != nil {
		return false, err
	}
	if ext.Flags&FlagUnwritten == 0 {
		return false, nil
	}
	e, err := m.edit(ctx, ino)
	if err != nil {
		return false, err
	}
	removed, err := e.removeRange(logicalBlock, logicalBlock+1)
	if err != nil {
		return false, err
	}
	block := removed[0]
	block.Flags &^= FlagUnwritten
	if err := e.insert(block); err != nil {
		return false, err
	}
	if err := e.commit(); err != nil {
		return false, err
	}
	ino.WrittenBlocks++
	return true, nil
}

// FreeExtent frees the physical blocks for the extent containing logicalBlock.
func (m *Manager) FreeExtent(ctx context.Context, ino *inode.Inode, logicalBlock uint64) error {
	ext, _, err := m.Lookup(ino, logicalBlock)
	if err != nil {
		return err
	}
	return m.unmap(ctx, ino, ext.LogicalBlock, ext.LogicalBlock+ext.Length)
}

// AllocatePhysicalBlock allocates one block without mapping it into any inode.
func (m *Manager) AllocatePhysicalBlock(ctx context.Context) (uint64, error) {
	blocks, err := m.bitmap.Alloc(ctx, 1)
	if err != nil {
		return 0, err
	}
	return blocks[0], nil
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
	if err := m.unmap(ctx, ino, BlocksForSize(newSize), math.MaxUint64); err != nil {
		return err
	}
	ino.Size = newSize
	return nil
}

// unmap removes logical blocks [from, to) from the inode and releases them.
func (m *Manager) unmap(ctx context.Context, ino *inode.Inode, from, to uint64) error {
	e, err := m.edit(ctx, ino)
	if err != nil {
		return err
	}
	removed, err := e.removeRange(from, to)
	if err != nil {
		return err
	}
	if err := e.commit(); err != nil {
		return err
	}
	m.release(ctx, ino, removed)
	return nil
}

// release updates block accounting for removed extents and queues their
// blocks to be freed when the inode write commits: if that write loses a
// conflict, the inode on disk still maps them. SFB extents point into
// containers rather than at bitmap blocks.
func (m *Manager) release(ctx context.Context, ino *inode.Inode, removed []Extent) {
	var blocks []uint64
	for _, x := range removed {
		ino.AllocatedBlocks -= min(ino.AllocatedBlocks, x.Length)
		if x.Flags&FlagUnwritten == 0 {
			ino.WrittenBlocks -= min(ino.WrittenBlocks, x.Length)
		}
		if x.Flags&FlagSFB == 0 {
			blocks = append(blocks, m.ownedBlocks(x)...)
		}
	}
	if len(blocks) == 0 {
		return
	}
	p := ensurePending(ino)
	p.release = append(p.release, blocks...)
}

// ownedBlocks returns the blocks of x that frees may give back. Nobody knows
// how many inodes share an uncounted block, so it stays allocated.
func (m *Manager) ownedBlocks(x Extent) []uint64 {
	if x.Flags&FlagShared != 0 && (x.Flags&FlagCounted == 0 || m.refs == nil) {
		return nil
	}
	blocks := make([]uint64, x.Length)
	for j := range blocks {
		blocks[j] = x.PhysicalBlock + uint64(j)
	}
	return blocks
}

// HasUncountedSharing reports whether ino shares blocks the way older versions
// did, without refcounts. Such blocks cannot be shared again safely.
func (m *Manager) HasUncountedSharing(ino *inode.Inode) (bool, error) {
	uncounted := false
	err := m.forEach(ino, func(x Extent) {
		if x.Flags&FlagShared != 0 && x.Flags&FlagCounted == 0 {
			uncounted = true
		}
	})
	return uncounted, err
}

// MarkSharingCounted adds FlagCounted to the extents of ino an older version
// shared. Call it only once the refcounts of their blocks count every owner.
func (m *Manager) MarkSharingCounted(ino *inode.Inode) (bool, error) {
	uncounted, err := m.HasUncountedSharing(ino)
	if err != nil || !uncounted {
		return false, err
	}
	e, err := m.edit(context.Background(), ino)
	if err != nil {
		return false, err
	}
	if err := e.updateAll(func(x *Extent) {
		if x.Flags&FlagShared != 0 {
			x.Flags |= FlagCounted
		}
	}); err != nil {
		return false, err
	}
	return true, e.commit()
}

// Delete unmaps every block of an inode that is being freed and frees the
// blocks now, since no inode write will follow to commit the change.
func (m *Manager) Delete(ctx context.Context, ino *inode.Inode) error {
	if err := m.unmap(ctx, ino, 0, math.MaxUint64); err != nil {
		return err
	}
	if p := pendingOf(ino); p != nil {
		m.freeBlocks(ctx, p.release)
		m.freeBlocks(ctx, p.fresh)
		ino.PendingTree = nil
	}
	ino.Size = 0
	return nil
}

// ListExtents returns all extents for an inode, sorted by logical block.
func (m *Manager) ListExtents(ino *inode.Inode) ([]*Extent, error) {
	var extents []*Extent
	err := m.forEach(ino, func(x Extent) {
		extents = append(extents, &x)
	})
	return extents, err
}

// DataBlocks returns every physical data block mapped by the inode, in logical
// order. SFB extents are skipped because they point into shared containers.
func (m *Manager) DataBlocks(ino *inode.Inode) ([]uint64, error) {
	var blocks []uint64
	err := m.forEach(ino, func(x Extent) {
		if x.Flags&FlagSFB != 0 {
			return
		}
		for j := uint64(0); j < x.Length; j++ {
			blocks = append(blocks, x.PhysicalBlock+j)
		}
	})
	return blocks, err
}

func (m *Manager) forEach(ino *inode.Inode, fn func(Extent)) error {
	leaves, err := m.layout(ino)
	if err != nil {
		return err
	}
	if len(leaves) == 0 {
		for _, x := range inlineExtents(ino) {
			fn(x)
		}
		return nil
	}
	for _, l := range leaves {
		exts, err := m.leafExtents(ino.InodeNum, l)
		if err != nil {
			return err
		}
		for _, x := range exts {
			fn(x)
		}
	}
	return nil
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
// Callers check with Lookup first; an overlap means another recovery won.
func (m *Manager) AddExtentForRecovery(ctx context.Context, ino *inode.Inode, ext *Extent) error {
	return m.Insert(ctx, ino, ext)
}

// RemoveExtentForRecovery unmaps logical blocks [fileBlock, fileBlock+length)
// during recovery and frees them.
func (m *Manager) RemoveExtentForRecovery(ctx context.Context, ino *inode.Inode, fileBlock, length uint32) error {
	return m.unmap(ctx, ino, uint64(fileBlock), uint64(fileBlock)+uint64(length))
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

// MapCopy allocates a block for a copy of logical block logicalBlock of ino
// and maps the block there alone, without copying the data. The caller copies
// it with CopyBlockData before the inode write commits the mapping.
func (m *Manager) MapCopy(ctx context.Context, ino *inode.Inode, logicalBlock uint64) (*CoWResult, error) {
	cur, offset, err := m.Lookup(ino, logicalBlock)
	if err != nil {
		return nil, err
	}
	newBlocks, err := m.bitmap.Alloc(ctx, 1)
	if err != nil {
		return nil, fmt.Errorf("CoW alloc failed: %w", err)
	}
	private := Extent{logicalBlock, newBlocks[0], 1, cur.Flags &^ (FlagShared | FlagCounted)}
	if err := m.remap(ctx, ino, private); err != nil {
		m.bitmap.Free(ctx, newBlocks[0])
		return nil, err
	}
	// The caller releases the old block after the inode write, so that write
	// must fail if another node copied the block first.
	ensurePending(ino)
	// Readers keep cached block mappings until DataGeneration changes.
	ino.DataGeneration++
	return &CoWResult{
		OldPhysBlock: cur.PhysicalBlock + offset,
		NewPhysBlock: newBlocks[0],
		WasShared:    true,
		Uncounted:    cur.Flags&FlagShared != 0 && cur.Flags&FlagCounted == 0,
	}, nil
}

// CopyBlockData copies the old block of a MapCopy into the new one. Unless the
// file is the caller's alone, the caller holds the inode lock exclusively, so
// no in-place write lands in the old block during the copy.
func (m *Manager) CopyBlockData(c *CoWResult, dataOffset uint64) error {
	buf := storage.GetBuffer(BlockSize)
	defer storage.PutBuffer(buf)
	if _, err := m.dev.ReadAt(buf.Bytes(), int64(dataOffset+c.OldPhysBlock*BlockSize)); err != nil {
		return fmt.Errorf("CoW read failed: %w", err)
	}
	if _, err := m.dev.WriteAt(buf.Bytes(), int64(dataOffset+c.NewPhysBlock*BlockSize)); err != nil {
		return fmt.Errorf("CoW write failed: %w", err)
	}
	log.Printf("[CoW] Copied block %d -> %d", c.OldPhysBlock, c.NewPhysBlock)
	return nil
}

// remap points the logical blocks covered by x at x's physical blocks without
// freeing the blocks they mapped before.
func (m *Manager) remap(ctx context.Context, ino *inode.Inode, x Extent) error {
	e, err := m.edit(ctx, ino)
	if err != nil {
		return err
	}
	if _, err := e.removeRange(x.LogicalBlock, x.LogicalBlock+x.Length); err != nil {
		return err
	}
	if err := e.insert(x); err != nil {
		return err
	}
	return e.commit()
}

// MarkAllShared sets FlagShared on all extents of an inode.
func (m *Manager) MarkAllShared(ino *inode.Inode) error {
	e, err := m.edit(context.Background(), ino)
	if err != nil {
		return err
	}
	if err := e.updateAll(func(x *Extent) { x.Flags |= FlagShared | FlagCounted }); err != nil {
		return err
	}
	return e.commit()
}

// CopyExtentsForClone copies all extents from src to dst with FlagShared set.
// Used when creating writable clones - the clone shares blocks with the source
// until either is modified, at which point CoW allocates new blocks.
// dst must have no extents; it gets its own extent tree when src has one.
func (m *Manager) CopyExtentsForClone(ctx context.Context, src *inode.Inode, dst *inode.Inode) error {
	if dst.ExtentCount != 0 || dst.HasExtentTree() || dst.PendingTree != nil {
		return fmt.Errorf("clone target inode %d already has extents", dst.InodeNum)
	}
	all := []Extent{}
	if err := m.forEach(src, func(x Extent) {
		x.Flags |= FlagShared | FlagCounted
		all = append(all, x)
	}); err != nil {
		return err
	}
	e, err := m.edit(ctx, dst)
	if err != nil {
		return err
	}
	e.set(0, all)
	if err := e.commit(); err != nil {
		return err
	}
	dst.AllocatedBlocks = src.AllocatedBlocks
	dst.WrittenBlocks = src.WrittenBlocks
	return nil
}
