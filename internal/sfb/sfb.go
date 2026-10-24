// Package sfb implements Sub-File Block containers for small file storage.
//
// SFB containers subdivide a 1MB block into smaller slots to efficiently
// store small files without wasting an entire block per file.
//
// Tier system:
//   - Tier4KB:  256 slots of 4KB  each (files ≤4KB)
//   - Tier8KB:  128 slots of 8KB  each (files ≤8KB)
//   - Tier16KB:  64 slots of 16KB each (files ≤16KB)
//   - Tier64KB:  16 slots of 64KB each (files ≤64KB)
//
// Files exceeding 64KB migrate to full extent allocation.
package sfb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// BlockSize is the size of a full data block (1MB).
	BlockSize = 1 << 20

	// HeaderSize is the size of the container header on disk.
	// Must be aligned for O_DIRECT I/O.
	HeaderSize = 64

	// MaxSlotSize is the maximum slot size (64KB).
	// Files larger than this migrate to full extent allocation.
	MaxSlotSize = 64 * 1024

	// MaxFileSize is an alias for MaxSlotSize - the largest file that fits in SFB.
	MaxFileSize = MaxSlotSize

	// ContainerMagic identifies a valid SFB container.
	ContainerMagic = 0x53464243 // "SFBC"

	// HeaderMagic is an alias for ContainerMagic.
	HeaderMagic = ContainerMagic

	// ContainerVersion is the current container format version.
	ContainerVersion = 1

	// HeaderVersion is an alias for ContainerVersion.
	HeaderVersion = ContainerVersion
)

// Tier identifies the slot size tier.
type Tier uint8

const (
	Tier4KB  Tier = 0 // 256 slots of 4KB
	Tier8KB  Tier = 1 // 128 slots of 8KB
	Tier16KB Tier = 2 //  64 slots of 16KB
	Tier64KB Tier = 3 //  16 slots of 64KB
)

// TierInfo contains metadata about each tier.
type TierInfo struct {
	SlotSize   uint32 // Size of each slot in bytes
	SlotCount  uint16 // Number of slots per container
	BitmapSize uint16 // Size of allocation bitmap in bytes
}

// Tiers contains information for each tier.
var Tiers = []TierInfo{
	{SlotSize: 4 * 1024, SlotCount: 256, BitmapSize: 32}, // Tier4KB
	{SlotSize: 8 * 1024, SlotCount: 128, BitmapSize: 16}, // Tier8KB
	{SlotSize: 16 * 1024, SlotCount: 64, BitmapSize: 8},  // Tier16KB
	{SlotSize: 64 * 1024, SlotCount: 16, BitmapSize: 2},  // Tier64KB
}

// String returns a human-readable representation of the tier.
func (t Tier) String() string {
	switch t {
	case Tier4KB:
		return "4KB"
	case Tier8KB:
		return "8KB"
	case Tier16KB:
		return "16KB"
	case Tier64KB:
		return "64KB"
	default:
		return "Unknown"
	}
}

// Info returns the slot size and slot count for this tier.
func (t Tier) Info() (slotSize uint32, slotCount uint16) {
	if int(t) >= len(Tiers) {
		return 0, 0
	}
	info := Tiers[t]
	return info.SlotSize, info.SlotCount
}

// TierForSize returns the smallest tier that can hold a file of the given size.
// Returns false if the size exceeds the maximum SFB slot size (64KB).
func TierForSize(size uint64) (Tier, bool) {
	switch {
	case size <= 4096:
		return Tier4KB, true
	case size <= 8192:
		return Tier8KB, true
	case size <= 16384:
		return Tier16KB, true
	case size <= 65536:
		return Tier64KB, true
	default:
		return 0, false
	}
}

// Errors
var (
	ErrNoFreeSlot       = errors.New("no free slot in container")
	ErrInvalidContainer = errors.New("invalid container header")
	ErrInvalidTier      = errors.New("invalid tier")
	ErrSlotNotAllocated = errors.New("slot not allocated")
	ErrSizeExceedsSlot  = errors.New("data size exceeds slot size")
	ErrNoContainer      = errors.New("no container with free slots")
)

// Header is the on-disk container header stored at the start of a 1MB block.
// Size: 64 bytes (fixed, regardless of tier).
type Header struct {
	Magic     uint32   // 0x53464243 "SFBC"
	Version   uint8    // Format version (currently 1)
	Tier      Tier     // Slot size tier
	SlotCount uint16   // Number of slots (256/128/64/16)
	FreeSlots uint16   // Count of available slots
	Reserved  [6]byte  // Padding for alignment
	Checksum  uint32   // CRC32C of header (excluding checksum)
	Bitmap    [32]byte // Slot allocation bitmap (max 32 bytes for 256 slots)
}

// Serialize converts the header to a byte slice for disk storage.
// The checksum is computed and included in the output.
func (h *Header) Serialize() []byte {
	buf := make([]byte, HeaderSize)

	// Pack header fields
	binary.LittleEndian.PutUint32(buf[0:4], h.Magic)
	buf[4] = h.Version
	buf[5] = byte(h.Tier)
	binary.LittleEndian.PutUint16(buf[6:8], h.SlotCount)
	binary.LittleEndian.PutUint16(buf[8:10], h.FreeSlots)
	copy(buf[10:16], h.Reserved[:])
	// Checksum at bytes 16-20 - computed below
	copy(buf[20:52], h.Bitmap[:])
	// Remaining bytes 52-64 are padding

	// Compute checksum over everything except the checksum field itself
	crc := crc32.ChecksumIEEE(buf[0:16])
	crc = crc32.Update(crc, crc32.IEEETable, buf[20:])
	binary.LittleEndian.PutUint32(buf[16:20], crc)

	return buf
}

// DeserializeHeader parses a header from a byte slice.
// Returns an error if the buffer is too small or the magic is invalid.
func DeserializeHeader(buf []byte) (*Header, error) {
	if len(buf) < HeaderSize {
		return nil, fmt.Errorf("buffer too small: %d < %d", len(buf), HeaderSize)
	}

	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != HeaderMagic {
		return nil, fmt.Errorf("invalid magic: 0x%X", magic)
	}

	h := &Header{
		Magic:     magic,
		Version:   buf[4],
		Tier:      Tier(buf[5]),
		SlotCount: binary.LittleEndian.Uint16(buf[6:8]),
		FreeSlots: binary.LittleEndian.Uint16(buf[8:10]),
		Checksum:  binary.LittleEndian.Uint32(buf[16:20]),
	}
	copy(h.Reserved[:], buf[10:16])
	copy(h.Bitmap[:], buf[20:52])

	return h, nil
}

// Container represents an in-memory view of an SFB container.
type Container struct {
	Block  uint64 // Physical block number
	Header Header // Container header
	dirty  bool   // Header needs to be written to disk
}

// Manager handles SFB container operations.
type Manager struct {
	mu         sync.RWMutex
	dev        *storage.Device
	bitmap     *bitmap.Region
	lockMgr    *lock.Manager
	containers map[uint64]*Container // Block -> Container cache

	// Per-tier lists of containers with free slots.
	// Used for fast allocation without scanning all containers.
	freeByTier [4][]uint64
}

// NewManager creates a new SFB container manager.
func NewManager(dev *storage.Device, bm *bitmap.Region, lockMgr *lock.Manager) *Manager {
	return &Manager{
		dev:        dev,
		bitmap:     bm,
		lockMgr:    lockMgr,
		containers: make(map[uint64]*Container),
	}
}

// SelectTier returns the appropriate tier for a given file size.
func SelectTier(size uint64) (Tier, error) {
	switch {
	case size <= 4*1024:
		return Tier4KB, nil
	case size <= 8*1024:
		return Tier8KB, nil
	case size <= 16*1024:
		return Tier16KB, nil
	case size <= 64*1024:
		return Tier64KB, nil
	default:
		return 0, ErrSizeExceedsSlot
	}
}

// SlotSize returns the slot size for a tier.
func (t Tier) SlotSize() uint32 {
	if int(t) >= len(Tiers) {
		return 0
	}
	return Tiers[t].SlotSize
}

// SlotCount returns the number of slots for a tier.
func (t Tier) SlotCount() uint16 {
	if int(t) >= len(Tiers) {
		return 0
	}
	return Tiers[t].SlotCount
}

// BitmapSize returns the bitmap size in bytes for a tier.
func (t Tier) BitmapSize() uint16 {
	if int(t) >= len(Tiers) {
		return 0
	}
	return Tiers[t].BitmapSize
}

// SlotOffset calculates the byte offset of a slot within a container block.
func SlotOffset(tier Tier, slotIndex uint16) uint64 {
	return uint64(HeaderSize) + uint64(slotIndex)*uint64(tier.SlotSize())
}

// PackPhysicalBlock packs a container block number and slot index into
// the PhysicalBlock field of an extent.
// High 48 bits: Container block number
// Low 16 bits: Slot index
func PackPhysicalBlock(containerBlock uint64, slotIndex uint16) uint64 {
	return (containerBlock << 16) | uint64(slotIndex)
}

// UnpackPhysicalBlock extracts container block and slot index from PhysicalBlock.
func UnpackPhysicalBlock(physicalBlock uint64) (containerBlock uint64, slotIndex uint16) {
	return physicalBlock >> 16, uint16(physicalBlock & 0xFFFF)
}

// AllocateSlot allocates a slot in an SFB container for a file of the given size.
// Returns the container block number and slot index.
func (m *Manager) AllocateSlot(ctx context.Context, size uint64) (containerBlock uint64, slotIndex uint16, err error) {
	tier, err := SelectTier(size)
	if err != nil {
		return 0, 0, err
	}

	// Try to find an existing container with free slots
	m.mu.RLock()
	candidates := m.freeByTier[tier]
	m.mu.RUnlock()

	for _, block := range candidates {
		slotIndex, err = m.tryAllocFromContainer(ctx, block, tier)
		if err == nil {
			return block, slotIndex, nil
		}
		if err != ErrNoFreeSlot {
			return 0, 0, err
		}
		// Container is full, try next
	}

	// No existing container has space - create a new one
	containerBlock, err = m.createContainer(ctx, tier)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create container: %w", err)
	}

	// Allocate from the new container
	slotIndex, err = m.tryAllocFromContainer(ctx, containerBlock, tier)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to allocate from new container: %w", err)
	}

	return containerBlock, slotIndex, nil
}

// tryAllocFromContainer attempts to allocate a slot from a specific container.
func (m *Manager) tryAllocFromContainer(ctx context.Context, block uint64, tier Tier) (uint16, error) {
	// Acquire SFBContainer lock (Level 7)
	if err := m.lockMgr.Acquire(ctx, lock.LockTypeSFBContainer, block, lock.LockModeExclusive); err != nil {
		return 0, err
	}
	defer m.lockMgr.Release(lock.LockTypeSFBContainer, block)

	// Load container (from cache or disk)
	container, err := m.loadContainer(block)
	if err != nil {
		return 0, err
	}

	// Find a free slot in the bitmap
	slotIndex, err := findFreeSlot(&container.Header)
	if err != nil {
		return 0, err
	}

	// Mark slot as allocated
	setSlotAllocated(&container.Header, slotIndex)
	container.Header.FreeSlots--
	container.dirty = true

	// Write header back to disk
	if err := m.writeContainerHeader(block, &container.Header); err != nil {
		// Rollback allocation
		clearSlotAllocated(&container.Header, slotIndex)
		container.Header.FreeSlots++
		return 0, err
	}
	container.dirty = false

	// Update free list if container is now full
	if container.Header.FreeSlots == 0 {
		m.removeFromFreeList(tier, block)
	}

	return slotIndex, nil
}

// createContainer allocates a new 1MB block and initializes it as an SFB container.
func (m *Manager) createContainer(ctx context.Context, tier Tier) (uint64, error) {
	// Allocate a block from the bitmap
	blocks, err := m.bitmap.Alloc(ctx, 1)
	if err != nil {
		return 0, err
	}
	block := blocks[0]

	// Initialize container header
	header := Header{
		Magic:     ContainerMagic,
		Version:   ContainerVersion,
		Tier:      tier,
		SlotCount: tier.SlotCount(),
		FreeSlots: tier.SlotCount(),
	}
	// Bitmap is already zeroed (all slots free)

	// Write header to disk
	if err := m.writeContainerHeader(block, &header); err != nil {
		m.bitmap.Free(ctx, block)
		return 0, err
	}

	// Cache the container
	container := &Container{
		Block:  block,
		Header: header,
	}
	m.mu.Lock()
	m.containers[block] = container
	m.freeByTier[tier] = append(m.freeByTier[tier], block)
	m.mu.Unlock()

	return block, nil
}

// FreeSlot releases a slot back to the container.
func (m *Manager) FreeSlot(ctx context.Context, containerBlock uint64, slotIndex uint16) error {
	// Acquire SFBContainer lock
	if err := m.lockMgr.Acquire(ctx, lock.LockTypeSFBContainer, containerBlock, lock.LockModeExclusive); err != nil {
		return err
	}
	defer m.lockMgr.Release(lock.LockTypeSFBContainer, containerBlock)

	// Load container
	container, err := m.loadContainer(containerBlock)
	if err != nil {
		return err
	}

	// Check if slot is actually allocated
	if !isSlotAllocated(&container.Header, slotIndex) {
		return ErrSlotNotAllocated
	}

	// Clear slot in bitmap
	wasFull := container.Header.FreeSlots == 0
	clearSlotAllocated(&container.Header, slotIndex)
	container.Header.FreeSlots++
	container.dirty = true

	// Write header back
	if err := m.writeContainerHeader(containerBlock, &container.Header); err != nil {
		// Rollback
		setSlotAllocated(&container.Header, slotIndex)
		container.Header.FreeSlots--
		return err
	}
	container.dirty = false

	// Add back to free list if it was full
	if wasFull {
		m.addToFreeList(container.Header.Tier, containerBlock)
	}

	return nil
}

// ReadSlot reads data from a slot.
func (m *Manager) ReadSlot(containerBlock uint64, slotIndex uint16, tier Tier) ([]byte, error) {
	offset := int64(containerBlock)*BlockSize + int64(SlotOffset(tier, slotIndex))
	slotSize := tier.SlotSize()

	buf := make([]byte, slotSize)
	if _, err := m.dev.ReadAt(buf, offset); err != nil {
		return nil, err
	}

	return buf, nil
}

// WriteSlot writes data to a slot.
func (m *Manager) WriteSlot(containerBlock uint64, slotIndex uint16, tier Tier, data []byte) error {
	slotSize := tier.SlotSize()
	if uint32(len(data)) > slotSize {
		return ErrSizeExceedsSlot
	}

	offset := int64(containerBlock)*BlockSize + int64(SlotOffset(tier, slotIndex))

	// Pad data to slot size for aligned writes
	buf := make([]byte, slotSize)
	copy(buf, data)

	if _, err := m.dev.WriteAt(buf, offset); err != nil {
		return err
	}

	return nil
}

// loadContainer loads a container from cache or disk.
func (m *Manager) loadContainer(block uint64) (*Container, error) {
	m.mu.RLock()
	if container, ok := m.containers[block]; ok {
		m.mu.RUnlock()
		return container, nil
	}
	m.mu.RUnlock()

	// Read from disk
	header, err := m.readContainerHeader(block)
	if err != nil {
		return nil, err
	}

	container := &Container{
		Block:  block,
		Header: *header,
	}

	m.mu.Lock()
	m.containers[block] = container
	// Add to free list if it has free slots
	if header.FreeSlots > 0 {
		m.freeByTier[header.Tier] = append(m.freeByTier[header.Tier], block)
	}
	m.mu.Unlock()

	return container, nil
}

// readContainerHeader reads the container header from disk.
func (m *Manager) readContainerHeader(block uint64) (*Header, error) {
	offset := int64(block) * BlockSize

	// Read header (sector-aligned)
	buf := make([]byte, storage.SectorSize)
	if _, err := m.dev.ReadAt(buf, offset); err != nil {
		return nil, err
	}

	return deserializeHeader(buf)
}

// writeContainerHeader writes the container header to disk.
func (m *Manager) writeContainerHeader(block uint64, header *Header) error {
	offset := int64(block) * BlockSize

	data := serializeHeader(header)

	// Pad to sector size for O_DIRECT alignment
	buf := make([]byte, storage.SectorSize)
	copy(buf, data)

	if _, err := m.dev.WriteAt(buf, offset); err != nil {
		return err
	}

	return nil
}

// addToFreeList adds a container to the free list for its tier.
func (m *Manager) addToFreeList(tier Tier, block uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already in list
	for _, b := range m.freeByTier[tier] {
		if b == block {
			return
		}
	}
	m.freeByTier[tier] = append(m.freeByTier[tier], block)
}

// removeFromFreeList removes a container from the free list.
func (m *Manager) removeFromFreeList(tier Tier, block uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	list := m.freeByTier[tier]
	for i, b := range list {
		if b == block {
			m.freeByTier[tier] = append(list[:i], list[i+1:]...)
			return
		}
	}
}

// findFreeSlot finds the first free slot in the bitmap.
func findFreeSlot(header *Header) (uint16, error) {
	bitmapSize := header.Tier.BitmapSize()
	for i := uint16(0); i < bitmapSize; i++ {
		if header.Bitmap[i] != 0xFF {
			// Found a byte with at least one free bit
			for bit := uint16(0); bit < 8; bit++ {
				slot := i*8 + bit
				if slot >= header.SlotCount {
					break
				}
				if (header.Bitmap[i] & (1 << bit)) == 0 {
					return slot, nil
				}
			}
		}
	}
	return 0, ErrNoFreeSlot
}

// isSlotAllocated checks if a slot is allocated.
func isSlotAllocated(header *Header, slot uint16) bool {
	byteIdx := slot / 8
	bitIdx := slot % 8
	return (header.Bitmap[byteIdx] & (1 << bitIdx)) != 0
}

// setSlotAllocated marks a slot as allocated.
func setSlotAllocated(header *Header, slot uint16) {
	byteIdx := slot / 8
	bitIdx := slot % 8
	header.Bitmap[byteIdx] |= (1 << bitIdx)
}

// clearSlotAllocated marks a slot as free.
func clearSlotAllocated(header *Header, slot uint16) {
	byteIdx := slot / 8
	bitIdx := slot % 8
	header.Bitmap[byteIdx] &^= (1 << bitIdx)
}

// serializeHeader converts a Header to bytes.
func serializeHeader(h *Header) []byte {
	buf := make([]byte, HeaderSize)

	binary.LittleEndian.PutUint32(buf[0:4], h.Magic)
	buf[4] = h.Version
	buf[5] = byte(h.Tier)
	binary.LittleEndian.PutUint16(buf[6:8], h.SlotCount)
	binary.LittleEndian.PutUint16(buf[8:10], h.FreeSlots)
	// Reserved: buf[10:16]
	// Checksum placeholder: buf[16:20]
	copy(buf[20:52], h.Bitmap[:])

	// Calculate checksum (excluding the checksum field itself)
	checksum := crc32.ChecksumIEEE(buf[:16])
	checksum = crc32.Update(checksum, crc32.IEEETable, buf[20:])
	binary.LittleEndian.PutUint32(buf[16:20], checksum)

	return buf
}

// deserializeHeader parses a Header from bytes.
func deserializeHeader(buf []byte) (*Header, error) {
	if len(buf) < HeaderSize {
		return nil, ErrInvalidContainer
	}

	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != ContainerMagic {
		return nil, ErrInvalidContainer
	}

	h := &Header{
		Magic:     magic,
		Version:   buf[4],
		Tier:      Tier(buf[5]),
		SlotCount: binary.LittleEndian.Uint16(buf[6:8]),
		FreeSlots: binary.LittleEndian.Uint16(buf[8:10]),
		Checksum:  binary.LittleEndian.Uint32(buf[16:20]),
	}
	copy(h.Reserved[:], buf[10:16])
	copy(h.Bitmap[:], buf[20:52])

	// Verify checksum
	expected := crc32.ChecksumIEEE(buf[:16])
	expected = crc32.Update(expected, crc32.IEEETable, buf[20:HeaderSize])
	if h.Checksum != expected {
		return nil, ErrInvalidContainer
	}

	return h, nil
}

// GetContainerStats returns statistics about cached containers.
func (m *Manager) GetContainerStats() (total, byTier [4]int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total[0] = len(m.containers)
	for tier := Tier(0); tier < 4; tier++ {
		byTier[tier] = len(m.freeByTier[tier])
	}
	return
}
