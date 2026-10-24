// Package snapshot implements snapshot management for GoClusterFS.
//
// Snapshots provide point-in-time copies of files using Copy-on-Write (CoW)
// semantics. When a snapshot is created, it references the same data blocks
// as the original. Modifications to either copy trigger CoW to preserve the
// other version.
//
// Snapshot creation is O(1) - it creates a descriptor pointing to the same
// extent tree and queues background refcount increments for all blocks.
//
// Layout: Each descriptor is 128 bytes, stored in 4096-byte sectors
// (32 descriptors per sector) with CRC32C checksums.
package snapshot

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// DescriptorSize is the size of each snapshot descriptor in bytes.
	DescriptorSize = 128

	// SectorSize is the disk sector size (matches storage.SectorSize).
	SectorSize = 4096

	// ChecksumSize is the size of the CRC32C checksum.
	ChecksumSize = 4

	// DescriptorsPerSector is the number of descriptors per disk sector.
	// (SectorSize - ChecksumSize) / DescriptorSize = 4092 / 128 = 31
	DescriptorsPerSector = (SectorSize - ChecksumSize) / DescriptorSize

	// BytesPerSector is the usable bytes per sector.
	BytesPerSector = DescriptorsPerSector * DescriptorSize

	// DescriptorMagic identifies a valid snapshot descriptor.
	DescriptorMagic uint32 = 0x534E4150 // "SNAP"

	// MaxSnapshotNameLen is the maximum length of a snapshot name.
	MaxSnapshotNameLen = 48

	// InvalidSnapshotID represents no snapshot.
	InvalidSnapshotID uint64 = 0
)

// Snapshot states
type State uint8

const (
	// StateInvalid represents an invalid/free slot.
	StateInvalid State = 0

	// StateCreating indicates snapshot creation is in progress.
	// Refcount increments may be pending.
	StateCreating State = 1

	// StateActive indicates snapshot is fully created and available.
	StateActive State = 2

	// StateDeleting indicates snapshot deletion is in progress.
	// Refcount decrements may be pending.
	StateDeleting State = 3
)

// String returns a human-readable state name.
func (s State) String() string {
	switch s {
	case StateInvalid:
		return "invalid"
	case StateCreating:
		return "creating"
	case StateActive:
		return "active"
	case StateDeleting:
		return "deleting"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// Snapshot flags
type Flags uint8

const (
	// FlagNone indicates no special flags.
	FlagNone Flags = 0

	// FlagReadOnly indicates the snapshot is read-only (typical).
	FlagReadOnly Flags = 1 << 0

	// FlagWritable indicates the snapshot is a writable clone.
	FlagWritable Flags = 1 << 1

	// FlagHidden indicates the snapshot is hidden from normal listing.
	FlagHidden Flags = 1 << 2

	// FlagAutoDelete indicates the snapshot should be auto-deleted
	// when no longer referenced.
	FlagAutoDelete Flags = 1 << 3
)

// Errors
var (
	ErrSnapshotNotFound     = errors.New("snapshot not found")
	ErrSnapshotExists       = errors.New("snapshot already exists")
	ErrSnapshotInvalidState = errors.New("snapshot in invalid state")
	ErrSnapshotFull         = errors.New("snapshot registry full")
	ErrInvalidDescriptor    = errors.New("invalid snapshot descriptor")
	ErrChecksumMismatch     = errors.New("snapshot descriptor checksum mismatch")
	ErrNameTooLong          = errors.New("snapshot name too long")
	ErrInvalidID            = errors.New("invalid snapshot ID")
)

// Descriptor represents a snapshot on disk (128 bytes).
//
// Snapshots form a tree structure where each snapshot references its parent.
// The root snapshot has ParentID = InvalidSnapshotID (0).
//
// Disk layout (128 bytes):
//
//	Offset  Size  Field
//	------  ----  -----
//	0       4     Magic (0x534E4150 "SNAP")
//	4       8     SnapshotID (unique identifier)
//	12      8     ParentID (parent snapshot, 0 for root)
//	20      4     TargetInode (file/directory being snapshotted)
//	24      1     State (Creating/Active/Deleting)
//	25      1     Flags (ReadOnly/Writable/Hidden)
//	26      2     Reserved1
//	28      8     ExtentTreeRoot (copied from inode at snapshot time)
//	36      8     TotalBlocks (block count at snapshot time)
//	44      8     TotalSize (file size at snapshot time)
//	52      8     CreateTime (Unix timestamp)
//	60      8     DeleteTime (Unix timestamp, 0 if not deleted)
//	68      8     RefCountJobID (background job ID for refcount updates)
//	76      48    Name (null-terminated UTF-8 string)
//	124     4     Checksum (CRC32C of bytes 0-123)
type Descriptor struct {
	Magic          uint32                   // 0x534E4150 "SNAP"
	SnapshotID     uint64                   // Unique snapshot identifier
	ParentID       uint64                   // Parent snapshot (0 for root)
	TargetInode    uint32                   // Inode number of snapshotted file
	State          State                    // Current state
	Flags          Flags                    // Snapshot flags
	Reserved1      uint16                   // Alignment padding
	ExtentTreeRoot uint64                   // Extent tree root block
	TotalBlocks    uint64                   // Number of blocks at snapshot time
	TotalSize      uint64                   // File size at snapshot time
	CreateTime     int64                    // Creation timestamp
	DeleteTime     int64                    // Deletion timestamp (0 if active)
	RefCountJobID  uint64                   // Background refcount job ID
	Name           [MaxSnapshotNameLen]byte // Snapshot name
	Checksum       uint32                   // CRC32C checksum
}

// IsValid returns true if this is a valid snapshot descriptor.
func (d *Descriptor) IsValid() bool {
	return d.Magic == DescriptorMagic && d.State != StateInvalid
}

// IsFree returns true if this slot is free (can be reused).
func (d *Descriptor) IsFree() bool {
	return d.Magic == 0 || d.State == StateInvalid
}

// IsActive returns true if the snapshot is fully available.
func (d *Descriptor) IsActive() bool {
	return d.State == StateActive
}

// IsReadOnly returns true if the snapshot is read-only.
func (d *Descriptor) IsReadOnly() bool {
	return (d.Flags & FlagReadOnly) != 0
}

// IsWritable returns true if the snapshot is a writable clone.
func (d *Descriptor) IsWritable() bool {
	return (d.Flags & FlagWritable) != 0
}

// GetName returns the snapshot name as a string.
func (d *Descriptor) GetName() string {
	// Find null terminator
	for i, b := range d.Name {
		if b == 0 {
			return string(d.Name[:i])
		}
	}
	return string(d.Name[:])
}

// SetName sets the snapshot name, truncating if too long.
func (d *Descriptor) SetName(name string) error {
	if len(name) >= MaxSnapshotNameLen {
		return ErrNameTooLong
	}
	// Clear the name buffer
	for i := range d.Name {
		d.Name[i] = 0
	}
	copy(d.Name[:], name)
	return nil
}

// Registry manages all snapshots on disk.
type Registry struct {
	mu              sync.RWMutex
	dev             *storage.Device
	offset          uint64        // Region start offset on device
	size            uint64        // Region size in bytes
	maxDescriptors  uint64        // Maximum number of descriptors
	lockMgr         *lock.Manager // For distributed locking (optional)
	nextID          uint64        // Next available snapshot ID (cached)
	descriptorCount uint64        // Active descriptor count (cached)
}

// NewRegistry creates a new snapshot registry manager.
//
// Parameters:
//   - dev: The storage device
//   - offset: Byte offset where snapshot region starts on device
//   - size: Total size of snapshot region in bytes
//   - lockMgr: Lock manager for distributed locking (can be nil for single-node)
func NewRegistry(dev *storage.Device, offset, size uint64, lockMgr *lock.Manager) *Registry {
	maxDesc := (size / SectorSize) * DescriptorsPerSector
	return &Registry{
		dev:            dev,
		offset:         offset,
		size:           size,
		maxDescriptors: maxDesc,
		lockMgr:        lockMgr,
		nextID:         1, // Start from 1 (0 is invalid)
	}
}

// Create creates a new snapshot descriptor.
//
// Parameters:
//   - name: Human-readable snapshot name
//   - targetInode: Inode number being snapshotted
//   - parentID: Parent snapshot ID (0 for root snapshot)
//   - extentTreeRoot: Extent tree root block from the inode
//   - totalBlocks: Number of blocks at snapshot time
//   - totalSize: File size at snapshot time
//   - flags: Snapshot flags (ReadOnly, Writable, etc.)
//
// Returns the new snapshot ID and descriptor.
func (r *Registry) Create(ctx context.Context, name string, targetInode uint32,
	parentID uint64, extentTreeRoot uint64, totalBlocks uint64,
	totalSize uint64, flags Flags) (*Descriptor, error) {

	if len(name) >= MaxSnapshotNameLen {
		return nil, ErrNameTooLong
	}

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		// Use lock type for snapshot registry (we'll use a constant ID since registry is single)
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeSnapshot, 0, lock.LockModeExclusive); err != nil {
			return nil, fmt.Errorf("failed to acquire snapshot lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeSnapshot, 0)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Find a free slot
	slot, err := r.findFreeSlot()
	if err != nil {
		return nil, err
	}

	// Allocate a new ID
	snapshotID := r.nextID
	r.nextID++

	// Create the descriptor
	desc := &Descriptor{
		Magic:          DescriptorMagic,
		SnapshotID:     snapshotID,
		ParentID:       parentID,
		TargetInode:    targetInode,
		State:          StateCreating, // Will be marked Active after refcount job
		Flags:          flags | FlagReadOnly,
		ExtentTreeRoot: extentTreeRoot,
		TotalBlocks:    totalBlocks,
		TotalSize:      totalSize,
		CreateTime:     time.Now().Unix(),
	}
	if err := desc.SetName(name); err != nil {
		return nil, err
	}

	// Write to disk
	if err := r.writeDescriptor(slot, desc); err != nil {
		return nil, err
	}

	r.descriptorCount++
	return desc, nil
}

// Get retrieves a snapshot descriptor by ID.
func (r *Registry) Get(ctx context.Context, snapshotID uint64) (*Descriptor, error) {
	if snapshotID == InvalidSnapshotID {
		return nil, ErrInvalidID
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Scan for the descriptor (linear search for now)
	for slot := uint64(0); slot < r.maxDescriptors; slot++ {
		desc, err := r.readDescriptor(slot)
		if err != nil {
			continue
		}
		if desc.IsValid() && desc.SnapshotID == snapshotID {
			return desc, nil
		}
	}

	return nil, ErrSnapshotNotFound
}

// GetByName retrieves a snapshot descriptor by name.
func (r *Registry) GetByName(ctx context.Context, name string) (*Descriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for slot := uint64(0); slot < r.maxDescriptors; slot++ {
		desc, err := r.readDescriptor(slot)
		if err != nil {
			continue
		}
		if desc.IsValid() && desc.GetName() == name {
			return desc, nil
		}
	}

	return nil, ErrSnapshotNotFound
}

// List returns all active snapshots.
func (r *Registry) List(ctx context.Context) ([]*Descriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var snapshots []*Descriptor
	for slot := uint64(0); slot < r.maxDescriptors; slot++ {
		desc, err := r.readDescriptor(slot)
		if err != nil {
			continue
		}
		if desc.IsValid() && (desc.Flags&FlagHidden) == 0 {
			snapshots = append(snapshots, desc)
		}
	}

	return snapshots, nil
}

// ListForInode returns all snapshots for a specific inode.
func (r *Registry) ListForInode(ctx context.Context, inodeNum uint32) ([]*Descriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var snapshots []*Descriptor
	for slot := uint64(0); slot < r.maxDescriptors; slot++ {
		desc, err := r.readDescriptor(slot)
		if err != nil {
			continue
		}
		if desc.IsValid() && desc.TargetInode == inodeNum {
			snapshots = append(snapshots, desc)
		}
	}

	return snapshots, nil
}

// GetChildren returns all snapshots whose ParentID matches the given snapshot ID.
// Used for merge-up during middle snapshot deletion.
func (r *Registry) GetChildren(ctx context.Context, snapshotID uint64) ([]*Descriptor, error) {
	if snapshotID == InvalidSnapshotID {
		return nil, ErrInvalidID
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	var children []*Descriptor
	for slot := uint64(0); slot < r.maxDescriptors; slot++ {
		desc, err := r.readDescriptor(slot)
		if err != nil {
			continue
		}
		if desc.IsValid() && desc.ParentID == snapshotID {
			children = append(children, desc)
		}
	}

	return children, nil
}

// UpdateParentID updates the parent ID of a snapshot.
// Used during merge-up when deleting a middle snapshot - children are re-parented
// to their grandparent (the deleted snapshot's parent).
func (r *Registry) UpdateParentID(ctx context.Context, snapshotID, newParentID uint64) error {
	if snapshotID == InvalidSnapshotID {
		return ErrInvalidID
	}

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeSnapshot, 0, lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire snapshot lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeSnapshot, 0)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for slot := uint64(0); slot < r.maxDescriptors; slot++ {
		desc, err := r.readDescriptor(slot)
		if err != nil {
			continue
		}
		if desc.IsValid() && desc.SnapshotID == snapshotID {
			desc.ParentID = newParentID
			return r.writeDescriptor(slot, desc)
		}
	}

	return ErrSnapshotNotFound
}

// UpdateState updates the state of a snapshot.
func (r *Registry) UpdateState(ctx context.Context, snapshotID uint64, newState State) error {
	if snapshotID == InvalidSnapshotID {
		return ErrInvalidID
	}

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeSnapshot, 0, lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire snapshot lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeSnapshot, 0)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Find and update the descriptor
	for slot := uint64(0); slot < r.maxDescriptors; slot++ {
		desc, err := r.readDescriptor(slot)
		if err != nil {
			continue
		}
		if desc.IsValid() && desc.SnapshotID == snapshotID {
			desc.State = newState
			if newState == StateDeleting {
				desc.DeleteTime = time.Now().Unix()
			}
			return r.writeDescriptor(slot, desc)
		}
	}

	return ErrSnapshotNotFound
}

// SetRefCountJobID sets the background refcount job ID for a snapshot.
func (r *Registry) SetRefCountJobID(ctx context.Context, snapshotID, jobID uint64) error {
	if snapshotID == InvalidSnapshotID {
		return ErrInvalidID
	}

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeSnapshot, 0, lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire snapshot lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeSnapshot, 0)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for slot := uint64(0); slot < r.maxDescriptors; slot++ {
		desc, err := r.readDescriptor(slot)
		if err != nil {
			continue
		}
		if desc.IsValid() && desc.SnapshotID == snapshotID {
			desc.RefCountJobID = jobID
			return r.writeDescriptor(slot, desc)
		}
	}

	return ErrSnapshotNotFound
}

// Delete marks a snapshot for deletion.
// The actual cleanup (refcount decrements, freeing blocks) happens asynchronously.
func (r *Registry) Delete(ctx context.Context, snapshotID uint64) error {
	return r.UpdateState(ctx, snapshotID, StateDeleting)
}

// Remove permanently removes a deleted snapshot descriptor.
// Should only be called after all cleanup (refcount decrements) is complete.
func (r *Registry) Remove(ctx context.Context, snapshotID uint64) error {
	if snapshotID == InvalidSnapshotID {
		return ErrInvalidID
	}

	// Acquire distributed lock if available
	if r.lockMgr != nil {
		if err := r.lockMgr.Acquire(ctx, lock.LockTypeSnapshot, 0, lock.LockModeExclusive); err != nil {
			return fmt.Errorf("failed to acquire snapshot lock: %w", err)
		}
		defer r.lockMgr.Release(lock.LockTypeSnapshot, 0)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for slot := uint64(0); slot < r.maxDescriptors; slot++ {
		desc, err := r.readDescriptor(slot)
		if err != nil {
			continue
		}
		if desc.IsValid() && desc.SnapshotID == snapshotID {
			if desc.State != StateDeleting {
				return ErrSnapshotInvalidState
			}
			// Clear the slot
			emptyDesc := &Descriptor{}
			if err := r.writeDescriptor(slot, emptyDesc); err != nil {
				return err
			}
			r.descriptorCount--
			return nil
		}
	}

	return ErrSnapshotNotFound
}

// Count returns the number of active snapshots.
func (r *Registry) Count() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.descriptorCount
}

// MaxDescriptors returns the maximum number of snapshots supported.
func (r *Registry) MaxDescriptors() uint64 {
	return r.maxDescriptors
}

// Initialize initializes the snapshot registry with all empty descriptors.
// This should be called during filesystem format.
func (r *Registry) Initialize(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	numSectors := r.size / SectorSize
	emptyData := make([]byte, BytesPerSector)

	for sectorIdx := uint64(0); sectorIdx < numSectors; sectorIdx++ {
		if err := r.writeSector(sectorIdx, emptyData); err != nil {
			return fmt.Errorf("failed to initialize sector %d: %w", sectorIdx, err)
		}
	}

	r.nextID = 1
	r.descriptorCount = 0
	return nil
}

// Scan scans the registry to rebuild cached counters.
// Call this on mount to restore state.
func (r *Registry) Scan(ctx context.Context) (ScanStats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var stats ScanStats
	var maxID uint64

	for slot := uint64(0); slot < r.maxDescriptors; slot++ {
		desc, err := r.readDescriptor(slot)
		if err != nil {
			continue
		}

		if desc.IsFree() {
			stats.FreeSlots++
			continue
		}

		if !desc.IsValid() {
			stats.InvalidSlots++
			continue
		}

		if desc.SnapshotID > maxID {
			maxID = desc.SnapshotID
		}

		switch desc.State {
		case StateCreating:
			stats.CreatingCount++
		case StateActive:
			stats.ActiveCount++
		case StateDeleting:
			stats.DeletingCount++
		}

		stats.TotalBlocks += desc.TotalBlocks
	}

	// Update cached values
	r.nextID = maxID + 1
	r.descriptorCount = stats.CreatingCount + stats.ActiveCount + stats.DeletingCount

	return stats, nil
}

// ScanStats contains statistics from a registry scan.
type ScanStats struct {
	FreeSlots     uint64 // Free descriptor slots
	InvalidSlots  uint64 // Invalid/corrupted slots
	CreatingCount uint64 // Snapshots in creating state
	ActiveCount   uint64 // Active snapshots
	DeletingCount uint64 // Snapshots being deleted
	TotalBlocks   uint64 // Total blocks referenced by all snapshots
}

// findFreeSlot finds the first free slot in the registry.
func (r *Registry) findFreeSlot() (uint64, error) {
	for slot := uint64(0); slot < r.maxDescriptors; slot++ {
		desc, err := r.readDescriptor(slot)
		if err != nil {
			// Treat read errors as potentially free (might be uninitialized)
			return slot, nil
		}
		if desc.IsFree() {
			return slot, nil
		}
	}
	return 0, ErrSnapshotFull
}

// readDescriptor reads a descriptor from a slot.
func (r *Registry) readDescriptor(slot uint64) (*Descriptor, error) {
	if slot >= r.maxDescriptors {
		return nil, ErrInvalidDescriptor
	}

	sectorIdx := slot / DescriptorsPerSector
	descOffset := slot % DescriptorsPerSector

	sectorData, err := r.readSector(sectorIdx)
	if err != nil {
		return nil, err
	}

	return decodeDescriptor(sectorData, descOffset)
}

// writeDescriptor writes a descriptor to a slot.
func (r *Registry) writeDescriptor(slot uint64, desc *Descriptor) error {
	if slot >= r.maxDescriptors {
		return ErrInvalidDescriptor
	}

	sectorIdx := slot / DescriptorsPerSector
	descOffset := slot % DescriptorsPerSector

	sectorData, err := r.readSector(sectorIdx)
	if err != nil {
		// Initialize sector if it can't be read
		sectorData = make([]byte, BytesPerSector)
	}

	encodeDescriptor(sectorData, descOffset, desc)
	return r.writeSector(sectorIdx, sectorData)
}

// readSector reads a snapshot registry sector from disk.
func (r *Registry) readSector(sectorIdx uint64) ([]byte, error) {
	offset := int64(r.offset) + int64(sectorIdx)*SectorSize

	buf := storage.GetBuffer(SectorSize)
	defer storage.PutBuffer(buf)

	if _, err := r.dev.ReadAt(buf.Bytes(), offset); err != nil {
		return nil, err
	}

	data := buf.Bytes()

	// Verify checksum (skip for all-zero sectors)
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
			result := make([]byte, BytesPerSector)
			return result, nil
		}
	}

	if storedChecksum != computedChecksum {
		return nil, ErrChecksumMismatch
	}

	// Return a copy of the data portion
	result := make([]byte, BytesPerSector)
	copy(result, data[:BytesPerSector])
	return result, nil
}

// writeSector writes a snapshot registry sector to disk with checksum.
func (r *Registry) writeSector(sectorIdx uint64, data []byte) error {
	if len(data) != BytesPerSector {
		return fmt.Errorf("invalid sector data size: %d", len(data))
	}

	offset := int64(r.offset) + int64(sectorIdx)*SectorSize

	buf := storage.GetBuffer(SectorSize)
	defer storage.PutBuffer(buf)

	// Copy data and compute checksum
	copy(buf.Bytes()[:BytesPerSector], data)
	// Zero padding between BytesPerSector and SectorSize-ChecksumSize
	for i := BytesPerSector; i < SectorSize-ChecksumSize; i++ {
		buf.Bytes()[i] = 0
	}
	checksum := storage.CRC32C(buf.Bytes()[:SectorSize-ChecksumSize])
	binary.LittleEndian.PutUint32(buf.Bytes()[SectorSize-ChecksumSize:], checksum)

	_, err := r.dev.WriteAt(buf.Bytes(), offset)
	return err
}

// decodeDescriptor decodes a descriptor from sector data.
func decodeDescriptor(sectorData []byte, descIdx uint64) (*Descriptor, error) {
	offset := descIdx * DescriptorSize
	if offset+DescriptorSize > uint64(len(sectorData)) {
		return nil, ErrInvalidDescriptor
	}

	data := sectorData[offset : offset+DescriptorSize]

	// Check for empty slot
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic == 0 {
		return &Descriptor{}, nil
	}

	// Verify per-descriptor checksum
	stored := binary.LittleEndian.Uint32(data[DescriptorSize-4:])
	computed := storage.CRC32C(data[:DescriptorSize-4])
	if stored != computed {
		return nil, ErrChecksumMismatch
	}

	desc := &Descriptor{
		Magic:          magic,
		SnapshotID:     binary.LittleEndian.Uint64(data[4:12]),
		ParentID:       binary.LittleEndian.Uint64(data[12:20]),
		TargetInode:    binary.LittleEndian.Uint32(data[20:24]),
		State:          State(data[24]),
		Flags:          Flags(data[25]),
		Reserved1:      binary.LittleEndian.Uint16(data[26:28]),
		ExtentTreeRoot: binary.LittleEndian.Uint64(data[28:36]),
		TotalBlocks:    binary.LittleEndian.Uint64(data[36:44]),
		TotalSize:      binary.LittleEndian.Uint64(data[44:52]),
		CreateTime:     int64(binary.LittleEndian.Uint64(data[52:60])),
		DeleteTime:     int64(binary.LittleEndian.Uint64(data[60:68])),
		RefCountJobID:  binary.LittleEndian.Uint64(data[68:76]),
		Checksum:       stored,
	}
	copy(desc.Name[:], data[76:124])

	return desc, nil
}

// encodeDescriptor encodes a descriptor into sector data.
func encodeDescriptor(sectorData []byte, descIdx uint64, desc *Descriptor) {
	offset := descIdx * DescriptorSize
	data := sectorData[offset : offset+DescriptorSize]

	// Clear the descriptor area
	for i := range data {
		data[i] = 0
	}

	// Don't write anything for empty descriptor
	if desc.Magic == 0 {
		return
	}

	binary.LittleEndian.PutUint32(data[0:4], desc.Magic)
	binary.LittleEndian.PutUint64(data[4:12], desc.SnapshotID)
	binary.LittleEndian.PutUint64(data[12:20], desc.ParentID)
	binary.LittleEndian.PutUint32(data[20:24], desc.TargetInode)
	data[24] = byte(desc.State)
	data[25] = byte(desc.Flags)
	binary.LittleEndian.PutUint16(data[26:28], desc.Reserved1)
	binary.LittleEndian.PutUint64(data[28:36], desc.ExtentTreeRoot)
	binary.LittleEndian.PutUint64(data[36:44], desc.TotalBlocks)
	binary.LittleEndian.PutUint64(data[44:52], desc.TotalSize)
	binary.LittleEndian.PutUint64(data[52:60], uint64(desc.CreateTime))
	binary.LittleEndian.PutUint64(data[60:68], uint64(desc.DeleteTime))
	binary.LittleEndian.PutUint64(data[68:76], desc.RefCountJobID)
	copy(data[76:124], desc.Name[:])

	// Compute and store checksum
	checksum := storage.CRC32C(data[:DescriptorSize-4])
	binary.LittleEndian.PutUint32(data[DescriptorSize-4:], checksum)
}

// SizeForSnapshots calculates the region size needed for a given number of snapshots.
// Rounds up to sector boundary.
func SizeForSnapshots(count uint64) uint64 {
	numSectors := (count + DescriptorsPerSector - 1) / DescriptorsPerSector
	return numSectors * SectorSize
}

// DescriptorsInSize returns how many descriptors fit in a given region size.
func DescriptorsInSize(size uint64) uint64 {
	return (size / SectorSize) * DescriptorsPerSector
}
