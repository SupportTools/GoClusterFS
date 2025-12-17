package journal

import (
	"fmt"
	"sync"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// Region manages a single node's journal area on disk.
// It handles reading/writing the superblock and transaction data
// using a ring buffer within the allocated space.
type Region struct {
	mu sync.Mutex

	dev          *storage.Device
	regionOffset uint64 // Absolute byte offset on device
	regionSize   uint64 // Total size of this node's journal region

	// Cached superblock - must be flushed to disk on changes
	superblock *JournalSuperblock

	// Node identity
	nodeUUID   [16]byte
	generation uint64
}

// OpenRegion opens an existing journal region.
// Returns an error if the journal is corrupt or belongs to a different node.
func OpenRegion(dev *storage.Device, regionOffset uint64, regionSize uint64, nodeUUID [16]byte) (*Region, error) {
	r := &Region{
		dev:          dev,
		regionOffset: regionOffset,
		regionSize:   regionSize,
		nodeUUID:     nodeUUID,
	}

	// Read and validate the superblock
	if err := r.readSuperblock(); err != nil {
		return nil, fmt.Errorf("failed to read journal superblock: %w", err)
	}

	// Verify this journal belongs to us
	if r.superblock.NodeUUID != nodeUUID {
		return nil, fmt.Errorf("journal belongs to different node: got %x, want %x",
			r.superblock.NodeUUID, nodeUUID)
	}

	r.generation = r.superblock.NodeGeneration

	return r, nil
}

// InitRegion initializes a new journal region.
// This should only be called when formatting a new filesystem or
// when a node is claiming a previously unused slot.
func InitRegion(dev *storage.Device, regionOffset uint64, regionSize uint64, nodeUUID [16]byte, generation uint64) (*Region, error) {
	if regionSize < uint64(JournalDataOffset)+4096 {
		return nil, fmt.Errorf("region size %d too small, minimum %d", regionSize, JournalDataOffset+4096)
	}

	r := &Region{
		dev:          dev,
		regionOffset: regionOffset,
		regionSize:   regionSize,
		nodeUUID:     nodeUUID,
		generation:   generation,
	}

	// Calculate data area size (region minus superblock area)
	dataAreaSize := uint32(regionSize - uint64(JournalDataOffset))

	// Create new superblock
	r.superblock = NewJournalSuperblock(nodeUUID, generation, dataAreaSize)

	// Write it to disk
	if err := r.writeSuperblock(); err != nil {
		return nil, fmt.Errorf("failed to write journal superblock: %w", err)
	}

	return r, nil
}

// readSuperblock reads the journal superblock from disk.
func (r *Region) readSuperblock() error {
	buf := storage.GetBuffer(JournalSuperblockSize)
	defer storage.PutBuffer(buf)

	if _, err := r.dev.ReadAt(buf.Bytes(), int64(r.regionOffset)); err != nil {
		return err
	}

	sb, err := DeserializeJournalSuperblock(buf.Bytes())
	if err != nil {
		return err
	}

	r.superblock = sb
	return nil
}

// writeSuperblock writes the journal superblock to disk.
func (r *Region) writeSuperblock() error {
	data := r.superblock.Serialize()

	// Pad to sector size for O_DIRECT
	buf := storage.GetBuffer(storage.SectorSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), data)

	if _, err := r.dev.WriteAt(buf.Bytes(), int64(r.regionOffset)); err != nil {
		return err
	}

	return nil
}

// Superblock returns a copy of the current superblock.
func (r *Region) Superblock() JournalSuperblock {
	r.mu.Lock()
	defer r.mu.Unlock()
	return *r.superblock
}

// State returns the current journal state.
func (r *Region) State() JournalState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.superblock.State
}

// SetState updates the journal state and writes to disk.
func (r *Region) SetState(state JournalState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.superblock.State = state
	return r.writeSuperblock()
}

// NeedsRecovery returns true if the journal has uncommitted transactions.
func (r *Region) NeedsRecovery() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.superblock.NeedsRecovery()
}

// IsEmpty returns true if the journal has no transactions.
func (r *Region) IsEmpty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.superblock.IsEmpty()
}

// AvailableSpace returns bytes available for new transactions.
func (r *Region) AvailableSpace() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.superblock.AvailableSpace()
}

// NextSeq returns and increments the next transaction sequence number.
func (r *Region) NextSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	seq := r.superblock.NextSeq
	r.superblock.NextSeq++
	return seq
}

// dataOffset returns the absolute byte offset for a position in the data area.
func (r *Region) dataOffset(pos uint32) int64 {
	return int64(r.regionOffset) + int64(r.superblock.DataAreaOffset) + int64(pos)
}

// WriteAt writes data to the journal at the current tail position.
// Returns the number of bytes written and advances the tail.
// The caller must ensure there is enough space (call AvailableSpace first).
func (r *Region) WriteAt(data []byte) (uint32, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	dataLen := uint32(len(data))
	if dataLen == 0 {
		return 0, nil
	}

	// Align length up to sector size for O_DIRECT
	alignedLen := ((dataLen + storage.SectorSize - 1) / storage.SectorSize) * storage.SectorSize

	available := r.superblock.AvailableSpace()
	if alignedLen > available {
		return 0, ErrJournalFull
	}

	tail := r.superblock.TailOffset
	dataAreaSize := r.superblock.DataAreaSize

	// Handle wrap-around (use aligned length for decision)
	if tail+alignedLen <= dataAreaSize {
		// Simple case: no wrap needed
		offset := r.dataOffset(tail)
		buf := storage.GetBuffer(int(alignedLen))
		defer storage.PutBuffer(buf)
		copy(buf.Bytes(), data)

		if _, err := r.dev.WriteAt(buf.Bytes()[:alignedLen], offset); err != nil {
			return 0, err
		}

		// Advance tail to next sector boundary for O_DIRECT alignment
		r.superblock.TailOffset = tail + alignedLen
	} else {
		// Wrap case: split write into two parts
		firstPart := dataAreaSize - tail
		secondPart := dataLen - firstPart

		// Write first part (to end of area)
		offset1 := r.dataOffset(tail)
		alignedLen1 := ((firstPart + storage.SectorSize - 1) / storage.SectorSize) * storage.SectorSize
		buf1 := storage.GetBuffer(int(alignedLen1))
		copy(buf1.Bytes(), data[:firstPart])
		if _, err := r.dev.WriteAt(buf1.Bytes()[:alignedLen1], offset1); err != nil {
			storage.PutBuffer(buf1)
			return 0, err
		}
		storage.PutBuffer(buf1)

		// Write second part (from start of area)
		offset2 := r.dataOffset(0)
		alignedLen2 := ((secondPart + storage.SectorSize - 1) / storage.SectorSize) * storage.SectorSize
		buf2 := storage.GetBuffer(int(alignedLen2))
		copy(buf2.Bytes(), data[firstPart:])
		if _, err := r.dev.WriteAt(buf2.Bytes()[:alignedLen2], offset2); err != nil {
			storage.PutBuffer(buf2)
			return 0, err
		}
		storage.PutBuffer(buf2)

		// Advance tail to next sector boundary for O_DIRECT alignment
		r.superblock.TailOffset = alignedLen2
		r.superblock.WrapCount++
	}

	return dataLen, nil
}

// ReadAt reads data from the journal at the specified offset (relative to data area).
// This is used during recovery to read transaction records.
func (r *Region) ReadAt(buf []byte, pos uint32) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	dataLen := uint32(len(buf))
	dataAreaSize := r.superblock.DataAreaSize

	if pos >= dataAreaSize {
		pos = pos % dataAreaSize // Handle wrapped positions
	}

	// Handle wrap-around
	if pos+dataLen <= dataAreaSize {
		// Simple case: no wrap needed
		offset := r.dataOffset(pos)

		// For O_DIRECT, we need aligned reads
		alignedLen := ((dataLen + storage.SectorSize - 1) / storage.SectorSize) * storage.SectorSize
		alignedBuf := storage.GetBuffer(int(alignedLen))
		defer storage.PutBuffer(alignedBuf)

		n, err := r.dev.ReadAt(alignedBuf.Bytes()[:alignedLen], offset)
		if err != nil {
			return n, err
		}
		copy(buf, alignedBuf.Bytes()[:dataLen])
		return int(dataLen), nil
	}

	// Wrap case: split read into two parts
	firstPart := dataAreaSize - pos
	secondPart := dataLen - firstPart

	// Read first part
	offset1 := r.dataOffset(pos)
	alignedLen1 := ((firstPart + storage.SectorSize - 1) / storage.SectorSize) * storage.SectorSize
	buf1 := storage.GetBuffer(int(alignedLen1))
	if _, err := r.dev.ReadAt(buf1.Bytes()[:alignedLen1], offset1); err != nil {
		storage.PutBuffer(buf1)
		return 0, err
	}
	copy(buf[:firstPart], buf1.Bytes()[:firstPart])
	storage.PutBuffer(buf1)

	// Read second part
	offset2 := r.dataOffset(0)
	alignedLen2 := ((secondPart + storage.SectorSize - 1) / storage.SectorSize) * storage.SectorSize
	buf2 := storage.GetBuffer(int(alignedLen2))
	if _, err := r.dev.ReadAt(buf2.Bytes()[:alignedLen2], offset2); err != nil {
		storage.PutBuffer(buf2)
		return int(firstPart), err
	}
	copy(buf[firstPart:], buf2.Bytes()[:secondPart])
	storage.PutBuffer(buf2)

	return int(dataLen), nil
}

// MarkDirty marks the journal as having uncommitted transactions.
func (r *Region) MarkDirty() error {
	return r.SetState(JournalStateDirty)
}

// MarkClean marks the journal as having no uncommitted transactions.
func (r *Region) MarkClean() error {
	return r.SetState(JournalStateClean)
}

// Checkpoint advances the head pointer to reclaim space from completed transactions.
func (r *Region) Checkpoint(seq uint64, newHead uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.superblock.HeadOffset = newHead
	r.superblock.LastCheckpointSeq = seq

	return r.writeSuperblock()
}

// Sync ensures all journal data is persisted to disk.
func (r *Region) Sync() error {
	return r.dev.Sync()
}

// Close closes the journal region.
// The journal should be marked clean before closing if appropriate.
func (r *Region) Close() error {
	// Nothing to close - device is managed externally
	return nil
}

// HeadOffset returns the current head offset.
func (r *Region) HeadOffset() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.superblock.HeadOffset
}

// TailOffset returns the current tail offset.
func (r *Region) TailOffset() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.superblock.TailOffset
}

// DataAreaSize returns the size of the data area.
func (r *Region) DataAreaSize() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.superblock.DataAreaSize
}

// UpdateTail updates the tail offset after a transaction is written.
// This is separate from WriteAt for cases where we need finer control.
func (r *Region) UpdateTail(newTail uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.superblock.TailOffset = newTail
	return nil
}

// FlushSuperblock writes the current superblock state to disk.
func (r *Region) FlushSuperblock() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writeSuperblock()
}

// Reset clears the journal, making it ready for new transactions.
// This is called after successful recovery.
func (r *Region) Reset() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.superblock.State = JournalStateClean
	r.superblock.HeadOffset = 0
	r.superblock.TailOffset = 0
	r.superblock.FirstSeq = r.superblock.NextSeq
	r.superblock.LastSeq = 0
	r.superblock.LastCheckpointSeq = r.superblock.NextSeq - 1

	return r.writeSuperblock()
}
