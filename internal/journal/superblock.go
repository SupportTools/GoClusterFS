// Package journal implements per-node metadata journaling for crash consistency.
//
// The journal uses write-ahead logging (WAL) to ensure that metadata operations
// can be recovered after a crash. Each node in the cluster has its own dedicated
// journal region to avoid cross-node contention.
package journal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// JournalSuperblockSize is the fixed size of each journal superblock.
	// 512 bytes is sector-aligned for O_DIRECT atomic writes.
	JournalSuperblockSize = 512

	// JournalMagic identifies a valid journal superblock.
	JournalMagic = "GCLSJOURNAL\x00"

	// JournalVersion is the current journal format version.
	JournalVersion uint32 = 1

	// DefaultJournalSlotSize is the default size per node (4MB).
	DefaultJournalSlotSize = 4 * 1024 * 1024

	// JournalDataOffset is where transaction data starts (after superblock).
	// Rounded up to 4KB for alignment.
	JournalDataOffset = 4096
)

// JournalState represents the state of a node's journal.
type JournalState uint32

const (
	// JournalStateClean indicates no uncommitted transactions.
	JournalStateClean JournalState = 0

	// JournalStateDirty indicates uncommitted transactions exist.
	// Recovery is required on mount.
	JournalStateDirty JournalState = 1

	// JournalStateRecovering indicates another node is recovering this journal.
	JournalStateRecovering JournalState = 2

	// JournalStateCorrupt indicates the journal failed validation.
	// Manual intervention (fsck) is required.
	JournalStateCorrupt JournalState = 3
)

// JournalFlags are optional flags for the journal.
type JournalFlags uint32

const (
	// JournalFlagNeedsRecovery indicates recovery was interrupted.
	JournalFlagNeedsRecovery JournalFlags = 1 << 0
)

// Errors
var (
	ErrInvalidJournalMagic     = errors.New("invalid journal magic")
	ErrJournalUninitialized    = errors.New("journal was never initialized")
	ErrJournalChecksumMismatch = errors.New("journal superblock checksum mismatch")
	ErrJournalTooSmall         = errors.New("buffer too small for journal superblock")
	ErrJournalCorrupt          = errors.New("journal is corrupt")
	ErrJournalFull             = errors.New("journal is full")
	ErrJournalRecovering       = errors.New("journal is being recovered by another node")
)

// JournalSuperblock is the on-disk header for each node's journal area.
// Total size: 512 bytes (sector-aligned for O_DIRECT atomic writes).
//
// Layout:
//
//	Offset  Size  Field
//	------  ----  -----
//	0       12    Magic ("GCLSJOURNAL\0")
//	12      4     Version
//	16      16    NodeUUID
//	32      8     NodeGeneration
//	40      4     State
//	44      4     Flags
//	48      8     FirstSeq
//	56      8     LastSeq
//	64      8     NextSeq
//	72      4     HeadOffset
//	76      4     TailOffset
//	80      8     WrapCount
//	88      4     DataAreaOffset
//	92      4     DataAreaSize
//	96      8     LastCheckpointSeq
//	104     8     LastCheckpointTime
//	112     16    RecoveryNodeUUID
//	128     380   Reserved
//	508     4     Checksum
type JournalSuperblock struct {
	// Identity (offset 0-15)
	Magic   [12]byte // "GCLSJOURNAL\0"
	Version uint32   // Journal format version

	// Ownership (offset 16-39)
	NodeUUID       [16]byte // UUID of owning node
	NodeGeneration uint64   // Generation when journal was initialized

	// State (offset 40-47)
	State JournalState // Current journal state
	Flags JournalFlags // Optional flags

	// Sequence tracking (offset 48-71)
	FirstSeq uint64 // Sequence of first valid transaction
	LastSeq  uint64 // Sequence of last committed transaction
	NextSeq  uint64 // Next sequence number to assign

	// Ring buffer pointers (offset 72-87)
	HeadOffset uint32 // Offset of first valid record (relative to DataAreaOffset)
	TailOffset uint32 // Offset after last valid record
	WrapCount  uint64 // Number of times journal has wrapped

	// Geometry (offset 88-95)
	DataAreaOffset uint32 // Where transaction data starts
	DataAreaSize   uint32 // Size of transaction data area

	// Checkpoint info (offset 96-127)
	LastCheckpointSeq  uint64   // Last fully checkpointed sequence
	LastCheckpointTime int64    // Timestamp of last checkpoint (Unix)
	RecoveryNodeUUID   [16]byte // If recovering, by which node

	// Reserved for future use (offset 128-507)
	Reserved [380]byte

	// Checksum (offset 508-511)
	Checksum uint32 // CRC32C of bytes 0-507
}

// String returns a human-readable representation of the journal state.
func (s JournalState) String() string {
	switch s {
	case JournalStateClean:
		return "Clean"
	case JournalStateDirty:
		return "Dirty"
	case JournalStateRecovering:
		return "Recovering"
	case JournalStateCorrupt:
		return "Corrupt"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// NewJournalSuperblock creates a new initialized journal superblock.
func NewJournalSuperblock(nodeUUID [16]byte, generation uint64, dataAreaSize uint32) *JournalSuperblock {
	sb := &JournalSuperblock{
		Version:        JournalVersion,
		NodeUUID:       nodeUUID,
		NodeGeneration: generation,
		State:          JournalStateClean,
		FirstSeq:       1,
		LastSeq:        0,
		NextSeq:        1,
		HeadOffset:     0,
		TailOffset:     0,
		WrapCount:      0,
		DataAreaOffset: JournalDataOffset,
		DataAreaSize:   dataAreaSize,
	}
	copy(sb.Magic[:], JournalMagic)
	return sb
}

// Serialize converts the superblock to bytes for writing to disk.
// Computes and sets the checksum before returning.
func (sb *JournalSuperblock) Serialize() []byte {
	buf := make([]byte, JournalSuperblockSize)

	// Identity
	copy(buf[0:12], sb.Magic[:])
	binary.LittleEndian.PutUint32(buf[12:16], sb.Version)

	// Ownership
	copy(buf[16:32], sb.NodeUUID[:])
	binary.LittleEndian.PutUint64(buf[32:40], sb.NodeGeneration)

	// State
	binary.LittleEndian.PutUint32(buf[40:44], uint32(sb.State))
	binary.LittleEndian.PutUint32(buf[44:48], uint32(sb.Flags))

	// Sequence tracking
	binary.LittleEndian.PutUint64(buf[48:56], sb.FirstSeq)
	binary.LittleEndian.PutUint64(buf[56:64], sb.LastSeq)
	binary.LittleEndian.PutUint64(buf[64:72], sb.NextSeq)

	// Ring buffer pointers
	binary.LittleEndian.PutUint32(buf[72:76], sb.HeadOffset)
	binary.LittleEndian.PutUint32(buf[76:80], sb.TailOffset)
	binary.LittleEndian.PutUint64(buf[80:88], sb.WrapCount)

	// Geometry
	binary.LittleEndian.PutUint32(buf[88:92], sb.DataAreaOffset)
	binary.LittleEndian.PutUint32(buf[92:96], sb.DataAreaSize)

	// Checkpoint info
	binary.LittleEndian.PutUint64(buf[96:104], sb.LastCheckpointSeq)
	binary.LittleEndian.PutUint64(buf[104:112], uint64(sb.LastCheckpointTime))
	copy(buf[112:128], sb.RecoveryNodeUUID[:])

	// Reserved (already zero)

	// Compute and store checksum (last 4 bytes)
	checksum := storage.CRC32C(buf[:JournalSuperblockSize-4])
	binary.LittleEndian.PutUint32(buf[JournalSuperblockSize-4:], checksum)
	sb.Checksum = checksum

	return buf
}

// DeserializeJournalSuperblock parses a journal superblock from raw bytes.
// Returns ErrJournalUninitialized for an all-zero header (format zeroes every
// slot's header), or an error if magic or checksum is invalid.
func DeserializeJournalSuperblock(buf []byte) (*JournalSuperblock, error) {
	if len(buf) < JournalSuperblockSize {
		return nil, ErrJournalTooSmall
	}
	if !slices.ContainsFunc(buf[:JournalSuperblockSize], func(b byte) bool { return b != 0 }) {
		return nil, ErrJournalUninitialized
	}

	sb := &JournalSuperblock{}

	// Identity
	copy(sb.Magic[:], buf[0:12])
	if string(sb.Magic[:]) != JournalMagic {
		return nil, ErrInvalidJournalMagic
	}

	storedChecksum := binary.LittleEndian.Uint32(buf[JournalSuperblockSize-4:])
	computedChecksum := storage.CRC32C(buf[:JournalSuperblockSize-4])
	if storedChecksum != computedChecksum {
		return nil, ErrJournalChecksumMismatch
	}
	sb.Version = binary.LittleEndian.Uint32(buf[12:16])

	// Ownership
	copy(sb.NodeUUID[:], buf[16:32])
	sb.NodeGeneration = binary.LittleEndian.Uint64(buf[32:40])

	// State
	sb.State = JournalState(binary.LittleEndian.Uint32(buf[40:44]))
	sb.Flags = JournalFlags(binary.LittleEndian.Uint32(buf[44:48]))

	// Sequence tracking
	sb.FirstSeq = binary.LittleEndian.Uint64(buf[48:56])
	sb.LastSeq = binary.LittleEndian.Uint64(buf[56:64])
	sb.NextSeq = binary.LittleEndian.Uint64(buf[64:72])

	// Ring buffer pointers
	sb.HeadOffset = binary.LittleEndian.Uint32(buf[72:76])
	sb.TailOffset = binary.LittleEndian.Uint32(buf[76:80])
	sb.WrapCount = binary.LittleEndian.Uint64(buf[80:88])

	// Geometry
	sb.DataAreaOffset = binary.LittleEndian.Uint32(buf[88:92])
	sb.DataAreaSize = binary.LittleEndian.Uint32(buf[92:96])

	// Checkpoint info
	sb.LastCheckpointSeq = binary.LittleEndian.Uint64(buf[96:104])
	sb.LastCheckpointTime = int64(binary.LittleEndian.Uint64(buf[104:112]))
	copy(sb.RecoveryNodeUUID[:], buf[112:128])

	sb.Checksum = storedChecksum

	return sb, nil
}

// IsEmpty returns true if the journal has no transactions.
func (sb *JournalSuperblock) IsEmpty() bool {
	return sb.HeadOffset == sb.TailOffset && sb.WrapCount == 0
}

// AvailableSpace returns the number of bytes available for new transactions.
func (sb *JournalSuperblock) AvailableSpace() uint32 {
	if sb.TailOffset >= sb.HeadOffset {
		// Normal case: head is before tail
		// Available = total - (tail - head)
		used := sb.TailOffset - sb.HeadOffset
		return sb.DataAreaSize - used
	}
	// Wrapped case: tail is before head
	// Available = head - tail
	return sb.HeadOffset - sb.TailOffset
}

// NeedsRecovery returns true if the journal needs to be replayed.
func (sb *JournalSuperblock) NeedsRecovery() bool {
	return sb.State == JournalStateDirty || sb.State == JournalStateRecovering
}
