// Package quota stores per-directory (project) quotas: a table of limits and usage per project ID, kept in the data
// blocks of a hidden inode that the superblock names.
package quota

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// HeaderMagic starts the table's first sector: "GCQT".
	HeaderMagic uint32 = 0x47435154
	// Version is the table format this build writes and reads.
	Version uint32 = 1

	// RecordSize is the bytes of one project's record.
	RecordSize = 64
	// RecordsPerSector is how many records fit in a sector before its checksum.
	RecordsPerSector = (storage.SectorSize - 4) / RecordSize

	// MaxSlots is the registry's node slot count, one FoldedThroughSeq each.
	MaxSlots = 64

	crcOffset = storage.SectorSize - 4
)

var (
	ErrChecksum = errors.New("quota table sector fails its checksum")
	ErrMagic    = errors.New("quota table header has the wrong magic")
	ErrVersion  = errors.New("quota table has a version this build does not know")
)

// Header is the table's first sector.
type Header struct {
	NextProjectID uint32
	// FoldedThroughSeq is, per registry slot, the journal sequence through which a dead node's unflushed deltas were
	// folded into the table, so a second replay of that journal does not count them twice.
	FoldedThroughSeq [MaxSlots]uint64
}

// Record is one project's limits and usage. Usage counts blocks of the volume's block size and inodes.
type Record struct {
	LimitBlocks uint64 // 0: no limit
	UsedBlocks  uint64
	LimitInodes uint64 // 0: no limit
	UsedInodes  uint64
	RootInode   uint32
	Flags       uint32
	RootBirth   uint64 // RootInode's BirthGeneration, so a later inode in the same slot is not taken for the root
}

// RecordInUse marks a record that belongs to a project.
const RecordInUse uint32 = 1 << 0

// RecordLocation returns the table sector that holds project id's record, counting the header as sector 0, and the
// record's byte offset in it.
func RecordLocation(id uint32) (sector uint64, offset int) {
	return 1 + uint64(id)/RecordsPerSector, int(id%RecordsPerSector) * RecordSize
}

// EncodeHeader writes h into a sector, with the magic, version and checksum.
func EncodeHeader(h *Header, sector []byte) {
	clear(sector[:storage.SectorSize])
	binary.LittleEndian.PutUint32(sector[0:4], HeaderMagic)
	binary.LittleEndian.PutUint32(sector[4:8], Version)
	binary.LittleEndian.PutUint32(sector[8:12], h.NextProjectID)
	for i, seq := range h.FoldedThroughSeq {
		binary.LittleEndian.PutUint64(sector[16+8*i:24+8*i], seq)
	}
	seal(sector)
}

// DecodeHeader reads a header sector, checking its checksum, magic and version.
func DecodeHeader(sector []byte) (*Header, error) {
	if err := verify(sector); err != nil {
		return nil, err
	}
	if magic := binary.LittleEndian.Uint32(sector[0:4]); magic != HeaderMagic {
		return nil, fmt.Errorf("%w: %#x", ErrMagic, magic)
	}
	if v := binary.LittleEndian.Uint32(sector[4:8]); v != Version {
		return nil, fmt.Errorf("%w: %d", ErrVersion, v)
	}
	h := &Header{NextProjectID: binary.LittleEndian.Uint32(sector[8:12])}
	for i := range h.FoldedThroughSeq {
		h.FoldedThroughSeq[i] = binary.LittleEndian.Uint64(sector[16+8*i : 24+8*i])
	}
	return h, nil
}

// PutRecord writes r at offset in a record sector and seals the sector again.
func PutRecord(sector []byte, offset int, r *Record) {
	b := sector[offset : offset+RecordSize]
	clear(b)
	binary.LittleEndian.PutUint64(b[0:8], r.LimitBlocks)
	binary.LittleEndian.PutUint64(b[8:16], r.UsedBlocks)
	binary.LittleEndian.PutUint64(b[16:24], r.LimitInodes)
	binary.LittleEndian.PutUint64(b[24:32], r.UsedInodes)
	binary.LittleEndian.PutUint32(b[32:36], r.RootInode)
	binary.LittleEndian.PutUint32(b[36:40], r.Flags)
	binary.LittleEndian.PutUint64(b[40:48], r.RootBirth)
	seal(sector)
}

// GetRecord reads the record at offset in a record sector after checking the sector's checksum.
func GetRecord(sector []byte, offset int) (*Record, error) {
	if err := verify(sector); err != nil {
		return nil, err
	}
	b := sector[offset : offset+RecordSize]
	return &Record{
		LimitBlocks: binary.LittleEndian.Uint64(b[0:8]),
		UsedBlocks:  binary.LittleEndian.Uint64(b[8:16]),
		LimitInodes: binary.LittleEndian.Uint64(b[16:24]),
		UsedInodes:  binary.LittleEndian.Uint64(b[24:32]),
		RootInode:   binary.LittleEndian.Uint32(b[32:36]),
		Flags:       binary.LittleEndian.Uint32(b[36:40]),
		RootBirth:   binary.LittleEndian.Uint64(b[40:48]),
	}, nil
}

// EmptyRecordSector returns a sealed sector of unused records, as a fresh table holds.
func EmptyRecordSector() []byte {
	sector := make([]byte, storage.SectorSize)
	seal(sector)
	return sector
}

func seal(sector []byte) {
	binary.LittleEndian.PutUint32(sector[crcOffset:], storage.CRC32C(sector[:crcOffset]))
}

func verify(sector []byte) error {
	if len(sector) < storage.SectorSize {
		return fmt.Errorf("quota table sector is %d bytes, want %d", len(sector), storage.SectorSize)
	}
	if got, want := storage.CRC32C(sector[:crcOffset]), binary.LittleEndian.Uint32(sector[crcOffset:]); got != want {
		return fmt.Errorf("%w: %#x, stored %#x", ErrChecksum, got, want)
	}
	return nil
}
