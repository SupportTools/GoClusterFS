package quota

import (
	"errors"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func TestHeaderRoundTrip(t *testing.T) {
	h := &Header{NextProjectID: 17}
	sector := make([]byte, storage.SectorSize)
	EncodeHeader(h, sector)
	got, err := DecodeHeader(sector)
	if err != nil || *got != *h {
		t.Fatalf("DecodeHeader = %+v, %v; want %+v", got, err, h)
	}
	sector[100] ^= 1
	if _, err := DecodeHeader(sector); !errors.Is(err, ErrChecksum) {
		t.Errorf("a flipped bit: %v, want ErrChecksum", err)
	}
	EncodeHeader(h, sector)
	sector[0] ^= 1
	seal(sector)
	if _, err := DecodeHeader(sector); !errors.Is(err, ErrMagic) {
		t.Errorf("a wrong magic: %v, want ErrMagic", err)
	}
}

// Records of neighbouring projects share a sector, and writing one leaves the other intact.
func TestRecordsShareASector(t *testing.T) {
	a := &Record{LimitBlocks: 1024, UsedBlocks: 3, LimitInodes: 100, UsedInodes: 2, RootInode: 9, Flags: RecordInUse, RootBirth: 4}
	b := &Record{LimitBlocks: 1, UsedBlocks: 1, Flags: RecordInUse}
	sectorA, offA := RecordLocation(1)
	sectorB, offB := RecordLocation(2)
	if sectorA != sectorB || offA == offB {
		t.Fatalf("projects 1 and 2 at sector %d offset %d and sector %d offset %d; want one sector", sectorA, offA, sectorB, offB)
	}
	sector := EmptyRecordSector()
	PutRecord(sector, offA, a)
	PutRecord(sector, offB, b)
	if got, err := GetRecord(sector, offA); err != nil || *got != *a {
		t.Errorf("record 1 = %+v, %v; want %+v", got, err, a)
	}
	if got, err := GetRecord(sector, offB); err != nil || *got != *b {
		t.Errorf("record 2 = %+v, %v; want %+v", got, err, b)
	}
	sector[offA] ^= 1
	if _, err := GetRecord(sector, offB); !errors.Is(err, ErrChecksum) {
		t.Errorf("a flipped bit in record 1, reading record 2: %v, want ErrChecksum", err)
	}
}

func TestRecordLocation(t *testing.T) {
	for _, tc := range []struct {
		id     uint32
		sector uint64
		offset int
	}{
		{0, 1, 0},
		{RecordsPerSector - 1, 1, (RecordsPerSector - 1) * RecordSize},
		{RecordsPerSector, 2, 0},
	} {
		if sector, offset := RecordLocation(tc.id); sector != tc.sector || offset != tc.offset {
			t.Errorf("RecordLocation(%d) = %d, %d; want %d, %d", tc.id, sector, offset, tc.sector, tc.offset)
		}
	}
	if end := RecordsPerSector * RecordSize; end > crcOffset {
		t.Errorf("records end at %d, past the checksum at %d", end, crcOffset)
	}
}

// A record belongs to the ID it stores, or to its index when an older build wrote it, and only while in use. Each
// reuse adds 1<<IndexBits to the ID, wrapping after the top bits run out.
func TestRecordOwnership(t *testing.T) {
	legacy := &Record{Flags: RecordInUse}
	if !legacy.Owns(5) || legacy.Owns(5+1<<IndexBits) || legacy.NextID(5) != 5 {
		t.Errorf("a record without an ID owns 5: %v, owns a later 5: %v, next ID %d; want true, false, 5",
			legacy.Owns(5), legacy.Owns(5+1<<IndexBits), legacy.NextID(5))
	}
	later := uint32(5 + 1<<IndexBits)
	rec := &Record{Flags: RecordInUse, ID: later}
	if rec.Owns(5) || !rec.Owns(later) || Index(later) != 5 {
		t.Errorf("record of %d owns 5: %v, owns %d: %v, index %d", later, rec.Owns(5), later, rec.Owns(later), Index(later))
	}
	rec.Flags = 0
	if rec.Owns(later) || rec.NextID(5) != 5+2<<IndexBits || rec.Owner(5) != later {
		t.Errorf("a freed record owns %d: %v, next ID %d, owner %d", later, rec.Owns(later), rec.NextID(5), rec.Owner(5))
	}
	last := &Record{ID: 5 | 0xFFF<<IndexBits}
	if next := last.NextID(5); next != 5 {
		t.Errorf("after the last reuse the next ID is %#x, want it to wrap to 5", next)
	}
	sector := EmptyRecordSector()
	PutRecord(sector, RecordSize, &Record{Flags: RecordInUse, ID: later, UsedBlocks: 3})
	if got, err := GetRecord(sector, RecordSize); err != nil || got.ID != later || got.UsedBlocks != 3 {
		t.Errorf("record read back: %+v, %v; want ID %d", got, err, later)
	}
}
