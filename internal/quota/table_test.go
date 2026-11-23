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
