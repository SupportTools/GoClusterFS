package quota

import (
	"errors"
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// ErrTableFull means every record the table's blocks hold is in use.
var ErrTableFull = errors.New("the quota table has no free project record")

// Table reads and writes the quota table's sectors. Writes need the quota inode's lock: each is a read-modify-write of
// one sector.
type Table struct {
	Dev *storage.Device
	// At maps a table sector, counting the header as 0, to its device offset.
	At func(sector uint64) (int64, error)
	// Sectors is how many sectors the table's blocks hold.
	Sectors uint64
}

// Capacity is the number of project IDs the table has records for, counting the unused ID 0.
func (t Table) Capacity() uint32 {
	if t.Sectors < 2 {
		return 0
	}
	return uint32(min((t.Sectors-1)*RecordsPerSector, 1<<32-1))
}

func (t Table) read(sector uint64) ([]byte, error) {
	at, err := t.At(sector)
	if err != nil {
		return nil, err
	}
	buf := storage.NewAlignedBuffer(storage.SectorSize)
	if _, err := t.Dev.ReadAt(buf.Bytes(), at); err != nil {
		return nil, fmt.Errorf("reading quota table sector %d: %w", sector, err)
	}
	return buf.Bytes(), nil
}

func (t Table) write(sector uint64, data []byte) error {
	at, err := t.At(sector)
	if err != nil {
		return err
	}
	if _, err := t.Dev.WriteAt(data, at); err != nil {
		return fmt.Errorf("writing quota table sector %d: %w", sector, err)
	}
	return nil
}

func (t Table) Header() (*Header, error) {
	sector, err := t.read(0)
	if err != nil {
		return nil, err
	}
	return DecodeHeader(sector)
}

func (t Table) PutHeader(h *Header) error {
	sector := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	EncodeHeader(h, sector)
	return t.write(0, sector)
}

// Record returns project id's record.
func (t Table) Record(id uint32) (*Record, error) {
	if id == 0 || id >= t.Capacity() {
		return nil, fmt.Errorf("project %d is outside the quota table's %d records", id, t.Capacity())
	}
	sectorNum, offset := RecordLocation(id)
	sector, err := t.read(sectorNum)
	if err != nil {
		return nil, err
	}
	return GetRecord(sector, offset)
}

// PutRecord writes project id's record and leaves its neighbours in the sector as they are.
func (t Table) PutRecord(id uint32, r *Record) error {
	if id == 0 || id >= t.Capacity() {
		return fmt.Errorf("project %d is outside the quota table's %d records", id, t.Capacity())
	}
	sectorNum, offset := RecordLocation(id)
	sector, err := t.read(sectorNum)
	if err != nil {
		return err
	}
	if err := verify(sector); err != nil {
		return err
	}
	PutRecord(sector, offset, r)
	return t.write(sectorNum, sector)
}

// FreeID returns the first project ID from from on, wrapping to 1, whose record is not in use.
func (t Table) FreeID(from uint32) (uint32, error) {
	capacity := t.Capacity()
	if capacity < 2 {
		return 0, ErrTableFull
	}
	if from == 0 || from >= capacity {
		from = 1
	}
	for n, id := uint32(0), from; n < capacity-1; n, id = n+1, id+1 {
		if id >= capacity {
			id = 1
		}
		r, err := t.Record(id)
		if err != nil {
			return 0, err
		}
		if r.Flags&RecordInUse == 0 {
			return id, nil
		}
	}
	return 0, ErrTableFull
}
