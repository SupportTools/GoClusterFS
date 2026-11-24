package quota

import (
	"errors"
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/inode"
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

// Capacity is the number of records the table has, counting the unused record 0.
func (t Table) Capacity() uint32 {
	if t.Sectors < 2 {
		return 0
	}
	return uint32(min((t.Sectors-1)*RecordsPerSector, 1<<IndexBits))
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

// Open maps the table in the data blocks of quota inode num. The table's blocks are inline extents.
func Open(dev *storage.Device, inodes *inode.Table, num uint32, dataOffset, blockSize uint64) (Table, error) {
	qi, err := inodes.Read(num)
	if err != nil {
		return Table{}, err
	}
	if qi.HasExtentTree() {
		return Table{}, fmt.Errorf("quota inode %d maps its table through an extent tree, which this build does not read", num)
	}
	perBlock := blockSize / storage.SectorSize
	physical := map[uint64]uint64{}
	for _, x := range qi.InlineExtents {
		for i := uint64(0); i < x.Length; i++ {
			physical[x.LogicalBlock+i] = x.PhysicalBlock + i
		}
	}
	return Table{
		Dev:     dev,
		Sectors: uint64(len(physical)) * perBlock,
		At: func(sector uint64) (int64, error) {
			block, ok := physical[sector/perBlock]
			if !ok {
				return 0, fmt.Errorf("quota table sector %d is past the table's blocks", sector)
			}
			return int64(dataOffset + block*blockSize + sector%perBlock*storage.SectorSize), nil
		},
	}, nil
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

// Record returns the record of project id, which may belong to another project now: see Record.Owns.
func (t Table) Record(id uint32) (*Record, error) {
	if Index(id) == 0 || Index(id) >= t.Capacity() {
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
	if Index(id) == 0 || Index(id) >= t.Capacity() {
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

// SetUsage writes project id's usage and leaves the rest of its record as it is.
func (t Table) SetUsage(id uint32, blocks, inodes uint64) error {
	r, err := t.Record(id)
	if err != nil {
		return err
	}
	r.UsedBlocks, r.UsedInodes = blocks, inodes
	return t.PutRecord(id, r)
}

// FreeID returns the index of the first record from from on, wrapping to 1, that is not in use. The project that takes
// it gets the record's NextID.
func (t Table) FreeID(from uint32) (uint32, error) {
	capacity := t.Capacity()
	if capacity < 2 {
		return 0, ErrTableFull
	}
	if from == 0 || from >= capacity {
		from = 1
	}
	var sector []byte
	var read uint64
	for n, id := uint32(0), from; n < capacity-1; n, id = n+1, id+1 {
		if id >= capacity {
			id = 1
		}
		num, offset := RecordLocation(id)
		if sector == nil || num != read {
			var err error
			if sector, err = t.read(num); err != nil {
				return 0, err
			}
			read = num
		}
		r, err := GetRecord(sector, offset)
		if err != nil {
			return 0, err
		}
		if r.Flags&RecordInUse == 0 {
			return id, nil
		}
	}
	return 0, ErrTableFull
}
