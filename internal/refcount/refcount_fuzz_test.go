package refcount

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	fuzzSectors  = 2
	fuzzCapacity = fuzzSectors * EntriesPerSector
	fuzzTotal    = fuzzCapacity + 40
)

// refModel is what the refcount region should hold: the entries of the
// blocks it covers, and a sector whose checksum fails, which every operation
// that reads it must refuse.
type refModel struct {
	entries   [fuzzCapacity]Entry
	badSector int // -1 when every sector reads
}

func (m *refModel) entry(block uint64) (Entry, error) {
	switch {
	case block >= fuzzTotal:
		return Entry{}, ErrBlockOutOfRange
	case block >= fuzzCapacity:
		return Entry{}, nil
	case int(block/EntriesPerSector) == m.badSector:
		return Entry{}, ErrInvalidChecksum
	}
	return m.entries[block], nil
}

// update applies fn to blocks in order on a copy and keeps the copy only if
// every block succeeds, as Region.updateLocked does.
func (m *refModel) update(blocks []uint64, fn func(i int, e *Entry) (bool, error)) error {
	for _, b := range blocks {
		if b >= fuzzTotal {
			return ErrBlockOutOfRange
		}
	}
	next := m.entries
	for i, b := range blocks {
		if b >= fuzzCapacity {
			e := Entry{}
			if changed, err := fn(i, &e); err != nil {
				return err
			} else if changed && e != (Entry{}) {
				return ErrBlockPastRegion
			}
			continue
		}
		if int(b/EntriesPerSector) == m.badSector {
			return ErrInvalidChecksum
		}
		if _, err := fn(i, &next[b]); err != nil {
			return err
		}
	}
	m.entries = next
	return nil
}

// FuzzRefcountSector runs a sequence of refcount operations over a region
// with one sector the fuzzer wrote and checks each result against refModel.
func FuzzRefcountSector(f *testing.F) {
	sector := make([]byte, SectorSize)
	for i := uint64(0); i < 20; i++ {
		encodeEntry(sector, i*7, Entry{RefCount: uint32(i % 4), Flags: EntryFlags(i % 3)})
	}
	encodeEntry(sector, 3, Entry{RefCount: 0xFFFFFFFF})
	f.Add(uint8(0), true, sector, []byte{0, 3, 1, 3, 2, 21, 5, 3, 21, 6, 0, 7, 8, 9})
	f.Add(uint8(1), false, sector, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	f.Add(uint8(0), false, make([]byte, SectorSize), []byte{2, 0, 2, 0, 7, 1, 7, 1, 4, 2})
	f.Add(uint8(1), true, []byte{0xff}, []byte{6, 1, 1, 8, 1, 1, 9, 9})

	dir := f.TempDir()
	f.Fuzz(func(t *testing.T, which uint8, fixChecksum bool, data, ops []byte) {
		s := int(which) % fuzzSectors
		raw := make([]byte, SectorSize)
		copy(raw, data)
		if fixChecksum {
			binary.LittleEndian.PutUint32(raw[SectorSize-ChecksumSize:], storage.CRC32C(raw[:SectorSize-ChecksumSize]))
		}
		model := refModel{badSector: -1}
		stored := binary.LittleEndian.Uint32(raw[SectorSize-ChecksumSize:])
		if stored != storage.CRC32C(raw[:SectorSize-ChecksumSize]) && !(stored == 0 && isZero(raw[:SectorSize-ChecksumSize])) {
			model.badSector = s
		} else {
			for i := range EntriesPerSector {
				model.entries[s*EntriesPerSector+i] = decodeEntry(raw, uint64(i))
			}
		}

		dev := fuzzDevice(t, dir)
		buf := storage.NewAlignedBuffer(SectorSize)
		copy(buf.Bytes(), raw)
		if _, err := dev.WriteAt(buf.Bytes(), int64(s)*SectorSize); err != nil {
			t.Fatal(err)
		}
		r := NewRegion(dev, 0, fuzzSectors*SectorSize, fuzzTotal, nil)
		runRefOps(t, r, &model, s, ops)
		checkRefSectors(t, dev, &model, raw)
	})
}

func runRefOps(t *testing.T, r *Region, model *refModel, s int, ops []byte) {
	ctx := context.Background()
	next := func() byte {
		if len(ops) == 0 {
			return 0
		}
		b := ops[0]
		ops = ops[1:]
		return b
	}
	block := func() uint64 {
		switch b := next(); b % 8 {
		case 0:
			return fuzzTotal + uint64(b)
		case 1:
			return fuzzCapacity + uint64(b)%(fuzzTotal-fuzzCapacity)
		case 2:
			return uint64((s+1)%fuzzSectors)*EntriesPerSector + uint64(next())%EntriesPerSector
		default:
			return uint64(s)*EntriesPerSector + uint64(next())*7%EntriesPerSector
		}
	}
	same := func(op string, got, want error) {
		t.Helper()
		if (got == nil) != (want == nil) || want != nil && !errors.Is(got, want) {
			t.Fatalf("%s: error %v, want %v", op, got, want)
		}
	}

	for len(ops) > 0 {
		switch op := next(); op % 10 {
		case 0:
			b := block()
			got, err := r.Get(ctx, b)
			want, wantErr := model.entry(b)
			same("Get", err, wantErr)
			if err == nil && got != want {
				t.Fatalf("Get(%d) = %+v, want %+v", b, got, want)
			}
		case 1, 2:
			b := block()
			var got uint32
			var err error
			fn := func(_ int, e *Entry) (bool, error) {
				if e.RefCount == 0xFFFFFFFF {
					return false, ErrRefCountOverflow
				}
				e.RefCount++
				return true, nil
			}
			name := "Increment"
			if op%10 == 2 {
				name = "Decrement"
				fn = func(_ int, e *Entry) (bool, error) {
					if e.RefCount == 0 {
						return false, ErrRefCountUnderflow
					}
					e.RefCount--
					return true, nil
				}
				got, err = r.Decrement(ctx, b)
			} else {
				got, err = r.Increment(ctx, b)
			}
			wantErr := model.update([]uint64{b}, fn)
			same(name, err, wantErr)
			if want, _ := model.entry(b); err == nil && got != want.RefCount {
				t.Fatalf("%s(%d) = %d, want %d", name, b, got, want.RefCount)
			}
		case 3:
			b, count, flags := block(), uint32(next())<<24|uint32(next()), EntryFlags(next())
			err := r.Set(ctx, b, count, flags)
			same("Set", err, model.update([]uint64{b}, func(_ int, e *Entry) (bool, error) {
				*e = Entry{RefCount: count, Flags: flags}
				return true, nil
			}))
		case 4:
			b, flags := block(), EntryFlags(next())
			err := r.SetFlags(ctx, b, flags)
			same("SetFlags", err, model.update([]uint64{b}, func(_ int, e *Entry) (bool, error) {
				e.Flags = flags
				return true, nil
			}))
		case 5:
			blocks := []uint64{block(), block()}
			_, err := r.Share(ctx, blocks)
			same("Share", err, model.update(blocks, func(_ int, e *Entry) (bool, error) {
				if e.RefCount == 0xFFFFFFFF {
					return false, ErrRefCountOverflow
				}
				e.RefCount = max(e.RefCount, 1) + 1
				return true, nil
			}))
		case 6:
			blocks := []uint64{block(), block()}
			free, err := r.Release(ctx, blocks)
			var wantFree []uint64
			same("Release", err, model.update(blocks, func(i int, e *Entry) (bool, error) {
				if e.RefCount > 1 {
					e.RefCount--
					return true, nil
				}
				wantFree = append(wantFree, blocks[i])
				changed := *e != (Entry{})
				*e = Entry{}
				return changed, nil
			}))
			if err == nil && !slicesEqual(free, wantFree) {
				t.Fatalf("Release(%v) freed %v, want %v", blocks, free, wantFree)
			}
		case 7:
			blocks := []uint64{block(), block()}
			_, err := r.IncrementBatch(ctx, blocks)
			same("IncrementBatch", err, model.update(blocks, func(_ int, e *Entry) (bool, error) {
				if e.RefCount == 0xFFFFFFFF {
					return false, ErrRefCountOverflow
				}
				e.RefCount++
				return true, nil
			}))
		case 8:
			blocks := []uint64{block(), block()}
			uncounted := r.Uncounted(blocks)
			for _, b := range blocks {
				e, err := model.entry(b)
				want := b < fuzzTotal && err == nil && e == (Entry{})
				if uncounted(b) != want {
					t.Fatalf("Uncounted(%d) = %v, want %v", b, !want, want)
				}
			}
		case 9:
			stats, err := r.Scan(ctx)
			var wantErr error
			if model.badSector >= 0 {
				wantErr = ErrInvalidChecksum
			}
			same("Scan", err, wantErr)
			if err == nil {
				checkScan(t, stats, model)
			}
		}
	}
}

func checkScan(t *testing.T, stats ScanStats, model *refModel) {
	want := ScanStats{TotalBlocks: fuzzTotal, FreeBlocks: fuzzTotal - fuzzCapacity}
	for _, e := range model.entries {
		switch e.RefCount {
		case 0:
			want.FreeBlocks++
		case 1:
			want.ExclusiveBlocks++
		default:
			want.SharedBlocks++
			want.TotalSharedRefs += uint64(e.RefCount)
		}
		if e.Flags&FlagSnapshot != 0 {
			want.SnapshotBlocks++
		}
	}
	if stats != want {
		t.Fatalf("Scan = %+v, want %+v", stats, want)
	}
}

// checkRefSectors checks the sectors on disk: the bad one exactly as written,
// the others holding the model's entries.
func checkRefSectors(t *testing.T, dev *storage.Device, model *refModel, raw []byte) {
	buf := storage.NewAlignedBuffer(SectorSize)
	for s := range fuzzSectors {
		if _, err := dev.ReadAt(buf.Bytes(), int64(s)*SectorSize); err != nil {
			t.Fatal(err)
		}
		if s == model.badSector {
			if !bytes.Equal(buf.Bytes(), raw) {
				t.Fatalf("sector %d failed its checksum, yet an operation rewrote it", s)
			}
			continue
		}
		for i := range EntriesPerSector {
			if got, want := decodeEntry(buf.Bytes(), uint64(i)), model.entries[s*EntriesPerSector+i]; got != want {
				t.Fatalf("block %d holds %+v on disk, want %+v", s*EntriesPerSector+i, got, want)
			}
		}
	}
}

func isZero(b []byte) bool {
	return bytes.Count(b, []byte{0}) == len(b)
}

func slicesEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func fuzzDevice(t *testing.T, dir string) *storage.Device {
	f, err := os.CreateTemp(dir, "refcount-*.img")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	t.Cleanup(func() { _ = os.Remove(path) })
	if err := f.Truncate(fuzzSectors * SectorSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	return dev
}
