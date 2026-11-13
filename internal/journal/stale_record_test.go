package journal

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func readSectorAt(t *testing.T, j *Journal, pos uint32) []byte {
	t.Helper()
	buf := storage.GetBuffer(storage.SectorSize)
	defer storage.PutBuffer(buf)
	if _, err := j.region.dev.ReadAt(buf.Bytes(), j.region.dataOffset(pos)); err != nil {
		t.Fatal(err)
	}
	return slices.Clone(buf.Bytes())
}

func writeSectorAt(t *testing.T, j *Journal, pos uint32, data []byte) {
	t.Helper()
	buf := storage.GetBuffer(storage.SectorSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), data)
	if _, err := j.region.dev.WriteAt(buf.Bytes(), j.region.dataOffset(pos)); err != nil {
		t.Fatal(err)
	}
}

// commitLosingRecord commits one transaction, then puts back what its first sector held before, as a device that
// wrote the superblock but lost the record would.
func commitLosingRecord(t *testing.T, j *Journal) uint64 {
	t.Helper()
	pos := j.region.TailOffset()
	before := readSectorAt(t, j, pos)
	seq := commitIntents(t, j, false)[0]
	writeSectorAt(t, j, pos, before)
	return seq
}

// Each mount starts its journal over at offset 0, so the previous mount's records stay on disk. Power fails after
// the superblock covering the new mount's first record landed, but not the record: recovery must not replay the
// previous mount's record in its place.
func TestRecoverIgnoresRecordFromEarlierMount(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	t.Cleanup(cleanup)
	earlier, err := Init(dev, 0, DefaultJournalSlotSize, [16]byte{1}, 1, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	commitIntents(t, earlier, true)

	j, err := Init(dev, 0, DefaultJournalSlotSize, [16]byte{1}, 2, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	commitLosingRecord(t, j)
	if got := replayedAfterCrash(t, j); len(got) != 0 {
		t.Errorf("replayed %v, records the previous mount wrote", got)
	}
}

// Once the journal wraps, the sector after the tail holds a record from the previous lap. Power fails after the
// superblock covering the next record landed, but not the record: recovery must not replay the old one.
func TestRecoverIgnoresRecordFromEarlierLap(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	t.Cleanup(cleanup)
	cfg := DefaultConfig()
	cfg.CommitMode = CommitAsync
	j, err := Init(dev, 0, DefaultJournalSlotSize, [16]byte{1}, 1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	twoSectorEntries := 1
	for ; ; twoSectorEntries++ {
		probe := NewTransaction(1, [16]byte{1}, 1)
		for k := range twoSectorEntries {
			if err := probe.LogInodeCreate(testInodePayload(uint64(100 + k))); err != nil {
				t.Fatal(err)
			}
		}
		if data, err := probe.Serialize(); err != nil || len(data) > storage.SectorSize {
			break
		}
	}
	for i := 0; j.region.superblock.WrapCount == 0 || binary.LittleEndian.Uint32(readSectorAt(t, j, j.region.TailOffset())) != TxnMagic; i++ {
		if i == 10000 {
			t.Fatalf("no previous-lap record after the tail after %d commits: wrap count %d, tail %d of %d",
				i, j.region.superblock.WrapCount, j.region.TailOffset(), j.region.DataAreaSize())
		}
		txn := j.Begin()
		// Once wrapped, a two-sector transaction moves records off the sectors that held completions in the last lap.
		entries := 1
		if j.region.superblock.WrapCount > 0 {
			entries = twoSectorEntries
		}
		for k := range entries {
			if err := txn.LogInodeCreate(testInodePayload(uint64(100 + k))); err != nil {
				t.Fatal(err)
			}
		}
		seq, err := j.Commit(txn)
		if err != nil {
			t.Fatal(err)
		}
		if err := j.CompleteIntent(seq); err != nil {
			t.Fatal(err)
		}
		if _, err := j.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	commitLosingRecord(t, j)
	if got := replayedAfterCrash(t, j); len(got) != 0 {
		t.Errorf("replayed %v, records from the journal's previous lap", got)
	}
}
