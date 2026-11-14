package journal

import (
	"slices"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func zeroJournalSector(t *testing.T, j *Journal, pos uint32) {
	t.Helper()
	buf := storage.GetBuffer(storage.SectorSize)
	defer storage.PutBuffer(buf)
	clear(buf.Bytes())
	if _, err := j.region.dev.WriteAt(buf.Bytes(), j.region.dataOffset(pos)); err != nil {
		t.Fatal(err)
	}
}

// Power fails while the last transaction's sectors are written, and its last sector never lands. Recovery replays the
// transactions before it and stops there.
func TestRecoverStopsAtTornLastTransaction(t *testing.T) {
	j := newTestJournal(t)
	seqs := commitIntents(t, j, false, false)

	txn := j.Begin()
	for i := range 200 {
		if err := txn.LogInodeCreate(testInodePayload(uint64(1000 + i))); err != nil {
			t.Fatal(err)
		}
	}
	start := j.region.TailOffset()
	if _, err := j.Commit(txn); err != nil {
		t.Fatal(err)
	}
	end := j.region.TailOffset()
	if end-start < 2*storage.SectorSize {
		t.Fatalf("the transaction takes %d bytes, want several sectors", end-start)
	}
	zeroJournalSector(t, j, end-storage.SectorSize)

	if got := replayedAfterCrash(t, j); !slices.Equal(got, seqs) {
		t.Errorf("replayed %v, want %v", got, seqs)
	}
}
