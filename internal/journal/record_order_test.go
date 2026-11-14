package journal

import (
	"slices"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// Transactions commit in another order than they began: replay finds each, since a record's seq is the order it was
// written in.
func TestRecoverReplaysCommitsOutOfBeginOrder(t *testing.T) {
	j := newTestJournal(t)
	first, second := j.Begin(), j.Begin()
	for i, txn := range []*Transaction{first, second} {
		if err := txn.LogInodeCreate(testInodePayload(uint64(100 + i))); err != nil {
			t.Fatal(err)
		}
	}
	var seqs []uint64
	for _, txn := range []*Transaction{second, first} {
		seq, err := j.Commit(txn)
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	if got := replayedAfterCrash(t, j); !slices.Equal(got, seqs) {
		t.Errorf("replayed %v, want %v", got, seqs)
	}
}

// The completion of an intent is lost, and the record in its place is the previous mount's completion of the intent
// with the same seq: replay must still replay the intent.
func TestRecoverIgnoresCompletionFromEarlierMount(t *testing.T) {
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
	seq := commitIntents(t, j, false)[0]
	pos := j.region.TailOffset()
	before := readSectorAt(t, j, pos)
	if err := j.CompleteIntent(seq); err != nil {
		t.Fatal(err)
	}
	writeSectorAt(t, j, pos, before)

	if got := replayedAfterCrash(t, j); !slices.Equal(got, []uint64{seq}) {
		t.Errorf("replayed %v, want intent %d, whose completion was lost", got, seq)
	}
}

// A journal an older build left dirty has version 1: its records are numbered in Begin order and its completions are
// IntentCompletions. Replay reads them as that build wrote them.
func TestRecoverReplaysVersion1Journal(t *testing.T) {
	j := newTestJournal(t)
	r := j.region
	r.superblock.Version = 1
	write := func(data []byte) {
		t.Helper()
		if _, err := r.WriteAt(data); err != nil {
			t.Fatal(err)
		}
	}
	for _, seq := range []uint64{3, 2, 4} {
		txn := NewTransaction(seq, [16]byte{1}, 1)
		if err := txn.LogInodeCreate(testInodePayload(seq)); err != nil {
			t.Fatal(err)
		}
		data, err := txn.Serialize()
		if err != nil {
			t.Fatal(err)
		}
		write(data)
	}
	completion := make([]byte, storage.SectorSize)
	copy(completion, NewIntentCompletion(2).Serialize())
	write(completion)
	if err := r.MarkDirty(); err != nil {
		t.Fatal(err)
	}

	if got := replayedAfterCrash(t, j); !slices.Equal(got, []uint64{3, 4}) {
		t.Errorf("replayed %v, want 3 and 4: 2 is complete", got)
	}
}

// The ring wraps, and the record a lost one should replace is a transaction of the same size from the previous lap,
// by the same mount: only its seq tells it apart.
func TestRecoverIgnoresSameSizeRecordFromEarlierLap(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	t.Cleanup(cleanup)
	cfg := DefaultConfig()
	cfg.CommitMode = CommitAsync
	j, err := Init(dev, 0, DefaultJournalSlotSize, [16]byte{1}, 1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	staleTxnAtTail := func() bool {
		header, err := DeserializeTxnHeader(readSectorAt(t, j, j.region.TailOffset())[:TxnHeaderSize])
		return j.region.superblock.WrapCount > 0 && err == nil && header.Type == TxnTypeCommit
	}
	for i := 0; !staleTxnAtTail(); i++ {
		if i == 5000 {
			t.Fatalf("no previous-lap transaction at the tail after %d commits", i)
		}
		// Two transactions, then their completions: the ring's odd sector count then leaves a transaction behind the
		// tail, where one record at a time would always leave a completion.
		for _, seq := range commitIntents(t, j, false, false) {
			if err := j.CompleteIntent(seq); err != nil {
				t.Fatal(err)
			}
		}
		if i%32 == 0 {
			if _, err := j.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		}
	}

	commitLosingRecord(t, j)
	if got := replayedAfterCrash(t, j); len(got) != 0 {
		t.Errorf("replayed %v, a transaction from the journal's previous lap", got)
	}
}
