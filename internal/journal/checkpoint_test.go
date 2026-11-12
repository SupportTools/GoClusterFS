package journal

import (
	"slices"
	"testing"
)

// commitIntents commits one transaction per entry and completes those marked true.
func commitIntents(t *testing.T, j *Journal, complete ...bool) []uint64 {
	t.Helper()
	var seqs []uint64
	for i, done := range complete {
		txn := j.Begin()
		if err := txn.LogInodeCreate(testInodePayload(uint64(100 + i))); err != nil {
			t.Fatal(err)
		}
		seq, err := j.Commit(txn)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if err := j.CompleteIntent(seq); err != nil {
				t.Fatal(err)
			}
		}
		seqs = append(seqs, seq)
	}
	return seqs
}

// replayedAfterCrash reopens the region as the next mount would and returns the transactions it replays.
func replayedAfterCrash(t *testing.T, j *Journal) []uint64 {
	t.Helper()
	region, err := OpenRegionForRecovery(j.region.dev, j.region.regionOffset, j.region.regionSize)
	if err != nil {
		t.Fatal(err)
	}
	var replayed []uint64
	if _, err := Recover(region, func(txn *RecoveredTxn) error {
		replayed = append(replayed, txn.Header.Seq)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return replayed
}

func newTestJournal(t *testing.T) *Journal {
	t.Helper()
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	t.Cleanup(cleanup)
	j, err := Init(dev, 0, DefaultJournalSlotSize, [16]byte{1}, 1, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestCheckpointKeepsIncompleteIntents(t *testing.T) {
	j := newTestJournal(t)
	seqs := commitIntents(t, j, true, false, true)
	if _, err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := replayedAfterCrash(t, j); !slices.Equal(got, seqs[1:2]) {
		t.Errorf("replayed %v after a checkpoint and a crash, want the incomplete intent %d", got, seqs[1])
	}
}

func TestCheckpointPassesCompletedIntents(t *testing.T) {
	j := newTestJournal(t)
	seqs := commitIntents(t, j, true, false)
	if err := j.CompleteIntent(seqs[1]); err != nil {
		t.Fatal(err)
	}
	if done, err := j.Checkpoint(); err != nil || !done {
		t.Fatalf("Checkpoint = %v, %v; want the head moved", done, err)
	}
	if got := replayedAfterCrash(t, j); len(got) > 0 {
		t.Errorf("replayed %v after checkpointing completed intents", got)
	}
}

func TestStopLeavesIncompleteIntentsForReplay(t *testing.T) {
	j := newTestJournal(t)
	j.Start()
	seqs := commitIntents(t, j, false)
	if err := j.Stop(); err != nil {
		t.Fatal(err)
	}
	if got := replayedAfterCrash(t, j); !slices.Equal(got, seqs) {
		t.Errorf("replayed %v after unmounting with an incomplete intent, want %v", got, seqs)
	}
}
