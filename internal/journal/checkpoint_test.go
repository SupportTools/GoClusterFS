package journal

import (
	"errors"
	"slices"
	"testing"
	"time"
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

// With the head pinned by a pending intent, other operations fill the ring until the journal is full, never past the intent.
func TestPendingIntentSurvivesFullRing(t *testing.T) {
	j := newTestJournal(t)
	commitIntents(t, j, slices.Repeat([]bool{true}, 100)...)
	if _, err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	pinned := commitIntents(t, j, false)[0]
	for n := 1; ; n++ {
		txn := j.Begin()
		if err := txn.LogInodeCreate(testInodePayload(uint64(n))); err != nil {
			t.Fatal(err)
		}
		seq, err := j.Commit(txn)
		if errors.Is(err, ErrJournalFull) {
			break
		}
		if err != nil {
			t.Fatalf("op %d: %v", n, err)
		}
		if err := j.CompleteIntent(seq); err != nil && !errors.Is(err, ErrJournalFull) {
			t.Fatalf("op %d: %v", n, err)
		}
		if _, err := j.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if head, tail := j.region.HeadOffset(), j.region.TailOffset(); head == tail {
			t.Fatalf("op %d: head == tail == %d with intent %d pending, which reads as an empty journal", n, head, pinned)
		}
	}
	if got := replayedAfterCrash(t, j); !slices.Contains(got, pinned) {
		t.Errorf("a full journal replayed %v, want the pending intent %d", got, pinned)
	}
}

// A crash with the tail at the very end of the data area must not hang the next mount's scan.
func TestRecoveryEndsWithTailAtEnd(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1024*1024)
	t.Cleanup(cleanup)
	j, err := Init(dev, 0, uint64(JournalDataOffset)+3*4096, [16]byte{1}, 1, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	commitIntents(t, j, true)
	if _, err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	pending := commitIntents(t, j, false)
	if tail := j.region.TailOffset(); tail != j.region.DataAreaSize() {
		t.Fatalf("tail %d, want the end of the %d-byte data area", tail, j.region.DataAreaSize())
	}

	done := make(chan []uint64, 1)
	go func() { done <- replayedAfterCrash(t, j) }()
	select {
	case got := <-done:
		if !slices.Equal(got, pending) {
			t.Errorf("replayed %v, want %v", got, pending)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("recovery scan still running after 10s with the tail at the end of the data area")
	}
}

// A tail that no record boundary reaches, from a corrupt pointer, must not keep the scan reading the ring forever.
func TestScanStopsAtUnreachableTail(t *testing.T) {
	j := newTestJournal(t)
	for j.region.Superblock().WrapCount == 0 {
		commitIntents(t, j, true)
		if _, err := j.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	pending := commitIntents(t, j, false, false)
	region, err := OpenRegionForRecovery(j.region.dev, j.region.regionOffset, j.region.regionSize)
	if err != nil {
		t.Fatal(err)
	}
	region.superblock.TailOffset += 512

	done := make(chan []uint64, 1)
	go func() {
		var replayed []uint64
		_ = scanJournal(region, func(txn *RecoveredTxn) error {
			replayed = append(replayed, txn.Header.Seq)
			return nil
		}, &RecoveryStats{}, nil, nil, false)
		done <- replayed
	}()
	select {
	case got := <-done:
		if !slices.Equal(got, pending) {
			t.Errorf("replayed %v, want each pending intent once: %v", got, pending)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("scan still running after 10s with an unreachable tail")
	}
}

// Replay that stops after the last record, which ends at the end of the data area, leaves head == tail, which reads as
// empty: a head of 0 would read as a full ring and replay the previous lap's records again.
func TestStoppedReplayAtEndLeavesJournalEmpty(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1024*1024)
	t.Cleanup(cleanup)
	j, err := Init(dev, 0, uint64(JournalDataOffset)+3*4096, [16]byte{1}, 1, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	commitIntents(t, j, true)
	if _, err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	pending := commitIntents(t, j, false)
	if tail := j.region.TailOffset(); tail != j.region.DataAreaSize() {
		t.Fatalf("tail %d, want the end of the %d-byte data area", tail, j.region.DataAreaSize())
	}

	region, err := OpenRegionForRecovery(dev, 0, j.region.regionSize)
	if err != nil {
		t.Fatal(err)
	}
	var replayed []uint64
	checks := 0
	lostLease := errors.New("lost the recovery lease")
	_, err = RecoverChecked(region, func(txn *RecoveredTxn) error {
		replayed = append(replayed, txn.Header.Seq)
		return nil
	}, func() error {
		if checks++; checks == 3 {
			return lostLease
		}
		return nil
	})
	if !errors.Is(err, lostLease) || !slices.Equal(replayed, pending) {
		t.Fatalf("first replay = %v, %v; want %v, then the lost lease", replayed, err, pending)
	}

	if again := replayedAfterCrash(t, j); len(again) > 0 {
		t.Errorf("replay after the stopped one replayed %v again", again)
	}
}
