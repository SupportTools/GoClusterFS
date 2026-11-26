package journal

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fillWithOpenIntents commits intents it leaves open until the ring refuses another, and returns them oldest first.
func fillWithOpenIntents(t *testing.T, j *Journal) []uint64 {
	t.Helper()
	var open []uint64
	for n := 1; ; n++ {
		seq, err := j.Commit(createTxn(t, j, n))
		if errors.Is(err, ErrJournalFull) {
			return open
		}
		if err != nil {
			t.Fatalf("op %d: %v", n, err)
		}
		open = append(open, seq)
	}
}

func createTxn(t *testing.T, j *Journal, n int) *Transaction {
	t.Helper()
	txn := j.Begin()
	if err := txn.LogInodeCreate(testInodePayload(uint64(n))); err != nil {
		t.Fatal(err)
	}
	return txn
}

// An operation that finds the ring full of open intents waits for them to complete, then commits.
func TestAdmitWaitsForOpenIntentsToComplete(t *testing.T) {
	j := newTestJournal(t)
	open := fillWithOpenIntents(t, j)
	txn := createTxn(t, j, 0)
	committed := make(chan error, 1)
	go func() {
		a, err := j.Admit(context.Background(), txn.TotalLength())
		if err == nil {
			_, err = j.CommitAdmitted(a, txn)
		}
		committed <- err
	}()
	select {
	case err := <-committed:
		t.Fatalf("committed into a ring full of open intents: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	for _, seq := range open {
		if err := j.CompleteIntent(seq); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-committed:
		if err != nil {
			t.Fatalf("commit after the open intents completed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("still waiting 10s after every open intent completed")
	}
}

// A wait for room ends with its context.
func TestAdmitEndsWithItsContext(t *testing.T) {
	j := newTestJournal(t)
	fillWithOpenIntents(t, j)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := j.Admit(ctx, 512); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Admit on a full ring = %v, want the context's error", err)
	}
}

// Room an admission set aside stays its own: commits that cannot wait fill the rest of the ring, and the admitted
// record still commits.
func TestAdmittedRoomOutlastsOtherCommits(t *testing.T) {
	j := newTestJournal(t)
	txn := createTxn(t, j, 0)
	a, err := j.Admit(context.Background(), txn.TotalLength())
	if err != nil {
		t.Fatal(err)
	}
	fillWithOpenIntents(t, j)
	if _, err := j.CommitAdmitted(a, txn); err != nil {
		t.Fatalf("admitted commit after other commits filled the ring: %v", err)
	}
}

// Commits that cannot wait leave a part of the ring to records that let the head move, such as the reserve's renewal
// of an old batch, so the head can still move once they filled the rest.
func TestHeadMovingCommitFindsRoomInAFullRing(t *testing.T) {
	j := newTestJournal(t)
	fillWithOpenIntents(t, j)
	if _, err := j.CommitMovingHead(createTxn(t, j, 0)); err != nil {
		t.Fatalf("renewal after other commits filled the ring: %v", err)
	}
}
