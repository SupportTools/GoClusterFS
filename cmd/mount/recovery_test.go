package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// dirtyJournal returns a device holding a journal with one committed,
// unreplayed transaction, as a crashed node leaves its slot.
func dirtyJournal(t *testing.T) *storage.Device {
	t.Helper()
	dir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	path := filepath.Join(dir, fmt.Sprintf("own-journal-%d.img", time.Now().UnixNano()))
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(path) })
	if err := os.Truncate(path, int64(journal.DefaultJournalSlotSize)); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	t.Cleanup(func() { dev.Close() })
	j, err := journal.Init(dev, 0, journal.DefaultJournalSlotSize, [16]byte{1}, 1, journal.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	txn := j.Begin()
	if err := txn.LogInodeCreate(&journal.InodeCreatePayload{InodeNum: 42}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Commit(txn); err != nil {
		t.Fatal(err)
	}
	return dev
}

// recoverWithin runs recoverOwnJournal and fails the test if it is still
// running after d.
func recoverWithin(t *testing.T, d time.Duration, dev *storage.Device, handler journal.RecoveryHandler) (*journal.ScanRecoveryResult, error) {
	t.Helper()
	type outcome struct {
		result *journal.ScanRecoveryResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := recoverOwnJournal(dev, 0, journal.DefaultJournalSlotSize, handler, 5)
		done <- outcome{result, err}
	}()
	select {
	case o := <-done:
		return o.result, o.err
	case <-time.After(d):
		t.Fatalf("recoverOwnJournal still running after %v", d)
		return nil, nil
	}
}

func TestOwnReplayRetriesWhileAnotherNodeHoldsALock(t *testing.T) {
	defer func(d time.Duration) { ownReplayRetry = d }(ownReplayRetry)
	ownReplayRetry = 10 * time.Millisecond
	dev := dirtyJournal(t)

	calls := 0
	handler := func(*journal.RecoveredTxn) error {
		calls++
		if calls == 1 {
			return fmt.Errorf("failed to acquire inode lock: %w", lock.ErrLockTimeout)
		}
		return nil
	}
	result, err := recoverWithin(t, 5*time.Second, dev, handler)
	if err != nil || result == nil || !result.WasNeeded || calls != 2 {
		t.Fatalf("recoverOwnJournal = %+v, %v after %d tries; want the transaction replayed on the second", result, err, calls)
	}
}

func TestOwnReplayStopsAtARecordThatDoesNotDecode(t *testing.T) {
	defer func(d time.Duration) { ownReplayRetry = d }(ownReplayRetry)
	ownReplayRetry = time.Minute
	dev := dirtyJournal(t)

	calls := 0
	handler := func(*journal.RecoveredTxn) error {
		calls++
		return errors.New("failed to apply entry op=InodeCreate: inode create payload too short")
	}
	_, err := recoverWithin(t, 5*time.Second, dev, handler)
	if err == nil || calls != 1 {
		t.Fatalf("recoverOwnJournal = %v after %d tries; want one failed try", err, calls)
	}
	if !strings.Contains(err.Error(), "-skip-recovery=5") {
		t.Errorf("error %q does not tell the operator how to discard the journal", err)
	}
}
