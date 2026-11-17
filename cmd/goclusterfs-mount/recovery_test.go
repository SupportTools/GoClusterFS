package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// dirtyJournal returns a device holding a journal with one committed,
// unreplayed transaction, as a crashed node leaves its slot, and room for a
// node registry after it.
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
	t.Cleanup(func() { _ = os.Remove(path) })
	if err := os.Truncate(path, int64(journal.DefaultJournalSlotSize)+1<<20); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	t.Cleanup(func() { _ = dev.Close() })
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
		result, err := recoverOwnJournal(dev, 0, journal.DefaultJournalSlotSize, handler, nil, 5)
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
	if _, err := recoverWithin(t, 5*time.Second, dev, handler); err == nil || calls != 1 {
		t.Fatalf("recoverOwnJournal = %v after %d tries; want one failed try", err, calls)
	}
}

func TestSkipRecoveryRefusesLossWhileNodesRun(t *testing.T) {
	const size = journal.DefaultJournalSlotSize
	const deadSlot = 5
	defer func(old time.Duration) { runningWatch = old }(runningWatch)
	runningWatch = 2 * node.HeartbeatInterval
	dev := dirtyJournal(t)
	registryAt := uint64(size)
	writeHeader := func(fill byte) {
		t.Helper()
		buf := storage.NewAlignedBuffer(storage.SectorSize)
		for i := range buf.Bytes() {
			buf.Bytes()[i] = fill
		}
		if _, err := dev.WriteAt(buf.Bytes(), 0); err != nil {
			t.Fatal(err)
		}
	}
	lossNow := func() string {
		t.Helper()
		loss, err := skipRecoveryLoss(node.NewRegistry(dev, registryAt), dev, 0, size, deadSlot)
		if err != nil {
			t.Fatal(err)
		}
		return loss
	}

	running := node.NewRegistry(dev, registryAt)
	runningSlot, err := running.Register(context.Background(), "running")
	if err != nil {
		t.Fatal(err)
	}
	if err := running.StartHeartbeat(); err != nil {
		t.Fatal(err)
	}
	if loss := lossNow(); !strings.Contains(loss, fmt.Sprintf("registry slot %d has a running node", runningSlot)) ||
		!strings.Contains(loss, "holds 1 committed transaction that discarding it drops") {
		t.Errorf("committed transaction while a node runs: loss %q", loss)
	}
	writeHeader(0xff)
	if loss := lossNow(); !strings.Contains(loss, "cannot be read") {
		t.Errorf("unreadable journal while a node runs: loss %q", loss)
	}
	writeHeader(0)
	if loss := lossNow(); loss != "" {
		t.Errorf("journal never initialized while a node runs: loss %q, want none", loss)
	}
	running.Stop()
	writeHeader(0xff)
	if loss := lossNow(); loss != "" {
		t.Errorf("unreadable journal with no node running: loss %q, want none", loss)
	}
}
