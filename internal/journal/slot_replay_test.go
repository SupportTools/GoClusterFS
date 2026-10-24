package journal

import (
	"context"
	"errors"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/node"
)

const (
	slotJournalOffset = 512 << 10
	slotJournalSize   = 256 << 10
)

func TestReplayJournalEmptySlotRegion(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()

	info := &node.FailedNodeInfo{UUID: [16]byte{2}, JournalOffset: slotJournalOffset, JournalSize: slotJournalSize}
	result, err := ReplayJournal(context.Background(), rctx, info)
	if err != nil {
		t.Fatalf("ReplayJournal on a slot without a journal: %v", err)
	}
	if result.TxnReplayed != 0 {
		t.Errorf("replayed %d transactions from an empty region", result.TxnReplayed)
	}
}

func TestReplayJournalOfPreviousSlotOwner(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()

	previousOwner := [16]byte{1}
	j, err := Init(rctx.Dev, slotJournalOffset, slotJournalSize, previousOwner, 1, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	txn := j.Begin()
	if err := txn.LogInodeCreate(&InodeCreatePayload{InodeNum: 42, Mode: 0644}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Commit(txn); err != nil {
		t.Fatal(err)
	}

	crashedOwner := &node.FailedNodeInfo{UUID: [16]byte{2}, JournalOffset: slotJournalOffset, JournalSize: slotJournalSize}
	result, err := ReplayJournal(context.Background(), rctx, crashedOwner)
	if err != nil {
		t.Fatalf("ReplayJournal: %v", err)
	}
	if result.TxnReplayed != 1 {
		t.Errorf("replayed %d transactions, want 1", result.TxnReplayed)
	}
	ino, err := rctx.InodeTbl.Read(42)
	if err != nil || ino.Magic != inode.InodeMagic {
		t.Errorf("inode 42 not recreated from the previous owner's journal: %v", err)
	}
}

func TestRecoverResumesInterruptedReplay(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region, err := InitRegion(dev, 0, DefaultJournalSlotSize, [16]byte{1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := region.MarkDirty(); err != nil {
		t.Fatal(err)
	}
	txn := NewTransaction(region.NextSeq(), [16]byte{1}, 1)
	if err := txn.LogInodeCreate(&InodeCreatePayload{InodeNum: 7}); err != nil {
		t.Fatal(err)
	}
	data, _ := txn.Serialize()
	if _, err := region.WriteAt(data); err != nil {
		t.Fatal(err)
	}

	if _, err := Recover(region, func(*RecoveredTxn) error { return errors.New("device closed") }); err == nil {
		t.Fatal("first replay: want the handler error")
	}
	replayed := 0
	if _, err := Recover(region, func(*RecoveredTxn) error { replayed++; return nil }); err != nil {
		t.Fatalf("resumed replay: %v", err)
	}
	if replayed != 1 || region.State() != JournalStateClean {
		t.Errorf("resumed replay applied %d transactions and left the journal %v, want 1 and clean", replayed, region.State())
	}
}

func journalWithTxns(t *testing.T, n int) (*Region, func()) {
	t.Helper()
	dev, cleanup := createTestDevice(t, 10<<20)
	region, err := InitRegion(dev, 0, DefaultJournalSlotSize, [16]byte{1}, 1)
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	if err := region.MarkDirty(); err != nil {
		cleanup()
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		txn := NewTransaction(region.NextSeq(), [16]byte{1}, 1)
		if err := txn.LogInodeCreate(&InodeCreatePayload{InodeNum: uint64(10 + i)}); err != nil {
			t.Fatal(err)
		}
		data, _ := txn.Serialize()
		if _, err := region.WriteAt(data); err != nil {
			cleanup()
			t.Fatal(err)
		}
	}
	return region, cleanup
}

func TestRecoverDoesNotReapplyFinishedTransactions(t *testing.T) {
	region, cleanup := journalWithTxns(t, 3)
	defer cleanup()

	var applied []uint64
	failSecond := func(txn *RecoveredTxn) error {
		if len(applied) == 1 {
			return errors.New("replaying node killed")
		}
		applied = append(applied, txn.Header.Seq)
		return nil
	}
	if _, err := Recover(region, failSecond); err == nil {
		t.Fatal("interrupted replay: want an error")
	}
	if _, err := Recover(region, func(txn *RecoveredTxn) error {
		applied = append(applied, txn.Header.Seq)
		return nil
	}); err != nil {
		t.Fatalf("resumed replay: %v", err)
	}
	if len(applied) != 3 || applied[0] == applied[1] {
		t.Errorf("applied transactions %v, want each of the 3 once", applied)
	}
}

func TestRecoverCheckedStopsWhenLeaseIsLost(t *testing.T) {
	region, cleanup := journalWithTxns(t, 2)
	defer cleanup()

	checks := 0
	lost := errors.New("lease lost")
	_, err := RecoverChecked(region, func(*RecoveredTxn) error { return nil }, func() error {
		checks++
		if checks > 2 {
			return lost
		}
		return nil
	})
	if !errors.Is(err, lost) {
		t.Fatalf("err = %v, want the lost lease", err)
	}
	if region.State() != JournalStateRecovering {
		t.Errorf("journal is %v after losing the lease before reset, want Recovering", region.State())
	}
}

func TestRecoverCheckedLeavesHeaderAloneAfterLosingLease(t *testing.T) {
	region, cleanup := journalWithTxns(t, 2)
	defer cleanup()
	head := region.HeadOffset()

	checks := 0
	_, err := RecoverChecked(region, func(*RecoveredTxn) error { return nil }, func() error {
		checks++
		if checks == 2 {
			return errors.New("lease lost")
		}
		return nil
	})
	if err == nil {
		t.Fatal("want an error after losing the lease")
	}
	onDisk, err := OpenRegionForRecovery(region.dev, region.regionOffset, region.regionSize)
	if err != nil {
		t.Fatal(err)
	}
	if got := onDisk.HeadOffset(); got != head {
		t.Errorf("head moved to %d after the lease was lost, want it left at %d", got, head)
	}
}
