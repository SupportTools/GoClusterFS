package journal

import (
	"context"
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
