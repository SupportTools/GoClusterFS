package fsck_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// metadataHash covers the node registry and every journal, which inspection must not change.
func metadataHash(t *testing.T, dev *storage.Device, sb *superblock.Superblock) [32]byte {
	t.Helper()
	h := sha256.New()
	for _, area := range [][2]uint64{
		{sb.NodeRegistryOffset, node.MaxNodes * node.SlotSize},
		{sb.JournalRegionOffset, sb.JournalRegionSize},
	} {
		buf := storage.NewAlignedBuffer(int(area[1])).Bytes()
		if _, err := dev.ReadAt(buf, int64(area[0])); err != nil {
			t.Fatal(err)
		}
		h.Write(buf)
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

func TestInspectSlot(t *testing.T) {
	t.Cleanup(fsck.SetHeartbeatWatch(2*node.HeartbeatInterval, 2*node.HeartbeatInterval))
	var sb *superblock.Superblock
	dev := formattedDevice(t, func(s *superblock.Superblock) { sb = s })
	var slots [3]int
	for i := range slots {
		slot, err := node.NewRegistry(dev, sb.NodeRegistryOffset).Register(context.Background(), "gone")
		if err != nil {
			t.Fatal(err)
		}
		slots[i] = slot
	}
	for _, slot := range slots {
		setHeartbeat(t, dev, sb, slot, time.Now().Add(-time.Hour))
	}
	fresh, dirty, damaged := slots[0], slots[1], slots[2]

	j, err := journal.Init(dev, sb.JournalRegionOffset+uint64(dirty)*sb.JournalSlotSize(), sb.JournalSlotSize(),
		[16]byte{1}, 1, journal.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, log := range []func(*journal.Transaction) error{
		func(txn *journal.Transaction) error {
			return txn.LogExtentAdd(&journal.ExtentAddPayload{InodeNum: 12, DiskBlock: 300, Length: 3})
		},
		func(txn *journal.Transaction) error {
			return txn.LogInodeDelete(&journal.InodeDeletePayload{InodeNum: 40})
		},
	} {
		txn := j.Begin()
		if err := log(txn); err != nil {
			t.Fatal(err)
		}
		if _, err := j.Commit(txn); err != nil {
			t.Fatal(err)
		}
	}
	damageJournal(t, dev, sb, damaged)
	before := metadataHash(t, dev, sb)

	checker := newChecker(dev)
	in, err := checker.InspectSlot(fresh)
	if err != nil || in.Header != nil || in.HeaderErr != nil || len(in.Pending) != 0 {
		t.Errorf("slot %d with an unused journal: header %v, error %v, %d pending, %v", fresh, in.Header, in.HeaderErr, len(in.Pending), err)
	}

	in, err = checker.InspectSlot(dirty)
	if err != nil || in.Header == nil || in.Status.Slot == nil {
		t.Fatalf("slot %d with a dirty journal: journal %q, error %v, %v", dirty, in.Status.Journal, in.HeaderErr, err)
	}
	got := fmt.Sprint(len(in.Pending))
	for _, p := range in.Pending {
		got += fmt.Sprintf(" | %v inodes %v blocks %v", p.Ops, p.Inodes, p.Blocks)
	}
	if want := "2 | [ExtentAdd] inodes [12] blocks [{300 3}] | [InodeDelete] inodes [40] blocks []"; got != want {
		t.Errorf("pending transactions:\n%s\nwant\n%s", got, want)
	}
	if in.ScanErr != nil {
		t.Errorf("scan error on a whole journal: %v", in.ScanErr)
	}

	in, err = checker.InspectSlot(damaged)
	if err != nil || !errors.Is(in.HeaderErr, journal.ErrJournalChecksumMismatch) || len(in.Pending) != 0 {
		t.Errorf("slot %d with a damaged journal header: %v, %d pending, %v", damaged, in.HeaderErr, len(in.Pending), err)
	}

	if _, err := checker.InspectSlot(node.MaxNodes); !errors.Is(err, fsck.ErrNoSuchSlot) {
		t.Errorf("InspectSlot(%d): %v, want ErrNoSuchSlot", node.MaxNodes, err)
	}
	if metadataHash(t, dev, sb) != before {
		t.Error("inspection changed the node registry or a journal")
	}
}
