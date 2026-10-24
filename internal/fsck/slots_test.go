package fsck_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

func TestReleaseSlot(t *testing.T) {
	t.Cleanup(fsck.SetHeartbeatWatch(2*node.HeartbeatInterval, 2*node.HeartbeatInterval))
	var sb *superblock.Superblock
	dev := formattedDevice(t, func(s *superblock.Superblock) { sb = s })
	ctx := context.Background()
	checker := func() *fsck.Checker { return fsck.New(dev, fsck.Options{Output: &bytes.Buffer{}}) }
	refused := func(slot int, reason string) {
		t.Helper()
		if _, err := checker().PlanRelease(slot); err == nil || !strings.Contains(err.Error(), reason) {
			t.Fatalf("PlanRelease(%d) error = %v, want one containing %q", slot, err, reason)
		}
	}

	gone := node.NewRegistry(dev, sb.NodeRegistryOffset)
	goneSlot, err := gone.Register(ctx, "gone")
	if err != nil {
		t.Fatal(err)
	}
	if err := gone.StartHeartbeat(); err != nil {
		t.Fatal(err)
	}
	live := node.NewRegistry(dev, sb.NodeRegistryOffset)
	if _, err := live.Register(ctx, "live"); err != nil {
		t.Fatal(err)
	}
	if err := live.StartHeartbeat(); err != nil {
		t.Fatal(err)
	}
	gone.Stop()
	refused(goneSlot, "mounted by node 1 (live)")
	if err := live.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
	refused(goneSlot, "node 0 (gone) may still be alive")

	editSlot(t, dev, sb, goneSlot, func(slot []byte) {
		binary.LittleEndian.PutUint32(slot[4:8], uint32(node.NodeStateDead))
		binary.LittleEndian.PutUint64(slot[172:180], uint64(time.Now().Unix()))
	})
	refused(goneSlot, "may still be alive")

	editSlot(t, dev, sb, goneSlot, func(slot []byte) {
		binary.LittleEndian.PutUint64(slot[172:180], uint64(time.Now().Add(-time.Hour).Unix()))
	})
	journalAt := int64(sb.JournalRegionOffset + uint64(goneSlot)*sb.JournalSlotSize())
	header := journal.NewJournalSuperblock([16]byte{1}, 1, 1<<20)
	header.State = journal.JournalStateDirty
	damaged := header.Serialize()
	damaged[100] ^= 0xFF
	writeSector(t, dev, journalAt, damaged)

	c := checker()
	plan, err := c.PlanRelease(goneSlot)
	if err != nil {
		t.Fatalf("PlanRelease of an expired slot with a damaged journal: %v", err)
	}
	if !strings.HasPrefix(plan.Slot.Journal, "damaged: ") || !strings.HasPrefix(plan.Discards, "the whole journal, unread") {
		t.Errorf("plan reports journal %q, discards %q", plan.Slot.Journal, plan.Discards)
	}
	if err := c.Release(plan); err != nil {
		t.Fatalf("Release: %v", err)
	}

	slots, err := checker().Slots()
	if err != nil {
		t.Fatal(err)
	}
	if s := slots[goneSlot]; s.InUse() || !s.JournalFresh {
		t.Errorf("after release slot %d is in use=%v with journal %q", goneSlot, s.InUse(), s.Journal)
	}
	replacement := node.NewRegistry(dev, sb.NodeRegistryOffset)
	if got, err := replacement.Register(ctx, "replacement"); err != nil || got != goneSlot {
		t.Fatalf("a new node registered in slot %d (%v), want released slot %d", got, err, goneSlot)
	}
	if result, err := journal.ScanAndRecover(dev, uint64(journalAt), sb.JournalSlotSize(), nil); result != nil || err != nil {
		t.Errorf("mount recovery of the released journal = %+v, %v; want nothing to recover", result, err)
	}
}

// A node that registers between the plan and the release stops the release
// before it writes anything.
func TestReleaseSlotStopsWhenRegistryChanges(t *testing.T) {
	t.Cleanup(fsck.SetHeartbeatWatch(2*node.HeartbeatInterval, 2*node.HeartbeatInterval))
	var sb *superblock.Superblock
	dev := formattedDevice(t, func(s *superblock.Superblock) { sb = s })
	ctx := context.Background()

	gone := node.NewRegistry(dev, sb.NodeRegistryOffset)
	goneSlot, err := gone.Register(ctx, "gone")
	if err != nil {
		t.Fatal(err)
	}
	editSlot(t, dev, sb, goneSlot, func(slot []byte) {
		binary.LittleEndian.PutUint64(slot[32:40], uint64(time.Now().Add(-time.Hour).Unix()))
	})
	journalAt := int64(sb.JournalRegionOffset + uint64(goneSlot)*sb.JournalSlotSize())
	writeSector(t, dev, journalAt, []byte("not a journal"))

	c := fsck.New(dev, fsck.Options{Output: &bytes.Buffer{}})
	plan, err := c.PlanRelease(goneSlot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.NewRegistry(dev, sb.NodeRegistryOffset).Register(ctx, "late"); err != nil {
		t.Fatal(err)
	}
	if err := c.Release(plan); err == nil || !strings.Contains(err.Error(), "changed since fsck checked it") {
		t.Fatalf("Release after a node registered: %v", err)
	}
	header := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	if _, err := dev.ReadAt(header, journalAt); err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(header, []byte("not a journal")) {
		t.Error("Release reset the journal although it stopped")
	}
}

func editSlot(t *testing.T, dev *storage.Device, sb *superblock.Superblock, slot int, edit func(slot []byte)) {
	t.Helper()
	off := int64(sb.NodeRegistryOffset) + int64(slot)*node.SlotSize
	buf := storage.NewAlignedBuffer(node.SlotSize).Bytes()
	if _, err := dev.ReadAt(buf, off); err != nil {
		t.Fatal(err)
	}
	edit(buf)
	binary.LittleEndian.PutUint32(buf[node.SlotSize-4:], storage.CRC32C(buf[:node.SlotSize-4]))
	writeSector(t, dev, off, buf)
}
