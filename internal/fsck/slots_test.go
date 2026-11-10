package fsck_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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
	refused := func(slot int, reason string) {
		t.Helper()
		if _, err := newChecker(dev).PlanRelease(slot, false); err == nil || !strings.Contains(err.Error(), reason) {
			t.Fatalf("PlanRelease(%d) error = %v, want one containing %q", slot, err, reason)
		}
	}

	gone := node.NewRegistry(dev, sb.NodeRegistryOffset)
	slot, err := gone.Register(ctx, "gone")
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
	refused(slot, "mounted by node 1 (live)")
	if err := live.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
	refused(slot, "node 0 (gone) may still be alive")
	setHeartbeat(t, dev, sb, slot, time.Now().Add(-30*time.Second))
	refused(slot, "may still be alive")

	editSlot(t, dev, sb, slot, func(s []byte) {
		binary.LittleEndian.PutUint32(s[4:8], uint32(node.NodeStateDead))
		binary.LittleEndian.PutUint64(s[172:180], uint64(time.Now().Unix()))
	})
	refused(slot, "may still be alive")
	editSlot(t, dev, sb, slot, func(s []byte) {
		binary.LittleEndian.PutUint64(s[172:180], uint64(time.Now().Add(-time.Hour).Unix()))
	})
	journalAt := damageJournal(t, dev, sb, slot)

	c := newChecker(dev)
	plan, err := c.PlanRelease(slot, false)
	if err != nil {
		t.Fatalf("PlanRelease of an expired slot with a damaged journal: %v", err)
	}
	if !strings.HasPrefix(plan.Slot.Journal, "damaged: ") || !strings.HasPrefix(plan.Discards, "the whole journal, unread") {
		t.Errorf("plan reports journal %q, discards %q", plan.Slot.Journal, plan.Discards)
	}
	if err := c.Release(plan); err != nil {
		t.Fatalf("Release: %v", err)
	}

	slots, err := newChecker(dev).Slots()
	if err != nil {
		t.Fatal(err)
	}
	if s := slots[slot]; s.InUse() || !s.JournalFresh {
		t.Errorf("after release slot %d is in use=%v with journal %q", slot, s.InUse(), s.Journal)
	}
	replacement := node.NewRegistry(dev, sb.NodeRegistryOffset)
	if got, err := replacement.Register(ctx, "replacement"); err != nil || got != slot {
		t.Fatalf("a new node registered in slot %d (%v), want released slot %d", got, err, slot)
	}
	if result, err := journal.ScanAndRecover(dev, uint64(journalAt), sb.JournalSlotSize(), nil); result != nil || err != nil {
		t.Errorf("mount recovery of the released journal = %+v, %v; want nothing to recover", result, err)
	}
}

func TestReleaseSlotRefusals(t *testing.T) {
	t.Cleanup(fsck.SetHeartbeatWatch(2*node.HeartbeatInterval, 2*node.HeartbeatInterval))
	var sb *superblock.Superblock
	dev := formattedDevice(t, func(s *superblock.Superblock) { sb = s })
	slot := goneNode(t, dev, sb)

	if _, err := newChecker(dev).PlanRelease(node.MaxNodes, false); !errors.Is(err, fsck.ErrNoSuchSlot) {
		t.Errorf("PlanRelease of slot %d: %v, want %v", node.MaxNodes, err, fsck.ErrNoSuchSlot)
	}

	j, err := journal.Init(dev, sb.JournalRegionOffset+uint64(slot)*sb.JournalSlotSize(), sb.JournalSlotSize(),
		[16]byte{1}, 1, journal.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	txn := j.Begin()
	if err := txn.LogInodeDelete(&journal.InodeDeletePayload{InodeNum: 7}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Commit(txn); err != nil {
		t.Fatal(err)
	}
	if _, err := newChecker(dev).PlanRelease(slot, false); err == nil || !strings.Contains(err.Error(), "mount a node to replay it") {
		t.Errorf("PlanRelease of a replayable journal: %v", err)
	}
	if plan, err := newChecker(dev).PlanRelease(slot, true); err != nil || plan.Discards != "1 committed transaction that was never replayed" {
		t.Errorf("PlanRelease with force: plan %+v, error %v", plan, err)
	}

	slotAt := int64(sb.NodeRegistryOffset) + int64(slot)*node.SlotSize
	corrupt := readSector(t, dev, slotAt)
	corrupt[100] ^= 0xFF
	writeSector(t, dev, slotAt, corrupt)
	if _, err := newChecker(dev).PlanRelease(slot, true); err == nil || !strings.Contains(err.Error(), "unreadable") {
		t.Errorf("PlanRelease of an unreadable slot: %v", err)
	}

	writeSector(t, dev, 0, []byte("not a superblock"))
	if _, err := newChecker(dev).PlanRelease(slot, true); err == nil || !strings.Contains(err.Error(), "mount would reject the superblock") {
		t.Errorf("PlanRelease with a bad superblock: %v", err)
	}
}

// A node that takes the slot between the registry check and the lease stops
// the release before the journal is touched. Once fsck holds the lease, a
// mounting node takes another slot.
func TestReleaseSlotLease(t *testing.T) {
	t.Cleanup(fsck.SetHeartbeatWatch(2*node.HeartbeatInterval, 2*node.HeartbeatInterval))
	var sb *superblock.Superblock
	dev := formattedDevice(t, func(s *superblock.Superblock) { sb = s })
	ctx := context.Background()
	release := func(slot int) error {
		t.Helper()
		c := newChecker(dev)
		plan, err := c.PlanRelease(slot, false)
		if err != nil {
			t.Fatal(err)
		}
		return c.Release(plan)
	}

	slot := goneNode(t, dev, sb)
	journalAt := damageJournal(t, dev, sb, slot)
	damaged := readSector(t, dev, journalAt)
	var late *node.Registry
	t.Cleanup(fsck.SetReleaseHooks(func() {
		late = node.NewRegistry(dev, sb.NodeRegistryOffset)
		if got, err := late.Register(ctx, "late"); err != nil || got != slot {
			t.Errorf("late node took slot %d (%v), want the stale slot %d", got, err, slot)
		}
	}, nil))
	if err := release(slot); err == nil || !strings.Contains(err.Error(), "its journal was not touched") {
		t.Fatalf("Release after a node took the slot: %v", err)
	}
	if !bytes.Equal(readSector(t, dev, journalAt), damaged) {
		t.Error("Release reset the journal of a slot another node took")
	}
	if err := late.Unregister(ctx); err != nil {
		t.Fatal(err)
	}

	slot = goneNode(t, dev, sb)
	damageJournal(t, dev, sb, slot)
	t.Cleanup(fsck.SetReleaseHooks(nil, func() {
		if got, err := node.NewRegistry(dev, sb.NodeRegistryOffset).Register(ctx, "during"); err != nil || got == slot {
			t.Errorf("node mounting during the release took slot %d (%v)", got, err)
		}
	}))
	if err := release(slot); err != nil {
		t.Fatalf("Release while a node mounts: %v", err)
	}

	slot = goneNode(t, dev, sb)
	damageJournal(t, dev, sb, slot)
	t.Cleanup(fsck.SetReleaseHooks(nil, func() {
		setHeartbeat(t, dev, sb, slot, time.Now())
	}))
	if err := release(slot); err == nil || !strings.Contains(err.Error(), "its journal is reset but the slot is not free") {
		t.Fatalf("Release of a slot written during the lease: %v", err)
	}
}

// fsck can crash after it takes a slot's recovery lease, before or after it
// resets the journal. The lease keeps mounts and peers away until it lapses;
// then a peer replays the journal it finds: it fails on the untouched damaged
// one, and frees the slot after the reset one.
func TestReleaseSlotCrash(t *testing.T) {
	for _, tt := range []struct {
		name       string
		resetFirst bool
	}{{"before reset", false}, {"after reset", true}} {
		t.Run(tt.name, func(t *testing.T) {
			var sb *superblock.Superblock
			dev := formattedDevice(t, func(s *superblock.Superblock) { sb = s })
			ctx := context.Background()
			gone := node.NewRegistry(dev, sb.NodeRegistryOffset)
			gone.SetJournalArea(sb.JournalRegionOffset, sb.JournalSlotSize())
			slot, err := gone.Register(ctx, "gone")
			if err != nil {
				t.Fatal(err)
			}
			setHeartbeat(t, dev, sb, slot, time.Now().Add(-time.Hour))
			journalAt := damageJournal(t, dev, sb, slot)

			crash := errors.New("fsck crashed")
			raw := readSector(t, dev, int64(sb.NodeRegistryOffset)+int64(slot)*node.SlotSize)
			released, err := node.NewRegistry(dev, sb.NodeRegistryOffset).ReleaseSlot(slot, raw, func() error {
				if tt.resetFirst {
					writeSector(t, dev, journalAt, nil)
				}
				return crash
			})
			if released || !errors.Is(err, crash) {
				t.Fatalf("ReleaseSlot with a crashing reset = %v, %v", released, err)
			}
			journalLeft := readSector(t, dev, journalAt)

			peer := node.NewRegistry(dev, sb.NodeRegistryOffset)
			peer.SetJournalArea(sb.JournalRegionOffset, sb.JournalSlotSize())
			if got, err := peer.Register(ctx, "peer"); err != nil || got == slot {
				t.Fatalf("a node mounting during the lease took slot %d (%v)", got, err)
			}
			if replays := detectFailures(peer, dev); len(replays) != 0 {
				t.Fatalf("a peer replayed slot %d during the lease", slot)
			}

			editSlot(t, dev, sb, slot, func(s []byte) {
				binary.LittleEndian.PutUint64(s[172:180], uint64(time.Now().Add(-61*time.Second).Unix()))
			})
			replays := detectFailures(peer, dev)
			if len(replays) != 1 || replays[0].slot != slot {
				t.Fatalf("after the lease lapsed the peer replayed %+v, want slot %d once", replays, slot)
			}
			state, err := node.ParseSlot(readSector(t, dev, int64(sb.NodeRegistryOffset)+int64(slot)*node.SlotSize))
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case tt.resetFirst && (replays[0].err != nil || state.State != node.NodeStateEmpty):
				t.Errorf("replay of the reset journal: %v, slot %s; want success and an empty slot", replays[0].err, state.State)
			case !tt.resetFirst && (replays[0].err == nil || !bytes.Equal(readSector(t, dev, journalAt), journalLeft)):
				t.Errorf("replay of the damaged journal: %v; want an error and the journal untouched", replays[0].err)
			}
		})
	}
}

type replay struct {
	slot int
	err  error
}

// detectFailures runs one failure detection pass on peer and lists the
// journals it replayed.
func detectFailures(peer *node.Registry, dev *storage.Device) []replay {
	var replays []replay
	peer.SetRecoveryHandler(func(info *node.FailedNodeInfo) error {
		_, err := journal.ReplayJournal(context.Background(), &journal.RecoveryContext{Dev: dev}, info)
		replays = append(replays, replay{info.SlotIndex, err})
		return err
	})
	peer.StartFailureDetection()
	peer.Stop()
	return replays
}

func newChecker(dev *storage.Device) *fsck.Checker {
	return fsck.New(dev, fsck.Options{Output: &bytes.Buffer{}})
}

// goneNode registers a node that stopped an hour ago and returns its slot.
func goneNode(t *testing.T, dev *storage.Device, sb *superblock.Superblock) int {
	t.Helper()
	slot, err := node.NewRegistry(dev, sb.NodeRegistryOffset).Register(context.Background(), "gone")
	if err != nil {
		t.Fatal(err)
	}
	setHeartbeat(t, dev, sb, slot, time.Now().Add(-time.Hour))
	return slot
}

// damageJournal writes a dirty journal header with a bad checksum into slot's
// journal and returns the journal's offset.
func damageJournal(t *testing.T, dev *storage.Device, sb *superblock.Superblock, slot int) int64 {
	t.Helper()
	at := int64(sb.JournalRegionOffset + uint64(slot)*sb.JournalSlotSize())
	header := journal.NewJournalSuperblock([16]byte{1}, 1, 1<<20)
	header.State = journal.JournalStateDirty
	damaged := header.Serialize()
	damaged[100] ^= 0xFF
	writeSector(t, dev, at, damaged)
	return at
}

func setHeartbeat(t *testing.T, dev *storage.Device, sb *superblock.Superblock, slot int, at time.Time) {
	t.Helper()
	editSlot(t, dev, sb, slot, func(s []byte) { binary.LittleEndian.PutUint64(s[32:40], uint64(at.Unix())) })
}

func editSlot(t *testing.T, dev *storage.Device, sb *superblock.Superblock, slot int, edit func(slot []byte)) {
	t.Helper()
	off := int64(sb.NodeRegistryOffset) + int64(slot)*node.SlotSize
	buf := readSector(t, dev, off)
	edit(buf)
	binary.LittleEndian.PutUint32(buf[node.SlotSize-4:], storage.CRC32C(buf[:node.SlotSize-4]))
	writeSector(t, dev, off, buf)
}

func readSector(t *testing.T, dev *storage.Device, offset int64) []byte {
	t.Helper()
	buf := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	if _, err := dev.ReadAt(buf, offset); err != nil {
		t.Fatal(err)
	}
	return buf
}
