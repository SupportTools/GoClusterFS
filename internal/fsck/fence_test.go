package fsck_test

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

func slotUUID(t *testing.T, dev *storage.Device, sb *superblock.Superblock, slot int) [16]byte {
	t.Helper()
	slots, err := node.NewRegistry(dev, sb.NodeRegistryOffset).Slots()
	if err != nil || slots[slot] == nil {
		t.Fatalf("read slot %d: %v", slot, err)
	}
	return slots[slot].UUID
}

// Releasing a dead node's slot revokes that node's device access before fsck resets its journal; if the revoke
// fails, the slot and its journal stay as they were.
func TestReleaseSlotFencesTheNode(t *testing.T) {
	t.Cleanup(fsck.SetHeartbeatWatch(2*node.HeartbeatInterval, 2*node.HeartbeatInterval))
	var sb *superblock.Superblock
	dev := formattedDevice(t, func(s *superblock.Superblock) { sb = s })
	slot := goneNode(t, dev, sb)
	gone := slotUUID(t, dev, sb, slot)

	offline := errors.New("array offline")
	failing := fsck.New(dev, fsck.Options{Output: &bytes.Buffer{}, Preempt: func([16]byte) error { return offline }})
	plan, err := failing.PlanRelease(slot, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.Release(plan, false); !errors.Is(err, offline) {
		t.Fatalf("release with a failing revoke: %v, want it", err)
	}
	if slots, err := newChecker(dev).Slots(); err != nil || !slots[slot].InUse() {
		t.Fatalf("the slot was freed although its node kept its device access (%v)", err)
	}

	var revoked [][16]byte
	c := fsck.New(dev, fsck.Options{Output: &bytes.Buffer{}, Preempt: func(owner [16]byte) error {
		revoked = append(revoked, owner)
		return nil
	}})
	plan, err = c.PlanRelease(slot, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Release(plan, false); err != nil {
		t.Fatalf("release: %v", err)
	}
	if !slices.Equal(revoked, [][16]byte{gone}) {
		t.Errorf("revoked %x, want the released node only", revoked)
	}
}

// Repair revokes the device access of every node its slots still name before it writes, and refuses if it cannot.
func TestRepairFencesStoppedNodes(t *testing.T) {
	t.Cleanup(fsck.SetHeartbeatWatch(2*node.HeartbeatInterval, 2*node.HeartbeatInterval))
	var sb *superblock.Superblock
	dev := formattedDevice(t, func(s *superblock.Superblock) { sb = s })
	gone := slotUUID(t, dev, sb, goneNode(t, dev, sb))

	var revoked [][16]byte
	result := fsck.New(dev, fsck.Options{Output: &bytes.Buffer{}, Mode: fsck.Repair, Preempt: func(owner [16]byte) error {
		revoked = append(revoked, owner)
		return nil
	}}).Check()
	if result.Aborted || !slices.Equal(revoked, [][16]byte{gone}) {
		t.Errorf("repair aborted %v (%s), revoked %x; want it to revoke the stopped node and go on", result.Aborted, result.AbortReason, revoked)
	}

	result = fsck.New(dev, fsck.Options{Output: &bytes.Buffer{}, Mode: fsck.Repair, Preempt: func([16]byte) error {
		return errors.New("array offline")
	}}).Check()
	if !result.Aborted || !strings.Contains(result.AbortReason, "revoking the device access") {
		t.Errorf("repair with a failing revoke: aborted %v (%s), want refused", result.Aborted, result.AbortReason)
	}
}
