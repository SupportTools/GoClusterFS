package node

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fence"
)

// fencedPair registers a survivor and a node that then stalls with a stale heartbeat, each with its key on target.
func fencedPair(t *testing.T, target *fence.Target) (survivor *Registry, failing int, victimKey uint64) {
	t.Helper()
	dev, cleanup := createTestDevice(t, 1<<20)
	t.Cleanup(cleanup)
	ctx := context.Background()
	survivor, victim := NewRegistry(dev, 0), NewRegistry(dev, 0)
	if _, err := survivor.Register(ctx, "survivor"); err != nil {
		t.Fatal(err)
	}
	failing, err := victim.Register(ctx, "victim")
	if err != nil {
		t.Fatal(err)
	}
	own := target.Node("survivor", fence.NodeKey(survivor.LocalUUID()))
	dead := target.Node("victim", fence.NodeKey(victim.LocalUUID()))
	for _, n := range []*fence.PR{own, dead} {
		if err := n.Register(); err != nil {
			t.Fatal(err)
		}
		if err := n.Reserve(); err != nil {
			t.Fatal(err)
		}
	}
	survivor.SetPreempter(func(owner [16]byte) error { return own.Preempt(fence.NodeKey(owner)) })

	stale := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: victim.LocalUUID(), HeartbeatSeq: 1,
		HeartbeatTime: time.Now().Add(-2 * HeartbeatTimeout).Unix(), JournalOffset: 1 << 20, JournalSize: 4 << 20}
	victim.mu.Lock()
	err = victim.writeSlot(failing, stale)
	victim.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	silence(survivor, failing, stale, deadAfter+time.Second)
	survivor.SetRecoveryHandler(func(*FailedNodeInfo) error { return nil })
	return survivor, failing, dead.Key()
}

// A survivor revokes the dead node's access before it marks the slot dead, so a write the dead node still has queued,
// its heartbeat included, cannot land over the Dead mark or the replay.
func TestRecoveryPreemptsBeforeMarkingDead(t *testing.T) {
	target := fence.NewTarget()
	survivor, failing, victimKey := fencedPair(t, target)
	early := false
	survivor.dev.SetWriteObserver(func(offset int64, _ int) {
		if offset == int64(failing*SlotSize) && len(target.Preempted()) == 0 {
			early = true
		}
	})
	if err := survivor.RecoverFailedNode(failing); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if early {
		t.Error("the survivor wrote the dead node's slot before preempting its key")
	}
	if !slices.Equal(target.Preempted(), []uint64{victimKey}) || target.MayWrite("victim") {
		t.Errorf("preempted %x, victim may write %v; want only the victim's key gone", target.Preempted(), target.MayWrite("victim"))
	}
}

// A preempt that fails leaves the dead node's slot, journal and locks alone; the next scan tries again.
func TestRecoveryStopsWhenPreemptFails(t *testing.T) {
	survivor, failing, _ := fencedPair(t, fence.NewTarget())
	offline := errors.New("array offline")
	survivor.SetPreempter(func([16]byte) error { return offline })
	replayed := false
	survivor.SetRecoveryHandler(func(*FailedNodeInfo) error { replayed = true; return nil })
	if err := survivor.RecoverFailedNode(failing); !errors.Is(err, offline) {
		t.Fatalf("recover with a failing preempt: %v, want it", err)
	}
	survivor.mu.Lock()
	slot, err := survivor.readSlot(failing)
	survivor.mu.Unlock()
	if err != nil || slot.State != NodeStateActive || replayed {
		t.Errorf("slot state %v (%v), replayed %v; want the slot untouched and no replay", slot.State, err, replayed)
	}
}

// fsck releases a dead node's slot only after revoking its access, so the journal reset cannot race its writes.
func TestReleaseSlotPreemptsBeforeTheReset(t *testing.T) {
	target := fence.NewTarget()
	survivor, failing, victimKey := fencedPair(t, target)
	survivor.mu.Lock()
	expected, _, err := survivor.readSlotRaw(failing)
	survivor.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	var preemptedAtReset []uint64
	released, err := survivor.ReleaseSlot(failing, expected, func(func() error) error {
		preemptedAtReset = target.Preempted()
		return nil
	})
	if err != nil || !released {
		t.Fatalf("release: %v, %v", released, err)
	}
	if !slices.Equal(preemptedAtReset, []uint64{victimKey}) {
		t.Errorf("preempted %x when the journal reset ran, want the victim's key", preemptedAtReset)
	}
}

// A node that mounts into a dead node's slot revokes that node's access before it writes over the slot.
func TestMountingIntoADeadSlotPreempts(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	target := fence.NewTarget()
	gone := NewRegistry(dev, 0)
	dead := target.Node("old-mount", fence.NodeKey(gone.LocalUUID()))
	if err := dead.Register(); err != nil {
		t.Fatal(err)
	}
	stale := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: gone.LocalUUID(), HeartbeatSeq: 1, BootID: readBootID(),
		HeartbeatTime: time.Now().Add(-2 * HeartbeatTimeout).Unix(), JournalOffset: 1 << 20, JournalSize: 4 << 20}
	copy(stale.Hostname[:], "host")
	gone.mu.Lock()
	err := gone.writeSlot(0, stale)
	gone.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	mounting := NewRegistry(dev, 0)
	own := target.Node("new-mount", fence.NodeKey(mounting.LocalUUID()))
	if err := own.Register(); err != nil {
		t.Fatal(err)
	}
	mounting.SetPreempter(func(owner [16]byte) error { return own.Preempt(fence.NodeKey(owner)) })
	slot, err := mounting.Register(context.Background(), "host")
	if err != nil || slot != 0 {
		t.Fatalf("register: slot %d, %v; want the dead node's slot 0", slot, err)
	}
	if !slices.Equal(target.Preempted(), []uint64{dead.Key()}) {
		t.Errorf("preempted %x, want the dead node's key", target.Preempted())
	}
}
