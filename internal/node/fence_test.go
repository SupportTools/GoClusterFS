package node

import (
	"context"
	"errors"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fence"
)

// fencedPair registers a survivor and a node that then stalls with a stale heartbeat, each with its key on target.
func fencedPair(t *testing.T, target *fence.Target) (survivor *Registry, failing int, victimKey uint64) {
	t.Helper()
	survivor, victim, failing := fencedVictim(t, target)
	return survivor, failing, fence.NodeKey(victim.LocalUUID())
}

// fencedVictim is fencedPair that also returns the stalled node's registry.
func fencedVictim(t *testing.T, target *fence.Target) (survivor, victim *Registry, failing int) {
	t.Helper()
	dev, cleanup := createTestDevice(t, 1<<20)
	t.Cleanup(cleanup)
	ctx := context.Background()
	survivor, victim = NewRegistry(dev, 0), NewRegistry(dev, 0)
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
	return survivor, victim, failing
}

// A survivor marks its intent in the dead node's slot, then revokes the node's access, then marks the slot dead: a
// write the dead node still has queued, its heartbeat included, cannot land over the Dead mark or the replay, and a
// node that registers its key again after the revoke reads the intent.
func TestRecoveryPreemptsBeforeMarkingDead(t *testing.T) {
	target := fence.NewTarget()
	survivor, failing, victimKey := fencedPair(t, target)
	own := target.Node("survivor", fence.NodeKey(survivor.LocalUUID()))
	var atPreempt *Slot
	survivor.SetPreempter(func(owner [16]byte) error {
		atPreempt, _ = survivor.readSlot(failing)
		return own.Preempt(fence.NodeKey(owner))
	})
	if err := survivor.RecoverFailedNode(failing); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if atPreempt == nil || atPreempt.State != NodeStateActive || atPreempt.RecoveryOwner != survivor.LocalUUID() {
		t.Errorf("the slot at the preempt: %+v; want it Active with the survivor's intent", atPreempt)
	}
	if !slices.Equal(target.Preempted(), []uint64{victimKey}) || target.MayWrite("victim") {
		t.Errorf("preempted %x, victim may write %v; want only the victim's key gone", target.Preempted(), target.MayWrite("victim"))
	}
}

// A preempt that fails leaves the dead node's slot Active with the survivor's intent, and its journal and locks alone;
// the next scan tries again.
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

// A mount waits out another host's slot before it takes it, and revokes the owner's key only after that wait: a slot
// whose heartbeat moves keeps its owner's key, and a silent one is preempted no sooner than silentSlotWatch.
func TestMountPreemptsOnlyAfterTheSilenceWatch(t *testing.T) {
	defer func(watch time.Duration) { silentSlotWatch = watch }(silentSlotWatch)
	silentSlotWatch = 2 * time.Second
	for _, beating := range []bool{true, false} {
		dev, cleanup := createTestDevice(t, 1<<20)
		target := fence.NewTarget()
		owner := NewRegistry(dev, 0)
		ownerKey := target.Node("owner", fence.NodeKey(owner.LocalUUID()))
		if err := ownerKey.Register(); err != nil {
			t.Fatal(err)
		}
		slot := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: owner.LocalUUID(), HeartbeatSeq: 1,
			HeartbeatTime: time.Now().Unix(), JournalOffset: 1 << 20, JournalSize: 4 << 20}
		copy(slot.Hostname[:], "other-host")
		writeOwnerSlot := func() {
			owner.mu.Lock()
			defer owner.mu.Unlock()
			if err := owner.writeSlot(0, slot); err != nil {
				t.Error(err)
			}
		}
		writeOwnerSlot()
		stop, stopped := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(stopped)
			for beating {
				select {
				case <-stop:
					return
				case <-time.After(200 * time.Millisecond):
				}
				slot.HeartbeatSeq++
				slot.HeartbeatTime = time.Now().Unix()
				writeOwnerSlot()
			}
		}()

		mounting := NewRegistry(dev, 0)
		own := target.Node("mounting", fence.NodeKey(mounting.LocalUUID()))
		if err := own.Register(); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		var preemptedAfter time.Duration
		mounting.SetPreempter(func(owner [16]byte) error {
			preemptedAfter = time.Since(start)
			return own.Preempt(fence.NodeKey(owner))
		})
		mounting.PreferSlot(0)
		got, err := mounting.Register(context.Background(), "this-host")
		close(stop)
		<-stopped
		cleanup()
		switch {
		case err != nil:
			t.Fatalf("beating=%v: register: %v", beating, err)
		case beating && (got == 0 || len(target.Preempted()) != 0):
			t.Errorf("a live owner's slot: took slot %d and preempted %x; want another slot and no preempt", got, target.Preempted())
		case !beating && (got != 0 || !slices.Equal(target.Preempted(), []uint64{ownerKey.Key()})):
			t.Errorf("a silent owner's slot: took slot %d and preempted %x; want slot 0 and its owner's key", got, target.Preempted())
		case !beating && preemptedAfter < silentSlotWatch:
			t.Errorf("preempted a silent owner after %v, before the %v watch ended", preemptedAfter, silentSlotWatch)
		}
	}
}

// The device can refuse the heartbeat's own write, which holds the registry lock. Fenced must still stop the node once
// and refuse its later writes.
func TestFencedStopsTheNodeOnce(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	n := registerStalledNode(t, dev)
	_ = n.stallAfterBeatRead(t, 0, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			n.reg.Fenced(syscall.EBADE)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Fenced waited for the registry lock the heartbeat holds")
		}
	})
	n.reg.Fenced(syscall.EBADE)
	n.stopped(t)
	if len(n.losses) == 1 && !errors.Is(n.losses[0].Fenced, syscall.EBADE) {
		t.Errorf("slot loss %+v, want the device's EBADE", n.losses[0])
	}
	if err := n.reg.CheckOwnership(); !errors.Is(err, ErrSlotLost) {
		t.Errorf("CheckOwnership after the refused write = %v, want ErrSlotLost", err)
	}
	refusesDataWrites(t, dev, 1<<20)
}
