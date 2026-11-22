package node

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fence"
	"github.com/supporttools/GoClusterFS/internal/lock"
)

// A node whose clock missed a pause restores its key while a survivor takes it over: the survivor's preempt comes
// between the node's first slot read and its register, or between its register and its second slot read. Either way
// the node reads the survivor's intent after its register, removes its key, and cannot write during the replay.
func TestRekeyDuringATakeoverReadsTheIntent(t *testing.T) {
	for _, tc := range []struct {
		name       string
		takeOverAt int
	}{
		{"register after the preempt", 1},
		{"preempt between the register and the slot read", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := fence.NewTarget()
			survivor, victim, failing := fencedVictim(t, target)
			target.Relogin("victim")
			own := target.Node("survivor", fence.NodeKey(survivor.LocalUUID()))
			preempted, resume := make(chan struct{}), make(chan struct{})
			survivor.SetPreempter(func(owner [16]byte) error {
				err := own.Preempt(fence.NodeKey(owner))
				close(preempted)
				<-resume
				return err
			})
			victimWroteDuringReplay := false
			survivor.SetRecoveryHandler(func(*FailedNodeInfo) error {
				victimWroteDuringReplay = target.MayWrite("victim")
				return nil
			})
			recovered := make(chan error, 1)

			reads := 0
			k := &fence.Rekeyer{
				PR:           target.Node("victim", fence.NodeKey(victim.LocalUUID())),
				HoldWrites:   func(fn func() error) error { return fn() },
				HeartbeatAge: func() time.Duration { return time.Second },
				SlotLive: func() error {
					reads++
					if reads != tc.takeOverAt {
						return victim.SlotLive(failing)
					}
					err := victim.SlotLive(failing)
					if tc.takeOverAt == 1 && err != nil {
						return err
					}
					go func() { recovered <- survivor.RecoverFailedNode(failing) }()
					<-preempted
					if tc.takeOverAt == 1 {
						return err
					}
					return victim.SlotLive(failing)
				},
				StallLimit: StallLimit,
			}
			err := k.Rekey()
			close(resume)
			if rerr := <-recovered; rerr != nil {
				t.Fatalf("survivor's recovery: %v", rerr)
			}
			if err == nil || !strings.Contains(err.Error(), "revoking") {
				t.Errorf("Rekey: %v, want it to read the survivor's intent", err)
			}
			if victimWroteDuringReplay || target.MayWrite("victim") {
				t.Errorf("victim may write during the replay %v, after it %v; want neither", victimWroteDuringReplay, target.MayWrite("victim"))
			}
		})
	}
}

// A heartbeat that finds a peer's intent in the node's slot stops the node, as a Dead mark does.
func TestBeatStopsOnAnIntent(t *testing.T) {
	_, victim, failing := fencedVictim(t, fence.NewTarget())
	var loss *SlotLoss
	victim.SetSlotLostHandler(func(l SlotLoss) { loss = &l })
	victim.mu.Lock()
	slot, err := victim.readSlot(failing)
	if err == nil {
		slot.RecoveryOwner = [16]byte{9}
		err = victim.writeSlot(failing, slot)
	}
	victim.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	victim.lastBeat.Store(int64(bootClock()))
	if err := victim.beat(failing); !errors.Is(err, ErrSlotLost) || loss == nil || !loss.Taken {
		t.Errorf("beat: %v, loss %+v; want the slot lost", err, loss)
	}
	if after, _ := victim.readSlot(failing); after.RecoveryOwner != [16]byte{9} {
		t.Error("the heartbeat wrote over the intent")
	}
}

// A heartbeat that lands between the survivor's slot read and its intent makes the intent CAS fail, so the survivor
// neither preempts nor replays.
func TestIntentFailsAfterTheVictimBeats(t *testing.T) {
	target := fence.NewTarget()
	survivor, victim, failing := fencedVictim(t, target)
	survivor.mu.Lock()
	raw, _, err := survivor.readSlotRaw(failing)
	survivor.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	victim.lastBeat.Store(int64(bootClock()))
	if err := victim.beat(failing); err != nil {
		t.Fatal(err)
	}
	survivor.mu.Lock()
	_, err = survivor.markIntent(failing, raw)
	survivor.mu.Unlock()
	if !errors.Is(err, ErrSlotTaken) || len(target.Preempted()) != 0 {
		t.Errorf("markIntent after a heartbeat: %v, preempted %x; want ErrSlotTaken and no preempt", err, target.Preempted())
	}
}

// A heartbeat the node read before the intent and wrote before the preempt erases the intent. The survivor has
// preempted by then, but its Dead CAS fails, so it replays nothing.
func TestTakeoverAbortsWhenABeatErasesTheIntent(t *testing.T) {
	target := fence.NewTarget()
	survivor, victim, failing := fencedVictim(t, target)
	victim.mu.Lock()
	before, err := victim.readSlot(failing)
	victim.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	own := target.Node("survivor", fence.NodeKey(survivor.LocalUUID()))
	survivor.SetPreempter(func(owner [16]byte) error {
		before.HeartbeatSeq++
		victim.mu.Lock()
		err := victim.writeSlot(failing, before)
		victim.mu.Unlock()
		if err != nil {
			return err
		}
		return own.Preempt(fence.NodeKey(owner))
	})
	replayed := false
	survivor.SetRecoveryHandler(func(*FailedNodeInfo) error { replayed = true; return nil })
	if err := survivor.RecoverFailedNode(failing); !errors.Is(err, ErrSlotTaken) || replayed {
		t.Errorf("recover after the intent was erased: %v, replayed %v; want ErrSlotTaken and no replay", err, replayed)
	}
	if after, _ := survivor.readSlot(failing); after.State != NodeStateActive || after.HeartbeatSeq != before.HeartbeatSeq {
		t.Errorf("slot after the abort: %v seq %d; want the node's own heartbeat", after.State, after.HeartbeatSeq)
	}
}

// A peer that wrote its intent and died leaves the slot Active with the intent, before or after its preempt. Peers
// watch the slot unchanged for deadAfter like any silent slot, then take it over from the intent; a preempt of a key
// the dead peer already removed finds nothing to remove and lets the takeover go on.
func TestOrphanedIntentIsTakenOverLater(t *testing.T) {
	for _, preempted := range []bool{false, true} {
		t.Run(fmt.Sprintf("died after its preempt %v", preempted), func(t *testing.T) {
			target := fence.NewTarget()
			survivor, victim, failing := fencedVictim(t, target)
			if preempted {
				dead := target.Node("dead-peer", fence.NodeKey([16]byte{9}))
				if err := dead.Register(); err != nil {
					t.Fatal(err)
				}
				if err := dead.Preempt(fence.NodeKey(victim.LocalUUID())); err != nil {
					t.Fatal(err)
				}
			}
			orphan := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: victim.LocalUUID(), HeartbeatSeq: 1,
				HeartbeatTime: time.Now().Add(-time.Minute).Unix(), JournalOffset: 1 << 20, JournalSize: 4 << 20,
				RecoveryOwner: [16]byte{9}, RecoveryStart: time.Now().Add(-time.Minute).Unix()}
			victim.mu.Lock()
			err := victim.writeSlot(failing, orphan)
			victim.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			survivor.mu.Lock()
			survivor.observe(failing, orphan)
			survivor.mu.Unlock()
			if err := survivor.RecoverFailedNode(failing); err == nil {
				t.Fatal("recovered an intent the survivor saw only just now")
			}
			silence(survivor, failing, orphan, deadAfter+time.Second)
			var owner [16]byte
			survivor.SetRecoveryHandler(func(*FailedNodeInfo) error {
				slot, _ := survivor.readSlot(failing)
				owner = slot.RecoveryOwner
				return nil
			})
			if err := survivor.RecoverFailedNode(failing); err != nil || owner != survivor.LocalUUID() {
				t.Errorf("recover the orphaned intent: %v, recovery owner %x; want the survivor", err, owner[:8])
			}
			if target.MayWrite("victim") {
				t.Error("the victim may still write")
			}
		})
	}
}

// A build that marks an intent first records another lock layout than one that does not, so neither joins while the
// other runs: a node that restores its key is safe only from peers that mark an intent first. An older build records
// lock.LayoutID() itself, and the same check in it refuses the newer build's slot.
func TestRegisterRefusesNextToAnotherTakeoverProtocol(t *testing.T) {
	shortenSilentSlotWatch(t)
	for _, newerLive := range []bool{false, true} {
		dev, cleanup := createTestDevice(t, 10<<20)
		ctx := context.Background()
		live, joining := NewRegistry(dev, 0), NewRegistry(dev, 0)
		live.SetLockLayout(lock.LayoutID())
		joining.SetLockLayout(lock.LayoutID())
		older := joining
		if !newerLive {
			older = live
		}
		older.lockLayout = lock.LayoutID()
		if _, err := live.Register(ctx, "live"); err != nil {
			t.Fatal(err)
		}
		if err := live.StartHeartbeat(); err != nil {
			t.Fatal(err)
		}
		_, err := joining.Register(ctx, "joining")
		if !errors.Is(err, ErrLockLayout) {
			t.Errorf("newer build live %v: Register = %v, want ErrLockLayout", newerLive, err)
		}
		// Joining next to an older live node, this build names the takeover protocol.
		if !newerLive && err != nil && !strings.Contains(err.Error(), "without marking an intent") {
			t.Errorf("Register = %v, want it to name the takeover protocol", err)
		}
		_ = live.Unregister(ctx)
		cleanup()
	}
}

// A takeover whose preempt did not take effect, as the device shows by still listing the victim's key, puts the slot
// back as the victim left it, so a live victim keeps it. A preempt whose outcome is unknown leaves the intent.
func TestFailedPreemptPutsTheSlotBackOnlyWhenItDidNotTakeEffect(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		keepsIts bool
	}{
		{"did not take effect", fmt.Errorf("%w: path down", fence.ErrNotPreempted), true},
		{"outcome unknown", errors.New("path down"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := fence.NewTarget()
			survivor, victim, failing := fencedVictim(t, target)
			survivor.SetPreempter(func([16]byte) error { return tc.err })
			if err := survivor.RecoverFailedNode(failing); err == nil {
				t.Fatal("RecoverFailedNode succeeded although its preempt failed")
			}
			slot, err := survivor.readSlot(failing)
			if err != nil {
				t.Fatal(err)
			}
			_, _, marked := slot.Intent()
			if tc.keepsIts && (marked || victim.SlotLive(failing) != nil) {
				t.Errorf("slot still marked %v, victim's slot check %v; want the slot back and the victim keeping it",
					marked, victim.SlotLive(failing))
			}
			if !tc.keepsIts && (!marked || slot.RecoveryOwner != survivor.LocalUUID()) {
				t.Errorf("slot marked %v by %x; want the survivor's intent to stay", marked, slot.RecoveryOwner[:8])
			}
		})
	}
}

// A live slot with an intent counts as failing for lock waiters, though the intent changed its bytes, and fsck's
// offline release waits deadAfter from the intent, however old the node's last heartbeat.
func TestAnIntentSlotIsFailingAndHoldsItsLease(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	t.Cleanup(cleanup)
	reg := NewRegistry(dev, 0)
	marked := time.Now()
	slot := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{1}, HeartbeatSeq: 1,
		HeartbeatTime: marked.Add(-time.Hour).Unix(), RecoveryOwner: [16]byte{2}, RecoveryStart: marked.Unix()}
	reg.mu.Lock()
	err := reg.writeSlot(3, slot)
	reg.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, occ := reg.Occupant(3, true); occ != OccupiedFailing {
		t.Errorf("Occupant = %v, want OccupiedFailing", occ)
	}
	if got, want := slot.LeaseExpiry(), time.Unix(marked.Unix(), 0).Add(deadAfter); !got.Equal(want) {
		t.Errorf("LeaseExpiry = %v, want %v, deadAfter after the intent", got, want)
	}
}
