package node

import (
	"context"
	"errors"
	"testing"
	"time"
)

// keepWriting rewrites slot idx every 250ms as a live node whose clock is lag
// behind would: its heartbeat, or for a dead slot its recovery lease, advances.
func keepWriting(t *testing.T, reg *Registry, idx int, slot *Slot, lag time.Duration) (stop func()) {
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			case <-time.After(250 * time.Millisecond):
			}
			if slot.State == NodeStateDead {
				slot.RecoveryStart = max(time.Now().Add(-lag).Unix(), slot.RecoveryStart+1)
			} else {
				slot.HeartbeatSeq++
				slot.HeartbeatTime = time.Now().Add(-lag).Unix()
			}
			if err := reg.writeSlot(idx, slot); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	return func() { close(done); <-stopped }
}

func TestRegisterLeavesLiveSlotOfNodeWithSlowClock(t *testing.T) {
	shortenSilentSlotWatch(t)
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	fillWithReplays(t, reg, 1)

	live := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{7}, HeartbeatSeq: 1, HeartbeatTime: time.Now().Add(-2 * time.Minute).Unix()}
	copy(live.Hostname[:], "other-host")
	if err := reg.writeSlot(0, live); err != nil {
		t.Fatal(err)
	}
	defer keepWriting(t, reg, 0, live, 2*time.Minute)()
	time.Sleep(time.Second)

	if slot, err := reg.Register(context.Background(), "new-host"); !errors.Is(err, ErrNoFreeSlots) {
		t.Errorf("Register = %d, %v; want ErrNoFreeSlots: the only slot not empty or under recovery holds a live node whose clock is 2 minutes behind", slot, err)
	}
}

func TestRenewedRecoveryLeaseIsHeldWhateverTheReplayersClock(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)

	failed := &Slot{Magic: SlotMagic, State: NodeStateDead, UUID: [16]byte{5}, RecoveryOwner: [16]byte{6}, RecoveryStart: time.Now().Add(-2 * time.Minute).Unix()}
	if err := reg.writeSlot(4, failed); err != nil {
		t.Fatal(err)
	}
	silence(reg, 4, failed, recoveryLease+time.Second)
	if _, state := reg.Occupant(4, true); state != OccupiedLapsed {
		t.Fatalf("a lease unchanged for longer than recoveryLease: %v, want lapsed", state)
	}

	stop := keepWriting(t, reg, 4, failed, 2*time.Minute)
	time.Sleep(time.Second)
	if _, state := reg.Occupant(4, true); state != OccupiedReplaying {
		t.Errorf("a lease the replayer keeps renewing, with its clock 2 minutes behind: %v, want replaying", state)
	}
	_, err := reg.acquireForRecovery(4, failed, HeartbeatTimeout)
	stop()
	if err == nil {
		t.Error("took over the recovery of a replayer that keeps renewing its lease")
	}
}

func TestCrashedNodeWithFastClockIsReplayed(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	if _, err := reg.Register(context.Background(), "observer"); err != nil {
		t.Fatal(err)
	}
	crashed := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{9}, HeartbeatSeq: 7, HeartbeatTime: time.Now().Add(3 * time.Minute).Unix()}
	copy(crashed.Hostname[:], "other-host")
	if err := reg.writeSlot(5, crashed); err != nil {
		t.Fatal(err)
	}
	replayed := false
	reg.SetRecoveryHandler(func(info *FailedNodeInfo) error {
		replayed = replayed || info.SlotIndex == 5
		return nil
	})

	silence(reg, 5, crashed, HeartbeatTimeout+time.Second)
	if _, state := reg.Occupant(5, false); state != OccupiedFailing {
		t.Errorf("a heartbeat 3 minutes in the future, unchanged for longer than HeartbeatTimeout: %v, want failing", state)
	}
	silence(reg, 5, crashed, deadAfter+time.Second)
	reg.detectAndRecoverFailedNodes()
	if !replayed {
		t.Error("a node whose heartbeat stopped for longer than deadAfter was not replayed, because its clock was 3 minutes ahead")
	}
}

func TestSlowRegistryReadsDoNotStarveHeartbeat(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	if _, err := reg.Register(context.Background(), "slow"); err != nil {
		t.Fatal(err)
	}
	dev.SetReadFault(func(int64) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	defer dev.SetReadFault(func(int64) error { return nil })
	if err := reg.StartHeartbeat(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reg.Unregister(context.Background()) }()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = reg.GetActiveNodes()
			reg.detectAndRecoverFailedNodes()
		}
	}()

	var worst time.Duration
	for start := time.Now(); time.Since(start) < 8*time.Second; time.Sleep(100 * time.Millisecond) {
		worst = max(worst, reg.sinceLastBeat())
	}
	if worst >= HeartbeatTimeout/2 {
		t.Errorf("with 100ms registry reads and registry scans running, the heartbeat went %v without a beat", worst.Round(100*time.Millisecond))
	}
}
