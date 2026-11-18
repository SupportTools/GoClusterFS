package node

import (
	"context"
	"testing"
	"time"
)

// The orphan handler can wait for a failed node's inode lock until that node's
// replay frees it, so a blocked orphan pass must not keep failure detection
// from replaying.
func TestBlockedOrphanPassLetsDetectionReplay(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	if reg.bootID == ([16]byte{}) || reg.pidNS == 0 {
		t.Skip("no kernel boot ID or PID namespace")
	}
	if _, err := reg.Register(context.Background(), "node"); err != nil {
		t.Fatal(err)
	}
	reg.failureCheckInterval = 20 * time.Millisecond
	entered, release := make(chan struct{}, 1), make(chan struct{})
	reg.SetOrphanHandler(func(int, []Orphan) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	})
	replayed := make(chan int, 1)
	reg.SetRecoveryHandler(func(info *FailedNodeInfo) error {
		replayed <- info.SlotIndex
		return nil
	})
	go reg.StartFailureDetection()
	defer reg.Stop()
	defer close(release)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no orphan pass started")
	}

	writeLocalSlot(t, reg, 3, exitedPID(t), 1)
	select {
	case slot := <-replayed:
		if slot != 3 {
			t.Errorf("replayed slot %d, want 3", slot)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failure detection did not replay the dead node while an orphan pass waited")
	}
}

func TestOrphanPassSkipsAFailedPeer(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	if _, err := reg.Register(context.Background(), "node"); err != nil {
		t.Fatal(err)
	}
	for _, idx := range []int{1, 2} {
		peer := writeOtherNamespaceSlot(t, reg, idx, time.Now())
		peer.Orphans = []Orphan{{Inode: uint32(100 + idx), Birth: 1}}
		if err := reg.writeSlot(idx, peer); err != nil {
			t.Fatal(err)
		}
	}
	var passed []int
	reg.SetOrphanHandler(func(slot int, _ []Orphan) { passed = append(passed, slot) })
	slots, err := reg.readAllSlots()
	if err != nil {
		t.Fatal(err)
	}
	reg.passOrphans(slots)
	silence(reg, 2, slots[2], 2*HeartbeatTimeout)
	passed = nil
	reg.passOrphans(slots)
	if len(passed) != 2 || passed[0] != 0 || passed[1] != 1 {
		t.Errorf("passed slots %v, want [0 1]: slot 2's heartbeat stopped, so its replay adopts its orphans", passed)
	}
}

// Occupant judges a slot by the last read failure detection kept. When
// detection is held up, other readers still see the slot's later heartbeats;
// Occupant must judge the same read, or each check restarts the other's silence.
func TestOccupantSeesASilentSlotThatOthersRead(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	if _, err := reg.Register(context.Background(), "node"); err != nil {
		t.Fatal(err)
	}
	writeOtherNamespaceSlot(t, reg, 1, time.Now().Add(-5*time.Second))
	if _, occ := reg.Occupant(1, true); occ != OccupiedLive {
		t.Fatalf("occupancy %d, want live", occ)
	}
	latest := writeOtherNamespaceSlot(t, reg, 1, time.Now())
	if _, err := reg.GetActiveNodes(); err != nil {
		t.Fatal(err)
	}
	silence(reg, 1, latest, 2*HeartbeatTimeout)

	if _, occ := reg.Occupant(1, false); occ != OccupiedFailing {
		t.Errorf("occupancy %d, want failing: the slot's heartbeat has not changed for %v", occ, 2*HeartbeatTimeout)
	}
}
