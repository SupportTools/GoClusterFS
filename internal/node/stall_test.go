package node

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// stalledNode is a registered node whose device writes pass its write gate and
// which records each slot loss it reports.
type stalledNode struct {
	reg    *Registry
	slot   int
	mu     sync.Mutex
	losses []SlotLoss
}

func registerStalledNode(t *testing.T, dev *storage.Device) *stalledNode {
	t.Helper()
	n := &stalledNode{reg: NewRegistry(dev, 0)}
	slot, err := n.reg.Register(context.Background(), "stalled")
	if err != nil {
		t.Fatal(err)
	}
	n.slot = slot
	n.reg.SetSlotLostHandler(func(l SlotLoss) {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.losses = append(n.losses, l)
	})
	dev.SetWriteGate(n.reg.WriteGate)
	return n
}

func (n *stalledNode) stopped(t *testing.T) {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.losses) != 1 {
		t.Errorf("slot-lost handler called %d times, want once: %v", len(n.losses), n.losses)
	}
}

// stallAfterBeatRead pauses the node's next heartbeat after its slot read, ages
// its last beat by gap as a stall that long would, and runs during while the
// heartbeat waits. It returns what the heartbeat returned.
func (n *stalledNode) stallAfterBeatRead(t *testing.T, gap time.Duration, during func()) error {
	t.Helper()
	paused, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	afterBeatRead = func() { once.Do(func() { close(paused); <-resume }) }
	t.Cleanup(func() { afterBeatRead = nil })
	done := make(chan error, 1)
	go func() { done <- n.reg.beat(n.slot) }()
	<-paused
	n.reg.lastBeat.Store(int64(bootClock() - gap))
	during()
	close(resume)
	return <-done
}

// stallFor makes the node's last heartbeat age old, by its own clock and in its slot.
func (n *stalledNode) stallFor(t *testing.T, age time.Duration) {
	t.Helper()
	n.reg.lastBeat.Store(int64(bootClock() - age))
	slot, err := n.reg.readSlot(n.slot)
	if err != nil {
		t.Fatal(err)
	}
	slot.HeartbeatTime = time.Now().Add(-age).Unix()
	if err := n.reg.writeSlot(n.slot, slot); err != nil {
		t.Fatal(err)
	}
}

// resumesAndStops resumes the node's heartbeat after newcomer took its slot,
// and checks that the node stops and newcomer keeps the slot.
func (n *stalledNode) resumesAndStops(t *testing.T, newcomer *Registry) {
	t.Helper()
	if err := n.reg.beat(n.slot); !errors.Is(err, ErrSlotLost) {
		t.Errorf("stalled node's heartbeat after its slot was taken = %v, want ErrSlotLost", err)
	}
	n.stopped(t)
	if err := newcomer.beat(n.slot); err != nil {
		t.Errorf("new mount's heartbeat after the stalled node resumed: %v", err)
	}
}

// refusesDataWrites checks that a data write fails with ErrSlotLost and leaves the device unchanged.
func refusesDataWrites(t *testing.T, dev *storage.Device, offset int64) {
	t.Helper()
	data := storage.NewAlignedBuffer(storage.SectorSize)
	copy(data.Bytes(), bytes.Repeat([]byte{'x'}, storage.SectorSize))
	if _, err := dev.WriteAt(data.Bytes(), offset); !errors.Is(err, ErrSlotLost) {
		t.Errorf("data write at %d after the stall = %v, want ErrSlotLost", offset, err)
	}
	got := storage.NewAlignedBuffer(storage.SectorSize)
	if _, err := dev.ReadAt(got.Bytes(), offset); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), make([]byte, storage.SectorSize)) {
		t.Errorf("a data write at %d reached the device after the stall", offset)
	}
}

func releaseSlot(t *testing.T, dev *storage.Device, slot int) {
	t.Helper()
	peer := NewRegistry(dev, 0)
	raw, _, err := peer.readSlotRaw(slot)
	if err != nil {
		t.Fatal(err)
	}
	if released, err := peer.ReleaseSlot(slot, raw, func(func() error) error { return nil }); !released || err != nil {
		t.Fatalf("peer releases slot %d: %v, %v", slot, released, err)
	}
}

// A heartbeat that read its slot and then stalled while a peer replayed this
// node and emptied the slot must not write its stale copy back: the node stops,
// and no data write passes the gate.
func TestStalledBeatStopsWithoutRewritingAReplayedSlot(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	n := registerStalledNode(t, dev)

	writeDone := make(chan struct{})
	err := n.stallAfterBeatRead(t, deadAfter+5*time.Second, func() {
		releaseSlot(t, dev, n.slot)
		// Other goroutines resume with the heartbeat; their writes reach the gate too.
		go func() {
			defer close(writeDone)
			refusesDataWrites(t, dev, 1<<20)
		}()
	})
	<-writeDone
	if !errors.Is(err, ErrSlotLost) {
		t.Errorf("stalled heartbeat = %v, want ErrSlotLost", err)
	}
	n.stopped(t)
	if slot, err := NewRegistry(dev, 0).readSlot(n.slot); err != nil || slot.State != NodeStateEmpty {
		t.Errorf("slot after the stalled heartbeat: %+v, %v; want it left empty", slot, err)
	}
	refusesDataWrites(t, dev, 2<<20)
}

// A node that registered in the slot while the old owner's heartbeat stalled
// keeps it, and the old owner stops.
func TestStalledBeatLeavesNewOwnerInItsSlot(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	n := registerStalledNode(t, dev)

	newcomer := NewRegistry(dev, 0)
	err := n.stallAfterBeatRead(t, deadAfter+5*time.Second, func() {
		releaseSlot(t, dev, n.slot)
		got, err := newcomer.Register(context.Background(), "newcomer")
		if err != nil || got != n.slot {
			t.Fatalf("new node registered in slot %d (%v), want the released slot %d", got, err, n.slot)
		}
	})
	if !errors.Is(err, ErrSlotLost) {
		t.Errorf("stalled heartbeat = %v, want ErrSlotLost", err)
	}
	n.stopped(t)
	if slot, err := newcomer.readSlot(n.slot); err != nil || slot.UUID != newcomer.LocalUUID() || slot.State != NodeStateActive {
		t.Errorf("slot after the old owner's stalled heartbeat: %+v, %v; want the new node's", slot, err)
	}
	if err := newcomer.beat(n.slot); err != nil {
		t.Errorf("new node's heartbeat after the old owner resumed: %v", err)
	}
}

// A heartbeat whose write completes more than stallLimit after the last one
// may have landed after a peer replayed this node, so the node stops.
func TestHeartbeatLandingLateStopsTheNode(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	n := registerStalledNode(t, dev)
	dev.SetWriteObserver(func(offset int64, _ int) {
		if offset < MaxNodes*SlotSize {
			n.reg.lastBeat.Store(int64(bootClock() - deadAfter - 5*time.Second))
		}
	})

	if err := n.reg.beat(n.slot); !errors.Is(err, ErrSlotLost) {
		t.Errorf("heartbeat that landed late = %v, want ErrSlotLost", err)
	}
	dev.SetWriteObserver(nil)
	n.stopped(t)
	refusesDataWrites(t, dev, 1<<20)
}

// What fencing has to close: a stall after the last check lets the heartbeat's
// one write land over a node that registered in the emptied slot meanwhile.
// Neither node stays up: the old one stops on the late write, and the new one
// on its next heartbeat, which finds the slot no longer its own.
func TestHeartbeatLandingLateOverANewOwnerStopsBothNodes(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	n := registerStalledNode(t, dev)
	newcomer := NewRegistry(dev, 0)
	var stalled atomic.Bool
	dev.SetWriteObserver(func(offset int64, _ int) {
		if offset >= MaxNodes*SlotSize || !stalled.CompareAndSwap(false, true) {
			return
		}
		releaseSlot(t, dev, n.slot)
		if got, err := newcomer.Register(context.Background(), "newcomer"); err != nil || got != n.slot {
			t.Errorf("new node registered in slot %d (%v), want the released slot %d", got, err, n.slot)
		}
		n.reg.lastBeat.Store(int64(bootClock() - deadAfter - 5*time.Second))
	})

	if err := n.reg.beat(n.slot); !errors.Is(err, ErrSlotLost) {
		t.Errorf("heartbeat that landed late = %v, want ErrSlotLost", err)
	}
	dev.SetWriteObserver(nil)
	n.stopped(t)
	refusesDataWrites(t, dev, 1<<20)
	if err := newcomer.beat(n.slot); !errors.Is(err, ErrSlotLost) {
		t.Errorf("new node's heartbeat after the late write landed over it = %v, want ErrSlotLost", err)
	}
}

// A 12s stall, as in a storage path failover, ends before any peer may replay
// the node, so its heartbeat writes and the node keeps writing data.
func TestShortStalledBeatKeepsTheNodeUp(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	n := registerStalledNode(t, dev)

	if err := n.stallAfterBeatRead(t, 12*time.Second, func() {}); err != nil {
		t.Errorf("heartbeat after a 12s stall = %v, want nil", err)
	}
	n.mu.Lock()
	if len(n.losses) != 0 {
		t.Errorf("slot-lost handler called after a 12s stall: %v", n.losses)
	}
	n.mu.Unlock()
	data := storage.NewAlignedBuffer(storage.SectorSize)
	if _, err := dev.WriteAt(data.Bytes(), 1<<20); err != nil {
		t.Errorf("data write after a 12s stall: %v", err)
	}
}

// A new mount on the same host watches a stalled node's slot before it takes
// it, so a node back from a stall just under stallLimit keeps its slot, and
// both nodes keep running.
func TestShortStallKeepsSlotFromSameHostMount(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	n := registerStalledNode(t, dev)
	if n.reg.bootID == ([16]byte{}) {
		t.Skip("no kernel boot ID")
	}

	newcomer := NewRegistry(dev, 0)
	registered := make(chan int, 1)
	err := n.stallAfterBeatRead(t, stallLimit-3*time.Second, func() {
		peer := NewRegistry(dev, 0)
		slot, err := peer.readSlot(n.slot)
		if err != nil {
			t.Fatal(err)
		}
		slot.HeartbeatTime = time.Now().Add(-stallLimit).Unix()
		if err := peer.writeSlot(n.slot, slot); err != nil {
			t.Fatal(err)
		}
		go func() {
			slot, err := newcomer.Register(context.Background(), "stalled")
			if err != nil {
				t.Error(err)
			}
			registered <- slot
		}()
		// The stall ends while the new mount watches the slot.
		time.Sleep(time.Second)
	})
	if err != nil {
		t.Errorf("heartbeat after the stall = %v, want nil", err)
	}
	newSlot := <-registered
	if newSlot == n.slot {
		t.Errorf("same-host mount registered in the stalled node's slot %d, want another", n.slot)
	}
	n.mu.Lock()
	if len(n.losses) != 0 {
		t.Errorf("slot-lost handler called after the stall: %v", n.losses)
	}
	n.mu.Unlock()
	if err := newcomer.beat(newSlot); err != nil {
		t.Errorf("same-host mount's heartbeat after the stall: %v", err)
	}
	if _, err := dev.WriteAt(storage.NewAlignedBuffer(storage.SectorSize).Bytes(), 1<<20); err != nil {
		t.Errorf("data write after the stall: %v", err)
	}
}

// A mount that cannot prove a stalled node gone watches its slot for deadAfter
// before it takes it, however old the heartbeat looks by its clock.
func TestWatchingMountTakesStalledSlotOnlyAfterDeadAfter(t *testing.T) {
	for _, tc := range []struct {
		name  string
		host  string
		setup func(t *testing.T, newcomer *Registry, stalled int)
	}{
		{"same-host mount", "stalled", func(*testing.T, *Registry, int) {}},
		{"restarted container", "stalled", func(_ *testing.T, newcomer *Registry, _ int) {
			newcomer.pidNS++
		}},
		{"named by -skip-recovery", "elsewhere", func(_ *testing.T, newcomer *Registry, stalled int) {
			newcomer.PreferSlot(stalled)
		}},
		{"rebooted host", "stalled", func(_ *testing.T, newcomer *Registry, _ int) {
			newcomer.bootID = [16]byte{0xEE}
		}},
		{"another host with no empty slot", "elsewhere", func(t *testing.T, newcomer *Registry, stalled int) {
			fillWithReplays(t, newcomer, stalled+1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dev, cleanup := createTestDevice(t, 10<<20)
			defer cleanup()
			n := registerStalledNode(t, dev)
			if n.reg.bootID == ([16]byte{}) {
				t.Skip("no kernel boot ID")
			}
			n.stallFor(t, HeartbeatTimeout+time.Second)
			newcomer := NewRegistry(dev, 0)
			tc.setup(t, newcomer, n.slot)

			start := time.Now()
			slot, err := newcomer.Register(context.Background(), tc.host)
			if err != nil || slot != n.slot {
				t.Fatalf("Register = %d, %v; want the stalled node's slot %d", slot, err, n.slot)
			}
			if waited := time.Since(start); waited < deadAfter {
				t.Errorf("mount took the stalled node's slot after watching it %v, want at least %v", waited, deadAfter)
			}
			n.resumesAndStops(t, newcomer)
		})
	}
}
