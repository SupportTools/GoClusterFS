package node

import (
	"bytes"
	"context"
	"errors"
	"sync"
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
	stalled := time.Now().Add(-gap)
	n.reg.lastBeat.Store(&stalled)
	during()
	close(resume)
	return <-done
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

// A heartbeat whose write completes more than HeartbeatTimeout after the last
// one may have landed after a peer replayed this node, so the node stops.
func TestHeartbeatLandingLateStopsTheNode(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	n := registerStalledNode(t, dev)
	dev.SetWriteObserver(func(offset int64, _ int) {
		if offset < MaxNodes*SlotSize {
			late := time.Now().Add(-deadAfter - 5*time.Second)
			n.reg.lastBeat.Store(&late)
		}
	})

	if err := n.reg.beat(n.slot); !errors.Is(err, ErrSlotLost) {
		t.Errorf("heartbeat that landed late = %v, want ErrSlotLost", err)
	}
	dev.SetWriteObserver(nil)
	n.stopped(t)
	refusesDataWrites(t, dev, 1<<20)
}
