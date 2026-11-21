package node

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"
)

// An unfenced mount refuses to join while any other node may still write. A
// paused node's slot looks like a crashed one, so a silent slot counts; only an
// exited mount process of this host, Dead slots and empty ones do not.
func TestMountedPeersListsEveryOtherNodeThatMayWrite(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	self, err := reg.Register(context.Background(), "self")
	if err != nil {
		t.Fatal(err)
	}
	if self != 0 {
		t.Fatalf("Register took slot %d, want 0", self)
	}

	old := time.Now().Add(-3 * HeartbeatTimeout).Unix()
	slots := map[int]*Slot{
		1: {State: NodeStateActive, UUID: [16]byte{1}, HeartbeatTime: old},
		2: {State: NodeStateLeaving, UUID: [16]byte{2}, HeartbeatTime: old},
		3: {State: NodeStateDead, UUID: [16]byte{3}, HeartbeatTime: old},
		4: {State: NodeStateActive, UUID: [16]byte{4}, HeartbeatTime: old, BootID: reg.bootID, PIDNamespace: reg.pidNS, PID: uint32(exitedPID(t))},
	}
	for i, s := range slots {
		s.Magic = SlotMagic
		host := "elsewhere"
		if i == 4 {
			host = reg.hostname
		}
		copy(s.Hostname[:], host)
		if err := reg.writeSlot(i, s); err != nil {
			t.Fatal(err)
		}
	}

	peers, err := reg.MountedPeers()
	if err != nil {
		t.Fatal(err)
	}
	if got := slices.Sorted(maps.Keys(peers)); !slices.Equal(got, []int{1, 2}) {
		t.Errorf("MountedPeers = slots %v, want [1 2]: the silent Active and the Leaving node, not this node, a Dead slot or this host's exited mount", got)
	}
}
