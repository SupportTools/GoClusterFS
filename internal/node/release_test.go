package node

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"
)

// A clean unmount empties its slot, so once every node stopped, a node of
// another host joins an unfenced volume without counting it. Orphans or a
// peer's mark keep the slot for a peer to recover.
func TestReleaseEmptiesOnlyACleanSlot(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	ctx := context.Background()
	register := func(host string) (*Registry, int) {
		t.Helper()
		reg := NewRegistry(dev, 0)
		slot, err := reg.Register(ctx, host)
		if err != nil {
			t.Fatal(err)
		}
		return reg, slot
	}
	peersOf := func(reg *Registry) []int {
		t.Helper()
		peers, err := reg.MountedPeers()
		if err != nil {
			t.Fatal(err)
		}
		return slices.Sorted(maps.Keys(peers))
	}

	a, slotA := register("host-a")
	b, _ := register("host-b")
	if got := peersOf(b); !slices.Equal(got, []int{slotA}) {
		t.Fatalf("before the release host-b sees peers %v, want [%d]", got, slotA)
	}
	if err := a.Release(); err != nil {
		t.Fatalf("Release of a clean slot: %v", err)
	}
	if got := peersOf(b); len(got) != 0 {
		t.Errorf("after host-a's clean unmount host-b sees peers %v, want none", got)
	}

	c, slotC := register("host-c")
	if err := c.AddOrphan(Orphan{Inode: 42, Birth: 1}); err != nil {
		t.Fatal(err)
	}
	if err := c.Release(); err == nil {
		t.Errorf("Release emptied slot %d, which lists a file unlinked while open", slotC)
	}
	if got := peersOf(b); !slices.Contains(got, slotC) {
		t.Errorf("slot %d with an orphan no longer counts as mounted: peers %v", slotC, got)
	}

	d, slotD := register("host-d")
	dead, err := d.readSlot(slotD)
	if err != nil {
		t.Fatal(err)
	}
	dead.State = NodeStateDead
	if err := d.writeSlot(slotD, dead); err != nil {
		t.Fatal(err)
	}
	if err := d.Release(); !errors.Is(err, ErrSlotLost) {
		t.Errorf("Release of a slot a peer marked Dead = %v, want ErrSlotLost", err)
	}
	if got, err := d.readSlot(slotD); err != nil || got.State != NodeStateDead {
		t.Errorf("after Release slot %d reads %v, %v; want the peer's Dead mark", slotD, got, err)
	}
}
