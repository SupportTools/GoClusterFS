package node

import (
	"context"
	"net/netip"
	"slices"
	"testing"
)

func TestSlotKeepsItsAdvertisedAddress(t *testing.T) {
	for _, addr := range []netip.AddrPort{
		netip.MustParseAddrPort("10.0.0.7:24731"),
		netip.MustParseAddrPort("[fd00::7]:9000"),
		{},
	} {
		got, err := deserializeSlot(serializeSlot(&Slot{Magic: SlotMagic, State: NodeStateActive, Addr: addr, Orphans: []Orphan{{1, 2}}}))
		if err != nil {
			t.Fatal(err)
		}
		if got.Addr != addr {
			t.Errorf("slot address %v came back as %v", addr, got.Addr)
		}
	}
}

// Older builds keep up to MaxOrphans entries where the address goes, so a slot
// holds the address only while its orphan list leaves room, and never loses an orphan for it.
func TestSlotAddressYieldsToALongOrphanList(t *testing.T) {
	addr := netip.MustParseAddrPort("10.0.0.7:24731")
	for _, n := range []int{addrOrphans, addrOrphans + 1, MaxOrphans} {
		orphans := make([]Orphan, n)
		for i := range orphans {
			orphans[i] = Orphan{Inode: uint32(1000 + i), Birth: 0xFFFFFFFF}
		}
		got, err := deserializeSlot(serializeSlot(&Slot{Magic: SlotMagic, Addr: addr, Orphans: orphans}))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got.Orphans, orphans) {
			t.Errorf("%d orphans: got %d back", n, len(got.Orphans))
		}
		if want := n <= addrOrphans; got.Addr.IsValid() != want {
			t.Errorf("%d orphans: address %v, want present = %v", n, got.Addr, want)
		}
	}
}

// A node whose orphan list grew past the room for its address advertises it
// again once the list shrinks, on its next orphan edit or heartbeat.
func TestAdvertisedAddressComesBackWhenTheOrphanListShrinks(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	addr := netip.MustParseAddrPort("10.0.0.7:24731")
	reg := NewRegistry(dev, 0)
	reg.SetAdvertiseAddr(addr)
	slot, err := reg.Register(context.Background(), "node")
	if err != nil {
		t.Fatal(err)
	}
	advertised := func() netip.AddrPort {
		t.Helper()
		s, err := reg.readSlot(slot)
		if err != nil {
			t.Fatal(err)
		}
		return s.Addr
	}

	for i := range addrOrphans + 1 {
		if err := reg.AddOrphan(Orphan{Inode: uint32(100 + i), Birth: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if got := advertised(); got.IsValid() {
		t.Fatalf("with %d orphans the slot advertises %v, want no address", addrOrphans+1, got)
	}
	if err := reg.DropOrphans(func(o Orphan) bool { return o.Inode == 100 }); err != nil {
		t.Fatal(err)
	}
	if got := advertised(); got != addr {
		t.Errorf("after the orphan list shrank the slot advertises %v, want %v", got, addr)
	}

	s, err := reg.readSlot(slot)
	if err != nil {
		t.Fatal(err)
	}
	s.Addr = netip.AddrPort{}
	if err := reg.writeSlot(slot, s); err != nil {
		t.Fatal(err)
	}
	if err := reg.beat(slot); err != nil {
		t.Fatal(err)
	}
	if got := advertised(); got != addr {
		t.Errorf("after a heartbeat the slot advertises %v, want %v", got, addr)
	}
}
