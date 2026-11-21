package node

import (
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
