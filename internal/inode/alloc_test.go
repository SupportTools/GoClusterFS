package inode

import (
	"context"
	"errors"
	"sort"
	"testing"
)

func TestAllocationStartSpreadsNodes(t *testing.T) {
	dev, cleanup := createTestDevice(t, 2<<20)
	defer cleanup()

	usable := uint32(4096) - InodeFirstUsable
	var starts []uint32
	for slot := 0; slot < 4; slot++ {
		table := NewTable(dev, 0, 4096*InodeSize, nil, [16]byte{byte(slot)})
		table.SetAllocationStart(slot, 64)
		num, err := table.FindFreeSlot()
		if err != nil {
			t.Fatal(err)
		}
		starts = append(starts, num)
	}
	t.Logf("first free inode per slot: %v", starts)

	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	for i := 1; i < len(starts); i++ {
		if gap := starts[i] - starts[i-1]; gap < usable/4-1 {
			t.Errorf("slots 0-3 start %d inodes apart, want at least a quarter of the table (%d)", gap, usable/4)
		}
	}
}

func TestFindFreeSlotWrapsAround(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	ctx := context.Background()

	table := NewTable(dev, 0, 64*InodeSize, nil, [16]byte{1})
	table.SetAllocationStart(1, 2)
	first, err := table.FindFreeSlot()
	if err != nil {
		t.Fatal(err)
	}
	for num := first; num < 64; num++ {
		if _, err := table.ClaimSlot(ctx, num); err != nil {
			t.Fatal(err)
		}
	}

	if num, err := table.FindFreeSlot(); err != nil || num != InodeFirstUsable {
		t.Errorf("FindFreeSlot = %d, %v; want inode %d before this node's start", num, err, InodeFirstUsable)
	}
}

func TestFindFreeSlotFindsEveryFreeInode(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	ctx := context.Background()

	table := NewTable(dev, 0, 64*InodeSize, nil, [16]byte{1})
	table.SetAllocationStart(3, 4)
	claimed := 0
	for {
		if _, err := table.Alloc(ctx); errors.Is(err, ErrInodeFull) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		claimed++
	}
	if want := 64 - int(InodeFirstUsable); claimed != want {
		t.Fatalf("allocated %d inodes, want all %d usable ones", claimed, want)
	}

	freed := []uint32{InodeFirstUsable, 40, 63}
	for _, num := range freed {
		if err := table.Free(ctx, num); err != nil {
			t.Fatal(err)
		}
	}
	var found []uint32
	for range freed {
		ino, err := table.Alloc(ctx)
		if err != nil {
			t.Fatalf("Alloc with free inodes left: %v", err)
		}
		found = append(found, ino.InodeNum)
	}
	sort.Slice(found, func(i, j int) bool { return found[i] < found[j] })
	if len(found) != 3 || found[0] != freed[0] || found[1] != freed[1] || found[2] != freed[2] {
		t.Errorf("reallocated %v, want the freed inodes %v", found, freed)
	}
	if _, err := table.FindFreeSlot(); !errors.Is(err, ErrInodeFull) {
		t.Errorf("FindFreeSlot on a full table: err = %v, want ErrInodeFull", err)
	}
}
