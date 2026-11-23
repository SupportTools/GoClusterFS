package inode

import (
	"context"
	"encoding/binary"
	"errors"
	"runtime"
	"sort"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
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
		if _, err := claimFree(ctx, table); errors.Is(err, ErrInodeFull) {
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
		ino, err := claimFree(ctx, table)
		if err != nil {
			t.Fatalf("claim with free inodes left: %v", err)
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

// Each create used to reread every used inode from this node's start, so its
// cost grew with the file count: 12 MB per create with 44k files.
func TestFindFreeSlotReadsFromWhereItLeftOff(t *testing.T) {
	const total, used = 65536, 40000
	dev, cleanup := createTestDevice(t, total*InodeSize)
	defer cleanup()
	ctx := context.Background()

	buf := storage.NewAlignedBuffer(used * InodeSize)
	for num := InodeFirstUsable; num < used; num++ {
		binary.LittleEndian.PutUint32(buf.Bytes()[num*InodeSize:], InodeMagic)
	}
	if _, err := dev.WriteAt(buf.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
	table := NewTable(dev, 0, total*InodeSize, nil, [16]byte{1})
	if _, err := claimFree(ctx, table); err != nil {
		t.Fatal(err)
	}

	counting, reads := false, 0
	dev.SetReadFault(func(int64) error {
		if counting {
			reads++
		}
		return nil
	})
	for i := range 200 {
		counting, reads = true, 0
		num, err := table.FindFreeSlot()
		counting = false
		if err != nil {
			t.Fatal(err)
		}
		if want := uint32(used + 1 + i); num != want {
			t.Fatalf("create %d got inode %d, want %d", i, num, want)
		}
		if reads > 2 {
			t.Fatalf("create %d read the inode table %d times, want at most 2: it rescanned the %d used inodes", i, reads, used)
		}
		if _, err := table.ClaimSlot(ctx, num); err != nil {
			t.Fatal(err)
		}
	}
}

// Scans start at this node's start (inode 33 here) and resume after the last
// free inode they found.
func TestFindFreeSlotFindsInodesAnotherNodeFreed(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	ctx := context.Background()

	a := NewTable(dev, 0, 64*InodeSize, nil, [16]byte{1})
	b := NewTable(dev, 0, 64*InodeSize, nil, [16]byte{2})
	a.SetAllocationStart(1, 2)
	for {
		if _, err := claimFree(ctx, a); errors.Is(err, ErrInodeFull) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}

	if err := b.Free(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if num, err := a.FindFreeSlot(); err != nil || num != 10 {
		t.Fatalf("FindFreeSlot = %d, %v; want inode 10, which the other node freed behind this node's last find", num, err)
	}
	if _, err := a.ClaimSlot(ctx, 10); err != nil {
		t.Fatal(err)
	}

	if err := b.Free(ctx, 20); err != nil {
		t.Fatal(err)
	}
	if err := a.Free(ctx, 50); err != nil {
		t.Fatal(err)
	}
	if num, err := a.FindFreeSlot(); err != nil || num != 50 {
		t.Errorf("FindFreeSlot = %d, %v; want inode 50, the first free inode from this node's start", num, err)
	}
}

func TestFindFreeSlotStartsOverForInodesOtherNodesFreed(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	ctx := context.Background()

	a := NewTable(dev, 0, 64*InodeSize, nil, [16]byte{1})
	b := NewTable(dev, 0, 64*InodeSize, nil, [16]byte{2})
	for range 20 {
		if _, err := claimFree(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Free(ctx, 5); err != nil {
		t.Fatal(err)
	}
	if num, err := a.FindFreeSlot(); err != nil || num != 23 {
		t.Fatalf("FindFreeSlot = %d, %v; want inode 23 before its next start over", num, err)
	}

	a.restartEvery = 0
	if num, err := a.FindFreeSlot(); err != nil || num != 5 {
		t.Errorf("FindFreeSlot = %d, %v; want inode 5, which the other node freed", num, err)
	}
}

// FreeCount reflects this node's claims and frees at once, and another node's
// once its estimate is older than recountAfter.
func TestFreeCountIsCachedAndAdjusted(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	ctx := context.Background()
	local := NewTable(dev, 0, 64*InodeSize, nil, [16]byte{1})
	peer := NewTable(dev, 0, 64*InodeSize, nil, [16]byte{2})

	usable := uint64(64 - InodeFirstUsable)
	if got := local.FreeCount(); got != usable {
		t.Fatalf("FreeCount = %d, want %d", got, usable)
	}
	for _, num := range []uint32{10, 11, 12} {
		if _, err := local.ClaimSlot(ctx, num); err != nil {
			t.Fatal(err)
		}
	}
	if err := local.Free(ctx, 11); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ClaimSlot(ctx, 20); err != nil {
		t.Fatal(err)
	}
	if got := local.FreeCount(); got != usable-2 {
		t.Fatalf("FreeCount after 3 local claims, 1 local free and 1 peer claim = %d, want the cached %d", got, usable-2)
	}

	local.countedAt.Store(0)
	local.FreeCount()
	for local.counting.Load() {
		runtime.Gosched()
	}
	if got := local.FreeCount(); got != usable-3 {
		t.Errorf("FreeCount after the refresh = %d, want %d", got, usable-3)
	}
}
