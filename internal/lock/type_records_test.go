package lock

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// crossedKeys finds inodes a, c and extents b, d with b in c's record and d in a's: a wait cycle between two nodes.
func crossedKeys(m *Manager) (a, b, c, d uint64, ok bool) {
	extentIn := map[uint64]uint64{}
	for id := uint64(0); id < 1<<16; id++ {
		r := m.hashToRecord(LockTypeExtent, id)
		if _, seen := extentIn[r]; !seen {
			extentIn[r] = id
		}
	}
	extentFor := func(inode uint64) (uint64, bool) {
		e, ok := extentIn[m.hashToRecord(LockTypeInode, inode)]
		return e, ok
	}

	for a = 1; a < 1<<16; a++ {
		if d, ok = extentFor(a); ok {
			break
		}
	}
	if !ok {
		return 0, 0, 0, 0, false
	}
	for c = a + 1; c < 1<<16; c++ {
		if m.hashToRecord(LockTypeInode, c) == m.hashToRecord(LockTypeInode, a) {
			continue
		}
		if b, ok = extentFor(c); ok {
			return a, b, c, d, true
		}
	}
	return 0, 0, 0, 0, false
}

func TestInodeAndExtentLocksDoNotWaitInACycle(t *testing.T) {
	const region = 256 * RowSize
	dev, cleanup := createTestDevice(t, region)
	defer cleanup()
	x := newTestNode(dev, 0, region, 1)
	y := newTestNode(dev, 0, region, 2)

	a, b, c, d, ok := crossedKeys(x)
	if !ok {
		t.Log("no extent lock shares a record with an inode lock")
		return
	}

	var held sync.WaitGroup
	held.Add(2)
	write := func(m *Manager, inode, extent uint64) error {
		err := acquireWithin(m, time.Second, LockTypeInode, inode, LockModeExclusive)
		held.Done()
		if err != nil {
			return fmt.Errorf("inode %d: %w", inode, err)
		}
		defer m.Release(LockTypeInode, inode)
		held.Wait()
		if err := acquireWithin(m, 2*time.Second, LockTypeExtent, extent, LockModeExclusive); err != nil {
			return fmt.Errorf("extent %d while holding inode %d: %w", extent, inode, err)
		}
		return m.Release(LockTypeExtent, extent)
	}

	errs := make([]error, 2)
	var done sync.WaitGroup
	done.Add(2)
	go func() { defer done.Done(); errs[0] = write(x, a, b) }()
	go func() { defer done.Done(); errs[1] = write(y, c, d) }()
	done.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("node %d: %v", i+1, err)
		}
	}
}

func TestLockTypesHaveTheirOwnRecords(t *testing.T) {
	var rows uint64
	for _, n := range typeRows {
		rows += n
	}
	if rows != layoutRows || layoutRows*RowSize != superblock.LockRegionSize {
		t.Fatalf("types share %d rows of %d bytes, a volume has %d bytes of lock records", rows, RowSize, superblock.LockRegionSize)
	}

	m := &Manager{size: layoutRows * RowSize}
	owner := map[uint64]LockType{}
	for lt := LockType(0); int(lt) < numLockTypes; lt++ {
		for id := uint64(0); id < 1<<12; id++ {
			r := m.hashToRecord(lt, id)
			if other, ok := owner[r]; ok && other != lt {
				t.Fatalf("type %d resource %d maps to record %d, which type %d also uses", lt, id, r, other)
			}
			owner[r] = lt
		}
	}
}

// Node B holds a lock in every inode record; node A's locks of other types must not wait on any of them.
func TestLocksDoNotWaitOnRecordsOfOtherTypes(t *testing.T) {
	const region = layoutRows * RowSize
	dev, cleanup := createTestDevice(t, region)
	defer cleanup()
	a := newTestNode(dev, 0, region, 1)
	b := newTestNode(dev, 0, region, 2)

	inodeIn := map[uint64]uint64{}
	for id := uint64(0); len(inodeIn) < int(typeRows[LockTypeInode]); id++ {
		r := b.hashToRecord(LockTypeInode, id)
		if _, ok := inodeIn[r]; !ok {
			inodeIn[r] = id
		}
	}
	rows := slices.Sorted(maps.Keys(inodeIn))
	for _, r := range rows {
		requireAcquired(t, b, LockTypeInode, inodeIn[r], LockModeExclusive)
	}
	defer func() {
		for _, r := range rows {
			b.Release(LockTypeInode, inodeIn[r])
		}
	}()

	for lt := LockType(0); int(lt) < numLockTypes; lt++ {
		if lt == LockTypeInode {
			continue
		}
		for id := uint64(0); id < 64; id++ {
			if err := acquireWithin(a, 200*time.Millisecond, lt, id, LockModeExclusive); err != nil {
				t.Fatalf("type %d resource %d waited on a record node B holds for inode locks: %v", lt, id, err)
			}
			a.Release(lt, id)
		}
	}
}

func TestLocksOfOneTypeAreTakenInRecordOrder(t *testing.T) {
	const region = layoutRows * RowSize
	dev, cleanup := createTestDevice(t, region)
	defer cleanup()
	m := newTestNode(dev, 0, region, 1)

	low, high := uint64(1), uint64(2)
	for m.hashToRecord(LockTypeInode, high) == m.hashToRecord(LockTypeInode, low) {
		high++
	}
	if m.hashToRecord(LockTypeInode, low) > m.hashToRecord(LockTypeInode, high) {
		low, high = high, low
	}
	requireAcquired(t, m, LockTypeInode, high, LockModeExclusive)
	if err := acquireWithin(m, time.Second, LockTypeInode, low, LockModeExclusive); !errors.Is(err, ErrDeadlockRisk) {
		t.Fatalf("inode %d below held inode %d: err = %v, want ErrDeadlockRisk", low, high, err)
	}
	m.Release(LockTypeInode, high)

	requireAcquired(t, m, LockTypeInode, low, LockModeExclusive)
	requireAcquired(t, m, LockTypeInode, high, LockModeExclusive)
	m.Release(LockTypeInode, high)
	m.Release(LockTypeInode, low)
}

func TestAcquireRefusesUnknownLockType(t *testing.T) {
	const region = layoutRows * RowSize
	dev, cleanup := createTestDevice(t, region)
	defer cleanup()
	m := newTestNode(dev, 0, region, 1)
	if err := acquireWithin(m, time.Second, LockType(numLockTypes), 1, LockModeExclusive); err == nil {
		t.Fatal("Acquire of a lock type outside the record layout succeeded")
	}
}
