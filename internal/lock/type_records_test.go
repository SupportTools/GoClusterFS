package lock

import (
	"fmt"
	"sync"
	"testing"
	"time"
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
