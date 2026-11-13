package lock

import (
	"testing"
	"time"
)

// TestSectorLocksDoNotWaitOnOtherLocks has one node hold inode locks spread
// over the lock records, then another node take inode table sector locks. Sector
// locks have records of their own, so none may wait: a sector lock waiting on a
// record held for another lock type could close a cycle of waits between nodes.
func TestSectorLocksDoNotWaitOnOtherLocks(t *testing.T) {
	const region = 256 * RowSize
	dev, cleanup := createTestDevice(t, region)
	defer cleanup()
	a := newTestNode(dev, 0, region, 1)
	b := newTestNode(dev, 0, region, 2)

	const inodes, sectors = 1024, 64
	for id := uint64(0); id < inodes; id++ {
		requireAcquired(t, b, LockTypeInode, id, LockModeExclusive)
	}
	defer func() {
		for id := uint64(0); id < inodes; id++ {
			b.Release(LockTypeInode, id)
		}
	}()

	for s := uint64(0); s < sectors; s++ {
		if err := acquireWithin(a, 200*time.Millisecond, LockTypeInodeSector, s, LockModeExclusive); err != nil {
			t.Fatalf("sector %d lock waited on a record node B holds for inode locks: %v", s, err)
		}
		a.Release(LockTypeInodeSector, s)
	}
}
