package lock

import (
	"testing"
	"time"
)

func shortenStaleness(t *testing.T) {
	t.Helper()
	oldStale, oldRenew := staleAfter, renewAfter
	t.Cleanup(func() { staleAfter, renewAfter = oldStale, oldRenew })
	staleAfter, renewAfter = 300*time.Millisecond, 100*time.Millisecond
}

func TestLongHoldStaysFreshWithoutNewAcquires(t *testing.T) {
	shortenStaleness(t)
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	holder := newTestNode(dev, 0, 1<<20, 1)
	peer := newTestNode(dev, 0, 1<<20, 2)
	holder.Start()
	defer holder.Stop()

	requireAcquired(t, holder, LockTypeInode, 2, LockModeExclusive)
	time.Sleep(3 * staleAfter)
	requireBlocked(t, peer, LockTypeInode, 2, LockModeExclusive)

	if err := holder.Release(LockTypeInode, 2); err != nil {
		t.Fatal(err)
	}
	holder.ReleaseCached()
	requireAcquired(t, peer, LockTypeInode, 2, LockModeExclusive)
}

func TestAdoptedSlotsStayFreshUntilReleased(t *testing.T) {
	shortenStaleness(t)
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()
	dead := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	replayer := NewManager(dev, 0, reclaimRegion, [16]byte{3}, 3)
	peer := NewManager(dev, 0, reclaimRegion, [16]byte{4}, 4)
	replayer.Start()
	defer replayer.Stop()

	requireAcquired(t, dead, LockTypeInode, 2, LockModeExclusive)
	adopted, err := replayer.AdoptOwner([16]byte{1}, 1)
	if err != nil || len(adopted) != 1 {
		t.Fatalf("AdoptOwner = %v, %v; want the dead node's record", adopted, err)
	}
	time.Sleep(3 * staleAfter)
	requireBlocked(t, peer, LockTypeInode, 2, LockModeExclusive)

	if err := replayer.ReleaseAdopted(adopted); err != nil {
		t.Fatal(err)
	}
	requireAcquired(t, peer, LockTypeInode, 2, LockModeExclusive)
}
