package lock

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const protoRegion = 8 * RowSize

// slowSlotWrites delays each of m's slot writes by up to max, as a slow device would.
func slowSlotWrites(m *Manager, max time.Duration) {
	m.casWrite = func(buf []byte, offset int64, _ time.Time) (int, error) {
		time.Sleep(time.Duration(rand.Int63n(int64(max))))
		return m.dev.WriteAt(buf, offset)
	}
}

// Six nodes take one write lock over and over while every slot write is
// delayed by up to 30ms, longer than #29's 20ms bound. Two holders at once
// would show up as inside > 1.
func TestSlotClaimsExcludeUnderSlowWrites(t *testing.T) {
	dev, cleanup := createTestDevice(t, protoRegion)
	defer cleanup()
	var inside, worst, acquired atomic.Int64
	stop := time.Now().Add(3 * time.Second)
	var wg sync.WaitGroup
	for n := 1; n <= 6; n++ {
		m := newTestNode(dev, 0, protoRegion, n)
		slowSlotWrites(m, 30*time.Millisecond)
		wg.Go(func() {
			for time.Now().Before(stop) {
				if err := m.Acquire(context.Background(), LockTypeInode, 50, LockModeExclusive); err != nil {
					t.Errorf("node %d Acquire: %v", n, err)
					return
				}
				now := inside.Add(1)
				for cur := worst.Load(); now > cur && !worst.CompareAndSwap(cur, now); cur = worst.Load() {
				}
				acquired.Add(1)
				time.Sleep(time.Millisecond)
				inside.Add(-1)
				if err := m.Release(LockTypeInode, 50); err != nil {
					t.Errorf("node %d Release: %v", n, err)
				}
				m.ReleaseCached()
			}
		})
	}
	wg.Wait()
	if worst.Load() > 1 {
		t.Fatalf("%d nodes held the write lock at once", worst.Load())
	}
	t.Logf("%d acquires by 6 nodes, %d claim conflicts", acquired.Load(), claimConflicts.Load())
}

// A slot that fails its checksum, such as one torn by a crash mid-write,
// counts as a claim, so no node takes the lock past it.
func TestTornSlotCountsAsClaim(t *testing.T) {
	dev, cleanup := createTestDevice(t, protoRegion)
	defer cleanup()
	a := newTestNode(dev, 0, protoRegion, 1)
	b := newTestNode(dev, 0, protoRegion, 2)
	recordID := a.hashToRecord(LockTypeInode, 51)
	torn := encodeSlot(&claimSlot{mode: LockModeShared, owner: b.nodeUUID, acquireTime: time.Now().Unix()})
	torn[100] ^= 0xFF
	if _, err := dev.WriteAt(torn, b.slotOffset(recordID, b.nodeSlot)); err != nil {
		t.Fatal(err)
	}
	requireBlocked(t, a, LockTypeInode, 51, LockModeShared)
	if err := b.writeSlotAt(recordID, b.nodeSlot, nil); err != nil {
		t.Fatal(err)
	}
	requireAcquired(t, a, LockTypeInode, 51, LockModeShared)
}

// Readers on two nodes hold one record together; a writer waits for both.
func TestSlotReadersShareWritersWait(t *testing.T) {
	dev, cleanup := createTestDevice(t, protoRegion)
	defer cleanup()
	a := newTestNode(dev, 0, protoRegion, 1)
	b := newTestNode(dev, 0, protoRegion, 2)
	w := newTestNode(dev, 0, protoRegion, 3)
	requireAcquired(t, a, LockTypeInode, 52, LockModeShared)
	requireAcquired(t, b, LockTypeInode, 52, LockModeShared)
	requireBlocked(t, w, LockTypeInode, 52, LockModeExclusive)
	for _, m := range []*Manager{a, b} {
		if err := m.Release(LockTypeInode, 52); err != nil {
			t.Fatal(err)
		}
		m.ReleaseCached()
	}
	requireAcquired(t, w, LockTypeInode, 52, LockModeExclusive)
}

// Four nodes contend for one write lock with no delays; random backoff must let
// every node finish its acquires, not leave them all backing off forever.
func TestContendedSlotClaimsAllFinish(t *testing.T) {
	dev, cleanup := createTestDevice(t, protoRegion)
	defer cleanup()
	start := time.Now()
	before := claimConflicts.Load()
	var wg sync.WaitGroup
	for n := 1; n <= 4; n++ {
		m := newTestNode(dev, 0, protoRegion, n)
		wg.Go(func() {
			for range 25 {
				if err := acquireWithin(m, 10*time.Second, LockTypeInode, 53, LockModeExclusive); err != nil {
					t.Errorf("node %d: %v", n, err)
					return
				}
				if err := m.Release(LockTypeInode, 53); err != nil {
					t.Errorf("node %d Release: %v", n, err)
				}
				m.ReleaseCached()
			}
		})
	}
	wg.Wait()
	t.Logf("100 contended acquires by 4 nodes took %v with %d claim conflicts", time.Since(start).Round(time.Millisecond), claimConflicts.Load()-before)
}

func BenchmarkSlotFreshAcquireRelease(b *testing.B) {
	dev, cleanup := createTestDevice(b, protoRegion)
	defer cleanup()
	m := newTestNode(dev, 0, protoRegion, 1)
	ctx := context.Background()
	for b.Loop() {
		if err := m.Acquire(ctx, LockTypeInode, 100, LockModeExclusive); err != nil {
			b.Fatal(err)
		}
		m.Release(LockTypeInode, 100)
		m.ReleaseCached()
	}
}
