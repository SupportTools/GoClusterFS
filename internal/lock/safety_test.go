package lock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func newTestNode(dev *storage.Device, offset, size uint64, n int) *Manager {
	var uuid [16]byte
	copy(uuid[:], fmt.Sprintf("test-node-%06d", n))
	return NewManager(dev, offset, size, uuid, n)
}

func acquireWithin(m *Manager, d time.Duration, lt LockType, id uint64, mode LockMode) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return m.Acquire(ctx, lt, id, mode)
}

func requireBlocked(t *testing.T, m *Manager, lt LockType, id uint64, mode LockMode) {
	t.Helper()
	err := acquireWithin(m, 50*time.Millisecond, lt, id, mode)
	if err == nil {
		m.Release(lt, id)
		t.Fatalf("node %d acquired type=%d id=%d mode=%d while another holder should exclude it", m.nodeSlot, lt, id, mode)
	}
	if !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("Acquire: got %v, want ErrLockTimeout", err)
	}
}

func requireAcquired(t *testing.T, m *Manager, lt LockType, id uint64, mode LockMode) {
	t.Helper()
	if err := acquireWithin(m, time.Second, lt, id, mode); err != nil {
		t.Fatalf("node %d Acquire type=%d id=%d mode=%d: %v", m.nodeSlot, lt, id, mode, err)
	}
}

func writeTestRecord(t *testing.T, m *Manager, recordID uint64, rec *Record) {
	t.Helper()
	buf := storage.GetBuffer(RecordSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), serializeRecord(rec))
	if _, err := m.dev.WriteAt(buf.Bytes(), int64(m.offset)+int64(recordID)*RecordSize); err != nil {
		t.Fatalf("write record: %v", err)
	}
}

// ageRecord rewrites the lock's record as if it was taken age ago and never renewed.
func ageRecord(t *testing.T, m *Manager, lt LockType, id uint64, age time.Duration) {
	t.Helper()
	recordID := m.hashToRecord(lt, id)
	rec, err := m.readRecord(recordID)
	if err != nil {
		t.Fatalf("readRecord: %v", err)
	}
	rec.AcquireTime = time.Now().Add(-age).Unix()
	writeTestRecord(t, m, recordID, rec)
}

const pastStale = StaleThreshold + 2*time.Second

func TestCollidingResourceIsBlockedWhileOtherNodeHoldsRecord(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, RecordSize, 1)
	b := newTestNode(dev, 0, RecordSize, 2)

	requireAcquired(t, a, LockTypeInode, 1, LockModeExclusive)
	requireBlocked(t, b, LockTypeInode, 2, LockModeExclusive)
}

func TestNodeHoldsCollidingResourcesUntilLastRelease(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, RecordSize, 1)
	b := newTestNode(dev, 0, RecordSize, 2)

	requireAcquired(t, a, LockTypeBlockBitmap, 0, LockModeExclusive)
	requireAcquired(t, a, LockTypeInode, 7, LockModeExclusive)
	if err := a.Release(LockTypeInode, 7); err != nil {
		t.Fatalf("Release inode: %v", err)
	}
	requireBlocked(t, b, LockTypeBlockBitmap, 0, LockModeExclusive)

	if err := a.Release(LockTypeBlockBitmap, 0); err != nil {
		t.Fatalf("Release bitmap: %v", err)
	}
	requireAcquired(t, b, LockTypeBlockBitmap, 0, LockModeExclusive)
}

func TestConcurrentAcquireOnOneNodeKeepsOtherNodesOut(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)

	const workers = 4
	for iter := 0; iter < 30; iter++ {
		start := make(chan struct{})
		release := make(chan struct{})
		var acquired, done sync.WaitGroup
		errs := make(chan error, workers)
		acquired.Add(workers)
		done.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer done.Done()
				<-start
				err := acquireWithin(a, time.Second, LockTypeInode, 42, LockModeExclusive)
				acquired.Done()
				if err != nil {
					errs <- err
					return
				}
				<-release
				if err := a.Release(LockTypeInode, 42); err != nil {
					errs <- err
				}
			}()
		}
		close(start)
		acquired.Wait()
		requireBlocked(t, b, LockTypeInode, 42, LockModeExclusive)
		close(release)
		done.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("iteration %d: %v", iter, err)
		}
	}
}

func TestSharedLockHeldAfterAnotherReaderReleases(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)
	c := newTestNode(dev, 0, 1<<20, 3)

	requireAcquired(t, a, LockTypeInode, 5, LockModeShared)
	requireAcquired(t, b, LockTypeInode, 5, LockModeShared)
	if err := b.Release(LockTypeInode, 5); err != nil {
		t.Fatalf("b Release: %v", err)
	}
	requireBlocked(t, c, LockTypeInode, 5, LockModeExclusive)

	if err := a.Release(LockTypeInode, 5); err != nil {
		t.Fatalf("a Release: %v", err)
	}
	requireAcquired(t, c, LockTypeInode, 5, LockModeExclusive)
}

func TestExclusiveWaitsForSameNodeReaders(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)

	requireAcquired(t, a, LockTypeInode, 9, LockModeShared)

	writer := func() error {
		errc := make(chan error, 1)
		go func() {
			err := acquireWithin(a, 50*time.Millisecond, LockTypeInode, 9, LockModeExclusive)
			if err == nil {
				err = a.Release(LockTypeInode, 9)
			}
			errc <- err
		}()
		return <-errc
	}

	if err := writer(); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("exclusive Acquire while a reader on the same node holds the lock: got %v, want ErrLockTimeout", err)
	}
	if err := a.Release(LockTypeInode, 9); err != nil {
		t.Fatalf("Release shared: %v", err)
	}
	if err := writer(); err != nil {
		t.Fatalf("exclusive Acquire after readers left: %v", err)
	}
}

func TestSameGoroutineCannotUpgradeSharedToExclusive(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)

	requireAcquired(t, a, LockTypeInode, 9, LockModeShared)
	err := acquireWithin(a, time.Second, LockTypeInode, 9, LockModeExclusive)
	if !errors.Is(err, ErrDeadlockRisk) {
		t.Fatalf("upgrade in the same goroutine: got %v, want ErrDeadlockRisk", err)
	}
}

func TestReadersOnTwoNodesCannotBothWrite(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)

	requireAcquired(t, a, LockTypeInode, 77, LockModeShared)
	requireAcquired(t, b, LockTypeInode, 77, LockModeShared)

	errs := make(chan error, 2)
	for _, m := range []*Manager{a, b} {
		go func(m *Manager) {
			err := acquireWithin(m, 50*time.Millisecond, LockTypeInode, 77, LockModeExclusive)
			if err == nil {
				m.Release(LockTypeInode, 77)
			}
			errs <- err
		}(m)
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; !errors.Is(err, ErrLockTimeout) {
			t.Fatalf("exclusive Acquire while both nodes read: got %v, want ErrLockTimeout", err)
		}
	}
}

func TestJoiningKeyRenewsBusyRecord(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, RecordSize, 1)
	b := newTestNode(dev, 0, RecordSize, 2)
	defer func(d time.Duration) { renewAfter = d }(renewAfter)
	renewAfter = 0

	requireAcquired(t, a, LockTypeInode, 1, LockModeExclusive)
	ageRecord(t, a, LockTypeInode, 1, pastStale)
	requireAcquired(t, a, LockTypeInode, 2, LockModeExclusive)
	if err := a.Release(LockTypeInode, 1); err != nil {
		t.Fatalf("Release: %v", err)
	}

	requireBlocked(t, b, LockTypeInode, 3, LockModeExclusive)
}

func TestStaleLockIsReclaimedFromPausedHolder(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)

	requireAcquired(t, a, LockTypeBlockBitmap, 0, LockModeExclusive)
	ageRecord(t, a, LockTypeBlockBitmap, 0, pastStale)

	requireAcquired(t, b, LockTypeBlockBitmap, 0, LockModeExclusive)
	if held := a.HeldLocks(); len(held) != 1 {
		t.Fatalf("paused holder's view: got %v, want it to still list the lock (no fencing)", held)
	}
}

func TestPausedHolderReleaseKeepsReclaimedLock(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)
	c := newTestNode(dev, 0, 1<<20, 3)

	requireAcquired(t, a, LockTypeInode, 3, LockModeExclusive)
	ageRecord(t, a, LockTypeInode, 3, pastStale)
	requireAcquired(t, b, LockTypeInode, 3, LockModeExclusive)

	a.Release(LockTypeInode, 3)
	time.Sleep(cacheGrace + 50*time.Millisecond)
	requireBlocked(t, c, LockTypeInode, 3, LockModeExclusive)
}

func TestStaleSharedLockReclaimResetsReaderCount(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	dead := newTestNode(dev, 0, 1<<20, 2)
	c := newTestNode(dev, 0, 1<<20, 3)

	requireAcquired(t, dead, LockTypeInode, 4, LockModeShared)
	ageRecord(t, dead, LockTypeInode, 4, pastStale)
	requireAcquired(t, a, LockTypeInode, 4, LockModeShared)
	if err := a.Release(LockTypeInode, 4); err != nil {
		t.Fatalf("Release: %v", err)
	}
	requireAcquired(t, c, LockTypeInode, 4, LockModeExclusive)
}

func TestLockRegionStaysBoundedAcrossCycles(t *testing.T) {
	const (
		guard      = 4 * RecordSize
		regionSize = 16 * RecordSize
		resources  = 1000
		cycles     = 1500
	)
	dev, cleanup := createTestDevice(t, guard+regionSize+guard)
	defer cleanup()
	defer func(d, g time.Duration) { claimSettleDelay, cacheGrace = d, g }(claimSettleDelay, cacheGrace)
	claimSettleDelay, cacheGrace = time.Millisecond, time.Millisecond

	sentinel := storage.NewAlignedBuffer(guard)
	for i := range sentinel.Bytes() {
		sentinel.Bytes()[i] = 0xA5
	}
	for _, off := range []int64{0, guard + regionSize} {
		if _, err := dev.WriteAt(sentinel.Bytes(), off); err != nil {
			t.Fatalf("write sentinel: %v", err)
		}
	}

	nodes := []*Manager{
		newTestNode(dev, guard, regionSize, 1),
		newTestNode(dev, guard, regionSize, 2),
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(nodes))
	for n, m := range nodes {
		wg.Add(1)
		go func(n int, m *Manager) {
			defer wg.Done()
			for i := 0; i < cycles; i++ {
				id := uint64((i*7 + n*13) % resources)
				lt := LockTypeInode
				if i%3 == 0 {
					lt = LockTypeExtent
				}
				if err := acquireWithin(m, 5*time.Second, lt, id, LockModeExclusive); err != nil {
					errs <- fmt.Errorf("node %d cycle %d: %w", n, i, err)
					return
				}
				m.Release(lt, id)
			}
		}(n, m)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	check := newTestNode(dev, guard, regionSize, 3)
	for id := uint64(0); id < resources; id++ {
		for _, lt := range []LockType{LockTypeInode, LockTypeExtent} {
			requireAcquired(t, check, lt, id, LockModeExclusive)
			if err := check.Release(lt, id); err != nil {
				t.Fatalf("Release type=%d id=%d: %v", lt, id, err)
			}
		}
	}

	got := storage.NewAlignedBuffer(guard)
	for _, off := range []int64{0, guard + regionSize} {
		if _, err := dev.ReadAt(got.Bytes(), off); err != nil {
			t.Fatalf("read sentinel: %v", err)
		}
		if !bytes.Equal(got.Bytes(), sentinel.Bytes()) {
			t.Fatalf("lock manager wrote outside its region near offset %d", off)
		}
	}
}

func TestNewReadersDoNotKeepLeakedSharedLockFresh(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	leaked := newTestNode(dev, 0, 1<<20, 1)
	reader := newTestNode(dev, 0, 1<<20, 2)
	writer := newTestNode(dev, 0, 1<<20, 3)

	requireAcquired(t, leaked, LockTypeInode, 2, LockModeShared)
	ageRecord(t, leaked, LockTypeInode, 2, StaleThreshold-2*time.Second)

	requireAcquired(t, reader, LockTypeInode, 2, LockModeShared)
	if err := reader.Release(LockTypeInode, 2); err != nil {
		t.Fatalf("reader Release: %v", err)
	}
	time.Sleep(3 * time.Second)

	requireAcquired(t, writer, LockTypeInode, 2, LockModeExclusive)
}

func TestJoinFailsAfterRecordTakenOver(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, RecordSize, 1)
	b := newTestNode(dev, 0, RecordSize, 2)
	defer func(d time.Duration) { renewAfter = d }(renewAfter)
	renewAfter = 0

	requireAcquired(t, a, LockTypeInode, 1, LockModeExclusive)
	ageRecord(t, a, LockTypeInode, 1, pastStale)
	requireAcquired(t, b, LockTypeInode, 2, LockModeExclusive)

	if err := acquireWithin(a, time.Second, LockTypeInode, 3, LockModeExclusive); !errors.Is(err, ErrLockLost) {
		t.Fatalf("joining a record another node took over: got %v, want ErrLockLost", err)
	}
}

func TestClaimOverwrittenDuringSettleIsNotHeld(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)
	defer func(d time.Duration) { claimSettleDelay = d }(claimSettleDelay)
	claimSettleDelay = 100 * time.Millisecond

	errc := make(chan error, 1)
	go func() { errc <- acquireWithin(a, 50*time.Millisecond, LockTypeInode, 5, LockModeExclusive) }()
	time.Sleep(20 * time.Millisecond)

	// B read the record as free before A's claim landed, so B's claim lands on top of it.
	recordID := a.hashToRecord(LockTypeInode, 5)
	rec, err := a.readRecord(recordID)
	if err != nil {
		t.Fatalf("readRecord: %v", err)
	}
	rec.OwnerUUID, rec.OwnerSlot, rec.AcquireTime = b.nodeUUID, uint32(b.nodeSlot), time.Now().Unix()
	writeTestRecord(t, a, recordID, rec)

	if err := <-errc; err == nil {
		t.Fatal("node A kept a claim that node B overwrote during the settle wait")
	}
}

func TestReleasedHoldIsCachedForGrace(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)

	requireAcquired(t, a, LockTypeInode, 6, LockModeExclusive)
	if err := a.Release(LockTypeInode, 6); err != nil {
		t.Fatalf("Release: %v", err)
	}
	requireBlocked(t, b, LockTypeInode, 6, LockModeExclusive)

	start := time.Now()
	requireAcquired(t, b, LockTypeInode, 6, LockModeExclusive)
	if waited := time.Since(start); waited > cacheGrace+claimSettleDelay+2*RetryMaxDelay {
		t.Fatalf("other node waited %v for a cached hold, want at most about grace+settle", waited)
	}
}

func TestCachedHoldRejoinSkipsSettleWait(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	defer func(d time.Duration) { claimSettleDelay = d }(claimSettleDelay)
	claimSettleDelay = 200 * time.Millisecond

	requireAcquired(t, a, LockTypeInode, 7, LockModeExclusive)
	if err := a.Release(LockTypeInode, 7); err != nil {
		t.Fatalf("Release: %v", err)
	}
	start := time.Now()
	requireAcquired(t, a, LockTypeInode, 7, LockModeExclusive)
	if took := time.Since(start); took >= claimSettleDelay/2 {
		t.Fatalf("re-acquiring a cached hold took %v, want well under the %v settle wait", took, claimSettleDelay)
	}
}

func TestPausedNodeDropsCachedHoldTakenOver(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)
	defer func(d time.Duration) { renewAfter = d }(renewAfter)

	requireAcquired(t, a, LockTypeInode, 8, LockModeShared)
	if err := a.Release(LockTypeInode, 8); err != nil {
		t.Fatalf("Release: %v", err)
	}
	// A pauses past StaleThreshold with the read hold cached, and B takes the record over.
	ageRecord(t, a, LockTypeInode, 8, pastStale)
	requireAcquired(t, b, LockTypeInode, 8, LockModeExclusive)
	renewAfter = 0

	requireBlocked(t, a, LockTypeInode, 8, LockModeShared)
}

func TestReleaseCachedFreesHoldsAtOnce(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)

	requireAcquired(t, a, LockTypeInode, 9, LockModeExclusive)
	if err := a.Release(LockTypeInode, 9); err != nil {
		t.Fatalf("Release: %v", err)
	}
	a.ReleaseCached()
	if err := acquireWithin(b, cacheGrace/4, LockTypeInode, 9, LockModeExclusive); err != nil {
		t.Fatalf("Acquire right after ReleaseCached: %v", err)
	}
}

// FUSE cancels a request's context whenever the calling process gets a signal,
// which happens all the time, so cancellation alone must not end the wait.
func TestCancelledContextStillWaitsForLock(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)

	requireAcquired(t, a, LockTypeInode, 10, LockModeExclusive)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	errc := make(chan error, 1)
	go func() { errc <- b.Acquire(ctx, LockTypeInode, 10, LockModeExclusive) }()

	time.Sleep(300 * time.Millisecond)
	if err := a.Release(LockTypeInode, 10); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("Acquire with a cancelled context: got %v, want the lock once node A let it go", err)
	}
}

// BenchmarkFreshAcquireRelease measures an acquire that cannot reuse a cached
// hold, so it pays the claim settle wait.
func BenchmarkFreshAcquireRelease(b *testing.B) {
	dev, cleanup := createTestDevice(b, 1<<20)
	defer cleanup()
	m := newTestNode(dev, 0, 1<<20, 1)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := m.Acquire(ctx, LockTypeInode, 100, LockModeExclusive); err != nil {
			b.Fatal(err)
		}
		m.Release(LockTypeInode, 100)
		m.ReleaseCached()
	}
}
