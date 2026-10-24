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

// ageRecord rewrites the lock's record as if it was taken age ago and never renewed.
func ageRecord(t *testing.T, m *Manager, lt LockType, id uint64, age time.Duration) {
	t.Helper()
	recordID := m.hashToRecord(lt, id)
	rec, err := m.readRecord(recordID)
	if err != nil {
		t.Fatalf("readRecord: %v", err)
	}
	rec.AcquireTime = time.Now().Add(-age).Unix()
	buf := storage.GetBuffer(RecordSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), serializeRecord(rec))
	if _, err := m.dev.WriteAt(buf.Bytes(), int64(m.offset)+int64(recordID)*RecordSize); err != nil {
		t.Fatalf("write record: %v", err)
	}
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

	if err := a.Release(LockTypeInode, 3); !errors.Is(err, ErrNotLockOwner) {
		t.Fatalf("paused holder Release: got %v, want ErrNotLockOwner", err)
	}
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
