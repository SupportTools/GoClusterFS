package lock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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

const pastStale = StaleThreshold + 2*time.Second

// ageSlot rewrites m's slot for the lock as if m wrote it age ago, as a paused
// node's slot looks to other nodes.
func ageSlot(t *testing.T, m *Manager, lt LockType, id uint64, age time.Duration) {
	t.Helper()
	recordID := m.hashToRecord(lt, id)
	row, err := m.readRow(recordID)
	if err != nil {
		t.Fatalf("readRow: %v", err)
	}
	s, err := decodeSlot(rowSlot(row, m.nodeSlot))
	if err != nil || s == nil {
		t.Fatalf("node %d has no slot for type=%d id=%d: %v", m.nodeSlot, lt, id, err)
	}
	s.acquireTime = time.Now().Add(-age).UnixNano()
	if err := m.writeSlot(recordID, m.nodeSlot, s); err != nil {
		t.Fatalf("writeSlot: %v", err)
	}
}

// tearSlot writes slot of the lock's record with a failing checksum, as a crash mid-write leaves it.
func tearSlot(t *testing.T, m *Manager, lt LockType, id uint64, slot int) {
	t.Helper()
	buf := storage.NewAlignedBuffer(SlotSize)
	copy(buf.Bytes(), encodeSlot(&claimSlot{mode: LockModeShared, acquireTime: time.Now().UnixNano()}))
	buf.Bytes()[100] ^= 0xFF
	if _, err := m.dev.WriteAt(buf.Bytes(), m.slotOffset(m.hashToRecord(lt, id), slot)); err != nil {
		t.Fatalf("write torn slot: %v", err)
	}
}

// writeClaim writes slot of the lock's record as a claim in mode, as a node that
// stopped mid-claim leaves it.
func writeClaim(t *testing.T, m *Manager, lt LockType, id uint64, slot int, mode LockMode) {
	t.Helper()
	claim := &claimSlot{mode: mode, lockType: lt, resourceID: id, owner: [16]byte{byte(slot)}, acquireTime: time.Now().UnixNano()}
	if err := m.writeSlot(m.hashToRecord(lt, id), slot, claim); err != nil {
		t.Fatalf("writeSlot: %v", err)
	}
}

// delaySlotWrites makes each of m's slot writes take delay(), as a slow device would.
func delaySlotWrites(m *Manager, delay func() time.Duration) {
	m.slotWrite = func(buf []byte, offset int64) (int, error) {
		time.Sleep(delay())
		return m.dev.WriteAt(buf, offset)
	}
}

func TestCollidingResourceIsBlockedWhileOtherNodeHoldsRecord(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, RowSize, 1)
	b := newTestNode(dev, 0, RowSize, 2)

	requireAcquired(t, a, LockTypeInode, 1, LockModeExclusive)
	requireBlocked(t, b, LockTypeInode, 2, LockModeExclusive)
}

func TestNodeHoldsCollidingResourcesUntilLastRelease(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, RowSize, 1)
	b := newTestNode(dev, 0, RowSize, 2)

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
	a := newTestNode(dev, 0, RowSize, 1)
	b := newTestNode(dev, 0, RowSize, 2)
	defer func(d time.Duration) { renewAfter = d }(renewAfter)
	renewAfter = 0

	requireAcquired(t, a, LockTypeInode, 1, LockModeExclusive)
	ageSlot(t, a, LockTypeInode, 1, pastStale)
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
	ageSlot(t, a, LockTypeBlockBitmap, 0, pastStale)

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
	ageSlot(t, a, LockTypeInode, 3, pastStale)
	requireAcquired(t, b, LockTypeInode, 3, LockModeExclusive)

	a.Release(LockTypeInode, 3)
	time.Sleep(cacheGrace + 50*time.Millisecond)
	requireBlocked(t, c, LockTypeInode, 3, LockModeExclusive)
}

func TestLockRegionStaysBoundedAcrossCycles(t *testing.T) {
	const (
		guard      = 4 * SlotSize
		regionSize = 16 * RowSize
		resources  = 1000
		cycles     = 1500
	)
	dev, cleanup := createTestDevice(t, guard+regionSize+guard)
	defer cleanup()
	defer func(g time.Duration) { cacheGrace = g }(cacheGrace)
	cacheGrace = time.Millisecond

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

func TestReleasedHoldIsCachedForGrace(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)
	defer func(g time.Duration) { cacheGrace = g }(cacheGrace)
	cacheGrace = 200 * time.Millisecond

	requireAcquired(t, a, LockTypeInode, 6, LockModeExclusive)
	if err := a.Release(LockTypeInode, 6); err != nil {
		t.Fatalf("Release: %v", err)
	}
	requireBlocked(t, b, LockTypeInode, 6, LockModeExclusive)

	start := time.Now()
	requireAcquired(t, b, LockTypeInode, 6, LockModeExclusive)
	if waited := time.Since(start); waited > cacheGrace+4*RetryMaxDelay {
		t.Fatalf("other node waited %v for a cached hold, want at most about the %v grace", waited, cacheGrace)
	}
}

func TestIdleCachedHoldFreesBeforeTurnEnds(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)

	requireAcquired(t, a, LockTypeInode, 7, LockModeExclusive)
	if err := a.Release(LockTypeInode, 7); err != nil {
		t.Fatalf("Release: %v", err)
	}
	start := time.Now()
	requireAcquired(t, b, LockTypeInode, 7, LockModeExclusive)
	if waited := time.Since(start); waited >= holdTurn {
		t.Fatalf("other node waited %v for an idle hold, want less than the %v turn a busy holder gets", waited, holdTurn)
	}
}

func TestPausedNodeDropsCachedHoldTakenOver(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)
	defer func(d time.Duration) { renewAfter = d }(renewAfter)
	defer func(g time.Duration) { cacheGrace = g }(cacheGrace)
	cacheGrace = time.Minute

	requireAcquired(t, a, LockTypeInode, 8, LockModeExclusive)
	if err := a.Release(LockTypeInode, 8); err != nil {
		t.Fatalf("Release: %v", err)
	}
	// A pauses past StaleThreshold with the write hold cached, and B takes the record over.
	ageSlot(t, a, LockTypeInode, 8, pastStale)
	requireAcquired(t, b, LockTypeInode, 8, LockModeExclusive)
	renewAfter = 0

	requireBlocked(t, a, LockTypeInode, 8, LockModeExclusive)
}

func TestReleaseCachedFreesHoldsAtOnce(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)
	defer func(g time.Duration) { cacheGrace = g }(cacheGrace)
	cacheGrace = time.Minute

	requireAcquired(t, a, LockTypeInode, 9, LockModeExclusive)
	if err := a.Release(LockTypeInode, 9); err != nil {
		t.Fatalf("Release: %v", err)
	}
	a.ReleaseCached()
	if err := acquireWithin(b, time.Second, LockTypeInode, 9, LockModeExclusive); err != nil {
		t.Fatalf("Acquire right after ReleaseCached: %v", err)
	}
}

// BenchmarkFreshAcquireRelease measures an acquire that cannot reuse a cached
// hold, so it writes its slot, reads the row, and clears the slot on release.
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

// Local holds that keep overlapping never let the key count reach zero, so the
// node must stop taking new ones once its turn is over, however long they last.
func TestOverlappingLocalHoldsStillYield(t *testing.T) {
	cases := []struct {
		busySlot, otherSlot int
		every, hold         time.Duration
	}{
		{1, 2, 3 * time.Millisecond, 6 * time.Millisecond},
		{2, 1, 3 * time.Millisecond, 6 * time.Millisecond},
		{1, 2, 100 * time.Millisecond, 300 * time.Millisecond},
	}
	for _, tc := range cases {
		slots := [2]int{tc.busySlot, tc.otherSlot}
		t.Run(fmt.Sprintf("busy slot %d holds %v every %v", slots[0], tc.hold, tc.every), func(t *testing.T) {
			dev, cleanup := createTestDevice(t, 1<<20)
			defer cleanup()
			busy := newTestNode(dev, 0, 1<<20, slots[0])
			other := newTestNode(dev, 0, 1<<20, slots[1])

			stop := make(chan struct{})
			var holders sync.WaitGroup
			defer func() { close(stop); holders.Wait() }()
			holders.Go(func() {
				tick := time.NewTicker(tc.every)
				defer tick.Stop()
				for {
					select {
					case <-stop:
						return
					case <-tick.C:
					}
					holders.Go(func() {
						if acquireWithin(busy, 5*time.Second, LockTypeInode, 12, LockModeExclusive) == nil {
							time.Sleep(tc.hold)
							busy.Release(LockTypeInode, 12)
						}
					})
				}
			})

			time.Sleep(50 * time.Millisecond)
			if err := acquireWithin(other, 3*time.Second, LockTypeInode, 12, LockModeExclusive); err != nil {
				t.Fatalf("other node never got a record whose local holds keep overlapping: %v", err)
			}
			other.Release(LockTypeInode, 12)
		})
	}
}

// A node that keeps using a record must still let it go soon enough for another
// node that wants it.
func TestBusyNodeYieldsCachedRecord(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	busy := newTestNode(dev, 0, 1<<20, 1)
	other := newTestNode(dev, 0, 1<<20, 2)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := acquireWithin(busy, time.Second, LockTypeInode, 12, LockModeExclusive); err == nil {
				time.Sleep(time.Millisecond)
				busy.Release(LockTypeInode, 12)
			}
		}
	}()
	defer func() { close(stop); <-done }()

	time.Sleep(50 * time.Millisecond)
	if err := acquireWithin(other, 2*time.Second, LockTypeInode, 12, LockModeExclusive); err != nil {
		t.Fatalf("other node never got a record a busy node keeps reusing: %v", err)
	}
	other.Release(LockTypeInode, 12)
}

// On slow storage an unmount frees its kept records in parallel, so it takes
// about as long as a few slot writes, not the sum of them.
func TestReleaseCachedWithSlowWritesRunsInParallel(t *testing.T) {
	const region = 32 * RowSize
	dev, cleanup := createTestDevice(t, region)
	defer cleanup()
	a := newTestNode(dev, 0, region, 1)
	defer func(g time.Duration) { cacheGrace = g }(cacheGrace)
	cacheGrace = time.Minute

	records := map[uint64]bool{}
	for id := uint64(100); len(records) < 16; id++ {
		if records[a.hashToRecord(LockTypeInode, id)] {
			continue
		}
		records[a.hashToRecord(LockTypeInode, id)] = true
		requireAcquired(t, a, LockTypeInode, id, LockModeExclusive)
		if err := a.Release(LockTypeInode, id); err != nil {
			t.Fatalf("Release: %v", err)
		}
	}
	delaySlotWrites(a, func() time.Duration { return 200 * time.Millisecond })

	start := time.Now()
	a.ReleaseCached()
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("ReleaseCached of %d kept records took %v", len(records), took.Round(time.Millisecond))
	}
}

// An acquire that gives up logs which node holds the record, so operators can find it.
func TestTimedOutAcquireLogsHolder(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)
	requireAcquired(t, a, LockTypeInode, 28, LockModeExclusive)

	var logs bytes.Buffer
	log.SetOutput(&logs)
	requireBlocked(t, b, LockTypeInode, 28, LockModeExclusive)
	log.SetOutput(os.Stderr)
	if got := logs.String(); !strings.Contains(got, "gave up waiting") || !strings.Contains(got, "slot 1 node") {
		t.Fatalf("timeout log does not name the holder:\n%s", got)
	}
}

// A caller whose context never ends, as FUSE handlers that detach theirs, still
// gives up once acquireTimeout passes.
func TestAcquireWithoutDeadlineGivesUp(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)
	defer func(d time.Duration) { acquireTimeout = d }(acquireTimeout)
	acquireTimeout = 200 * time.Millisecond
	requireAcquired(t, a, LockTypeInode, 40, LockModeExclusive)

	errc := make(chan error, 1)
	go func() {
		errc <- b.Acquire(context.WithoutCancel(context.Background()), LockTypeInode, 40, LockModeExclusive)
	}()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrLockTimeout) {
			t.Fatalf("Acquire: got %v, want ErrLockTimeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Acquire with a context that never ends was still waiting after 5s")
	}
}

func TestNewReadersDoNotKeepLeakedSharedLockFresh(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	leaked := newTestNode(dev, 0, 1<<20, 1)
	reader := newTestNode(dev, 0, 1<<20, 2)
	writer := newTestNode(dev, 0, 1<<20, 3)

	requireAcquired(t, leaked, LockTypeInode, 2, LockModeShared)
	ageSlot(t, leaked, LockTypeInode, 2, StaleThreshold-2*time.Second)

	requireAcquired(t, reader, LockTypeInode, 2, LockModeShared)
	if err := reader.Release(LockTypeInode, 2); err != nil {
		t.Fatalf("reader Release: %v", err)
	}
	reader.ReleaseCached()
	time.Sleep(3 * time.Second)

	requireAcquired(t, writer, LockTypeInode, 2, LockModeExclusive)
}

func TestStatsCountsHeldAndWaitingLocks(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, RowSize, 1)
	b := newTestNode(dev, 0, RowSize, 2)

	waitingDuringUncontended := -1
	a.SetOnAcquire(func(LockType, uint64, LockMode) { waitingDuringUncontended = a.Stats().Waiting })
	requireAcquired(t, a, LockTypeInode, 1, LockModeExclusive)
	if waitingDuringUncontended != 0 {
		t.Errorf("an uncontended Acquire counted as waiting: Waiting = %d inside it", waitingDuringUncontended)
	}
	if got := a.Stats(); got.Held != 1 || got.Waiting != 0 {
		t.Fatalf("holder Stats = %+v, want Held 1, Waiting 0", got)
	}

	granted := make(chan error, 1)
	go func() { granted <- acquireWithin(b, 10*time.Second, LockTypeInode, 1, LockModeExclusive) }()
	deadline := time.Now().Add(5 * time.Second)
	for b.Stats().Waiting != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("waiter Stats = %+v, want Waiting 1", b.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := a.Release(LockTypeInode, 1); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := <-granted; err != nil {
		t.Fatalf("waiter Acquire: %v", err)
	}
	if got := b.Stats(); got.Held != 1 || got.Waiting != 0 {
		t.Errorf("waiter Stats after grant = %+v, want Held 1, Waiting 0", got)
	}
	if got := a.Stats(); got.Held != 0 {
		t.Errorf("holder Stats after release = %+v, want Held 0", got)
	}
	b.Release(LockTypeInode, 1)
}

// Six nodes take one lock over and over, mostly for writing, while every slot
// write takes up to 30ms. A writer next to any other holder shows up in the counts.
func TestSlotClaimsExcludeUnderSlowWrites(t *testing.T) {
	const region = 8 * RowSize
	dev, cleanup := createTestDevice(t, region)
	defer cleanup()
	var writers, readers, worst, acquired atomic.Int64
	stop := time.Now().Add(3 * time.Second)
	var wg sync.WaitGroup
	for n := 1; n <= 6; n++ {
		m := newTestNode(dev, 0, region, n)
		delaySlotWrites(m, func() time.Duration { return time.Duration(rand.Int63n(int64(30 * time.Millisecond))) })
		wg.Go(func() {
			for i := 0; time.Now().Before(stop); i++ {
				mode := LockModeExclusive
				if i%3 == 2 {
					mode = LockModeShared
				}
				if err := acquireWithin(m, 20*time.Second, LockTypeInode, 50, mode); err != nil {
					t.Errorf("node %d Acquire: %v", n, err)
					return
				}
				mine, others := &writers, &readers
				if mode == LockModeShared {
					mine, others = &readers, &writers
				}
				now := mine.Add(1)
				if mode == LockModeExclusive && (now > 1 || others.Load() > 0) || mode == LockModeShared && others.Load() > 0 {
					worst.Add(1)
				}
				acquired.Add(1)
				time.Sleep(time.Millisecond)
				mine.Add(-1)
				if err := m.Release(LockTypeInode, 50); err != nil {
					t.Errorf("node %d Release: %v", n, err)
				}
				m.ReleaseCached()
			}
		})
	}
	wg.Wait()
	if worst.Load() > 0 {
		t.Fatalf("a writer shared the lock with another holder %d times in %d acquires", worst.Load(), acquired.Load())
	}
	t.Logf("%d acquires by 6 nodes", acquired.Load())
}

// A slot that fails its checksum, such as one torn by a crash mid-write,
// counts as a claim, so no node takes the lock past it.
func TestTornSlotCountsAsClaim(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	tearSlot(t, a, LockTypeInode, 51, 2)
	requireBlocked(t, a, LockTypeInode, 51, LockModeShared)

	if err := a.writeSlot(a.hashToRecord(LockTypeInode, 51), 2, nil); err != nil {
		t.Fatal(err)
	}
	requireAcquired(t, a, LockTypeInode, 51, LockModeShared)
}

// A slot left torn by a node that never writes it again stops blocking others
// once it has read torn for tornExpiry.
func TestTornSlotExpires(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	defer func(d time.Duration) { tornExpiry = d }(tornExpiry)
	tornExpiry = 200 * time.Millisecond
	a := newTestNode(dev, 0, 1<<20, 1)
	tearSlot(t, a, LockTypeInode, 51, 2)
	requireBlocked(t, a, LockTypeInode, 51, LockModeExclusive)

	time.Sleep(tornExpiry)
	requireAcquired(t, a, LockTypeInode, 51, LockModeExclusive)
}

// Readers on two nodes hold one record together; a writer waits for both.
func TestSlotReadersShareWritersWait(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)
	w := newTestNode(dev, 0, 1<<20, 3)
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

// Four nodes contend for one write lock with no delays. The lower slot wins
// each collision, so every node finishes its acquires.
func TestContendedSlotClaimsAllFinish(t *testing.T) {
	const region = 8 * RowSize
	dev, cleanup := createTestDevice(t, region)
	defer cleanup()
	start := time.Now()
	var conflicts, waits, deferred atomic.Uint64
	var wg sync.WaitGroup
	for n := 1; n <= 4; n++ {
		m := newTestNode(dev, 0, region, n)
		wg.Go(func() {
			defer func() {
				conflicts.Add(m.conflicts.Load())
				waits.Add(m.waits.Load())
				deferred.Add(m.deferred.Load())
			}()
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
	t.Logf("100 contended acquires by 4 nodes took %v: %d claims cleared, %d kept to wait, %d tries waited for a lower slot",
		time.Since(start).Round(time.Millisecond), conflicts.Load(), waits.Load(), deferred.Load())
}

// Two claims race: the first node's slot write completes, then it stalls before
// reading the row while the second node claims. Whichever slot is lower wins,
// and the other never holds beside it.
func TestRacingClaimsLowerSlotWins(t *testing.T) {
	for _, tc := range []struct {
		name               string
		firstSlot, another int
	}{
		{"first claimant in the lower slot", 1, 2},
		{"first claimant in the higher slot", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dev, cleanup := createTestDevice(t, 1<<20)
			defer cleanup()
			first := newTestNode(dev, 0, 1<<20, tc.firstSlot)
			second := newTestNode(dev, 0, 1<<20, tc.another)
			written, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			first.slotWrite = func(buf []byte, offset int64) (int, error) {
				n, err := first.dev.WriteAt(buf, offset)
				once.Do(func() { close(written); <-proceed })
				return n, err
			}

			firstErr := make(chan error, 1)
			go func() { firstErr <- acquireWithin(first, 2*time.Second, LockTypeInode, 54, LockModeExclusive) }()
			<-written
			secondErr := make(chan error, 1)
			go func() { secondErr <- acquireWithin(second, 2*time.Second, LockTypeInode, 54, LockModeExclusive) }()
			time.Sleep(100 * time.Millisecond)
			close(proceed)

			winner, loser, winErr, loseErr := first, second, firstErr, secondErr
			if second.nodeSlot < first.nodeSlot {
				winner, loser, winErr, loseErr = second, first, secondErr, firstErr
			}
			if err := <-winErr; err != nil {
				t.Fatalf("node in slot %d: %v", winner.nodeSlot, err)
			}
			select {
			case err := <-loseErr:
				t.Fatalf("node in slot %d returned %v while slot %d held the lock", loser.nodeSlot, err, winner.nodeSlot)
			case <-time.After(300 * time.Millisecond):
			}
			if err := winner.Release(LockTypeInode, 54); err != nil {
				t.Fatal(err)
			}
			winner.ReleaseCached()
			if err := <-loseErr; err != nil {
				t.Fatalf("node in slot %d after the winner released: %v", loser.nodeSlot, err)
			}
		})
	}
}

// A node that stops mid-claim leaves its slot. Whether that slot is below or
// above a live claimant's, the live one gets the lock once the slot is stale.
func TestDeadClaimantBlocksOnlyUntilStale(t *testing.T) {
	for _, deadSlot := range []int{1, 5} {
		t.Run(fmt.Sprintf("dead claimant in slot %d", deadSlot), func(t *testing.T) {
			dev, cleanup := createTestDevice(t, 1<<20)
			defer cleanup()
			live := newTestNode(dev, 0, 1<<20, 3)
			writeClaim(t, live, LockTypeInode, 55, deadSlot, LockModeExclusive)
			requireBlocked(t, live, LockTypeInode, 55, LockModeExclusive)

			dead := newTestNode(dev, 0, 1<<20, deadSlot)
			ageSlot(t, dead, LockTypeInode, 55, pastStale)
			requireAcquired(t, live, LockTypeInode, 55, LockModeExclusive)
		})
	}
}

// A lower-slot node gives up waiting for a holder. It clears the claim it kept,
// so a higher-slot node gets the lock as soon as the holder lets go.
func TestAbandonedClaimIsCleared(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	low := newTestNode(dev, 0, 1<<20, 1)
	holder := newTestNode(dev, 0, 1<<20, 2)
	high := newTestNode(dev, 0, 1<<20, 3)
	requireAcquired(t, holder, LockTypeInode, 56, LockModeExclusive)
	requireBlocked(t, low, LockTypeInode, 56, LockModeExclusive)
	if err := holder.Release(LockTypeInode, 56); err != nil {
		t.Fatal(err)
	}
	holder.ReleaseCached()
	requireAcquired(t, high, LockTypeInode, 56, LockModeExclusive)
}

// Slot writes as slow as the slowest seen on CI, 333ms, make claims slow but
// never keep a record from other nodes once its holder lets go.
func TestSlowSlotWritesCauseNoFreeze(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	nodes := []*Manager{newTestNode(dev, 0, 1<<20, 1), newTestNode(dev, 0, 1<<20, 2)}
	for _, m := range nodes {
		delaySlotWrites(m, func() time.Duration { return 333 * time.Millisecond })
	}
	start := time.Now()
	for round := range 4 {
		m := nodes[round%2]
		if err := acquireWithin(m, 5*time.Second, LockTypeInode, 57, LockModeExclusive); err != nil {
			t.Fatalf("round %d, node %d: %v", round, m.nodeSlot, err)
		}
		if err := m.Release(LockTypeInode, 57); err != nil {
			t.Fatal(err)
		}
		m.ReleaseCached()
	}
	if took := time.Since(start); took > 8*time.Second {
		t.Fatalf("4 hand-offs with 333ms slot writes took %v", took.Round(time.Millisecond))
	}
}

// A cached hold is taken again from memory, with no slot write or row read.
func TestCachedHoldRejoinNeedsNoDiskIO(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	defer func(g time.Duration) { cacheGrace = g }(cacheGrace)
	cacheGrace = time.Minute
	requireAcquired(t, a, LockTypeInode, 7, LockModeExclusive)
	if err := a.Release(LockTypeInode, 7); err != nil {
		t.Fatalf("Release: %v", err)
	}
	writes, reads := a.slotWrites.Load(), a.rowReads.Load()
	requireAcquired(t, a, LockTypeInode, 7, LockModeExclusive)
	if a.slotWrites.Load() != writes || a.rowReads.Load() != reads {
		t.Fatalf("re-acquiring a cached hold did %d slot writes and %d row reads, want none",
			a.slotWrites.Load()-writes, a.rowReads.Load()-reads)
	}
}
