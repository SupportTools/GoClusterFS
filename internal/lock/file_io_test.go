package lock

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// keepAcquiring takes the lock in mode on m, holds it briefly and releases it,
// again and again until stop closes.
func keepAcquiring(t *testing.T, m *Manager, mode LockMode, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		if err := acquireWithin(m, 10*time.Second, LockTypeFileIO, 7, mode); err != nil {
			t.Errorf("node %d Acquire mode %d: %v", m.nodeSlot, mode, err)
			return
		}
		time.Sleep(5 * time.Millisecond)
		release(t, m, LockTypeFileIO, 7)
	}
}

func waitsFor(t *testing.T, m *Manager, mode LockMode, times int) []time.Duration {
	var waits []time.Duration
	for range times {
		start := time.Now()
		if err := acquireWithin(m, 10*time.Second, LockTypeFileIO, 7, mode); err != nil {
			t.Fatalf("node %d never got mode %d: %v", m.nodeSlot, mode, err)
		}
		waits = append(waits, time.Since(start))
		release(t, m, LockTypeFileIO, 7)
		time.Sleep(20 * time.Millisecond)
	}
	return waits
}

func TestWriterGetsRecordWhileReadersOnLowerSlotsKeepReading(t *testing.T) {
	const region = layoutRows * RowSize
	dev, cleanup := createTestDevice(t, region)
	defer cleanup()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	defer func() { close(stop); wg.Wait() }()
	for slot := range 4 {
		reader := newTestNode(dev, 0, region, slot)
		wg.Go(func() { keepAcquiring(t, reader, LockModeShared, stop) })
		time.Sleep(60 * time.Millisecond)
	}
	writer := newTestNode(dev, 0, region, 4)

	waits := waitsFor(t, writer, LockModeExclusive, 8)
	t.Logf("writer waits: %v", waits)
	if longest := slices.Max(waits); longest > 1500*time.Millisecond {
		t.Errorf("writer waited up to %v for 4 reading nodes, want under 1.5s: %v", longest, waits)
	}
}

func TestReaderGetsRecordWhileWriterKeepsWriting(t *testing.T) {
	for _, readerSlot := range []int{0, 2} {
		const region = layoutRows * RowSize
		dev, cleanup := createTestDevice(t, region)
		defer cleanup()
		writer := newTestNode(dev, 0, region, 1)
		reader := newTestNode(dev, 0, region, readerSlot)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() { keepAcquiring(t, writer, LockModeExclusive, stop) })
		time.Sleep(20 * time.Millisecond)

		waits := waitsFor(t, reader, LockModeShared, 8)
		t.Logf("reader in slot %d waits: %v", readerSlot, waits)
		close(stop)
		wg.Wait()
		if longest := slices.Max(waits); longest > 1500*time.Millisecond {
			t.Errorf("reader in slot %d waited up to %v for a writing node, want under 1.5s: %v", readerSlot, longest, waits)
		}
	}
}

func TestFileIOLockRanksBeforeEveryOtherType(t *testing.T) {
	const region = layoutRows * RowSize
	dev, cleanup := createTestDevice(t, region)
	defer cleanup()
	m := newTestNode(dev, 0, region, 1)

	requireAcquired(t, m, LockTypeFileIO, 9, LockModeShared)
	for lt := LockType(0); lt < LockTypeFileIO; lt++ {
		requireAcquired(t, m, lt, 9, LockModeExclusive)
		release(t, m, lt, 9)
	}
	release(t, m, LockTypeFileIO, 9)

	requireAcquired(t, m, LockTypeVolume, 9, LockModeExclusive)
	if err := acquireWithin(m, time.Second, LockTypeFileIO, 9, LockModeShared); !errors.Is(err, ErrDeadlockRisk) {
		t.Errorf("file I/O lock while holding the volume lock: err = %v, want ErrDeadlockRisk", err)
	}
	release(t, m, LockTypeVolume, 9)
}

func TestHeldSinceMovesOnlyWhenAnotherNodeCouldHaveWritten(t *testing.T) {
	const region = layoutRows * RowSize
	dev, cleanup := createTestDevice(t, region)
	defer cleanup()
	a := newTestNode(dev, 0, region, 1)
	b := newTestNode(dev, 0, region, 2)

	if !a.HeldSince(LockTypeFileIO, 7).IsZero() {
		t.Fatal("HeldSince of a lock never taken is set")
	}
	requireAcquired(t, a, LockTypeFileIO, 7, LockModeShared)
	first := a.HeldSince(LockTypeFileIO, 7)
	release(t, a, LockTypeFileIO, 7)
	requireAcquired(t, a, LockTypeFileIO, 7, LockModeShared)
	if again := a.HeldSince(LockTypeFileIO, 7); !again.Equal(first) {
		t.Errorf("HeldSince moved from %v to %v while node A kept its cached hold", first, again)
	}
	release(t, a, LockTypeFileIO, 7)

	requireAcquired(t, b, LockTypeFileIO, 7, LockModeExclusive)
	release(t, b, LockTypeFileIO, 7)
	b.ReleaseCached()
	requireAcquired(t, a, LockTypeFileIO, 7, LockModeShared)
	if after := a.HeldSince(LockTypeFileIO, 7); !after.After(first) {
		t.Errorf("HeldSince stayed at %v after node B held the record exclusive", after)
	}
	release(t, a, LockTypeFileIO, 7)
}
