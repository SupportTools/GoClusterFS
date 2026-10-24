package bitmap

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/trim"
)

type discardRecorder struct {
	mu        sync.Mutex
	discarded map[uint64]bool
}

func (d *discardRecorder) discard(offset, length uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for b := offset / trim.BlockSize; b < (offset+length)/trim.BlockSize; b++ {
		d.discarded[b] = true
	}
	return nil
}

func (d *discardRecorder) has(block uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.discarded[block]
}

func newTrimmedRegion(t *testing.T, batch int, maxAge time.Duration) (*Region, *trim.Manager, *discardRecorder, func()) {
	t.Helper()
	dev, cleanup := createTestDevice(t, 10<<20)
	rec := &discardRecorder{discarded: make(map[uint64]bool)}
	tm := trim.NewManager(dev, trim.Config{BatchSize: batch, MaxAge: maxAge, Enabled: true, Discard: rec.discard})
	region := NewRegion(dev, 0, 1000, nil, [16]byte{1})
	region.SetTrimManager(tm)
	tm.Start()
	return region, tm, rec, func() {
		_ = tm.Stop()
		cleanup()
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestTrimSkipsBlockReallocatedBeforeFlush(t *testing.T) {
	region, _, rec, cleanup := newTrimmedRegion(t, 1000, 50*time.Millisecond)
	defer cleanup()
	ctx := context.Background()

	blocks, err := region.Alloc(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	reused, stillFree := blocks[0], blocks[2]
	for _, b := range []uint64{reused, stillFree} {
		if err := region.Free(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	again, err := region.Alloc(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if again[0] != reused {
		t.Fatalf("Alloc after Free got block %d, want the freed block %d", again[0], reused)
	}

	// Ranges are discarded in block order, so once stillFree is discarded the
	// flush is past reused.
	waitFor(t, func() bool { return rec.has(stillFree) })
	if rec.has(reused) {
		t.Errorf("block %d was discarded after it was allocated again", reused)
	}
}

func TestTrimSkipsBlockAnotherNodeAllocated(t *testing.T) {
	region, tm, rec, cleanup := newTrimmedRegion(t, 1000, time.Hour)
	defer cleanup()
	other := NewRegion(region.dev, 0, 1000, nil, [16]byte{2})
	ctx := context.Background()

	blocks, err := region.Alloc(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := region.Free(ctx, blocks[0]); err != nil {
		t.Fatal(err)
	}
	theirs, err := other.Alloc(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if theirs[0] != blocks[0] {
		t.Fatalf("other node got block %d, want the freed block %d", theirs[0], blocks[0])
	}

	if err := tm.Stop(); err != nil {
		t.Fatal(err)
	}
	if rec.has(blocks[0]) {
		t.Errorf("block %d was discarded after another node allocated it", blocks[0])
	}
}

func TestTrimDiscardsFreedBlocks(t *testing.T) {
	region, tm, rec, cleanup := newTrimmedRegion(t, 1000, time.Hour)
	defer cleanup()
	ctx := context.Background()

	blocks, err := region.Alloc(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range blocks {
		if err := region.Free(ctx, b); err != nil {
			t.Fatal(err)
		}
	}

	if err := tm.Stop(); err != nil {
		t.Fatal(err)
	}
	for _, b := range blocks {
		if !rec.has(b) {
			t.Errorf("free block %d was not discarded", b)
		}
	}
}

// TestTrimBatchFlushFromFree checks that a full batch does not flush inside
// Free, which holds the bitmap lock the flush needs.
func TestTrimBatchFlushFromFree(t *testing.T) {
	region, _, rec, cleanup := newTrimmedRegion(t, 1, time.Hour)
	defer cleanup()
	ctx := context.Background()

	blocks, err := region.Alloc(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- region.Free(ctx, blocks[0]) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Free did not return: the batch flush deadlocked on the bitmap lock")
	}

	waitFor(t, func() bool { return rec.has(blocks[0]) })
}

// TestTrimDiscardHoldsNoBitmapLock runs the guard with real lock managers on two
// nodes. A discard can outlast the lock's 15s stale threshold, so the block must
// stay reserved without the lock held, and other nodes must keep allocating.
func TestTrimDiscardHoldsNoBitmapLock(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	lockA := lock.NewManager(dev, 1<<20, 1<<20, [16]byte{1}, 0)
	lockB := lock.NewManager(dev, 1<<20, 1<<20, [16]byte{2}, 1)

	started := make(chan struct{})
	finish := make(chan struct{})
	var once sync.Once
	tm := trim.NewManager(dev, trim.Config{BatchSize: 1000, MaxAge: time.Hour, Enabled: true, Discard: func(offset, length uint64) error {
		once.Do(func() { close(started) })
		<-finish
		return nil
	}})
	a := NewRegion(dev, 0, 1000, lockA, [16]byte{1})
	a.SetTrimManager(tm)
	tm.Start()
	b := NewRegion(dev, 0, 1000, lockB, [16]byte{2})
	ctx := context.Background()

	blocks, err := a.Alloc(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Free(ctx, blocks[0]); err != nil {
		t.Fatal(err)
	}

	stopped := make(chan error, 1)
	go func() { stopped <- tm.Stop() }()
	<-started

	if free, err := b.IsFree(blocks[0]); err != nil || free {
		t.Errorf("block %d being discarded: IsFree = %v, %v; want reserved", blocks[0], free, err)
	}
	allocCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	theirs, err := b.Alloc(allocCtx, 1)
	cancel()
	if err != nil {
		t.Fatalf("node B could not allocate during node A's discard: %v", err)
	}
	if theirs[0] == blocks[0] {
		t.Errorf("node B allocated block %d while node A was discarding it", blocks[0])
	}

	close(finish)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if free, err := a.IsFree(blocks[0]); err != nil || !free {
		t.Errorf("block %d after discard: IsFree = %v, %v; want free again", blocks[0], free, err)
	}
}
