package reserve

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/journal"
)

type fakeBitmap struct {
	mu    sync.Mutex
	total uint64
	free  []uint64
	gate  chan struct{} // when set, Reserve waits on it, as for a busy bitmap lock
}

func newFakeBitmap(total, used uint64) *fakeBitmap {
	b := &fakeBitmap{total: total}
	for i := used; i < total; i++ {
		b.free = append(b.free, i)
	}
	return b
}

func (b *fakeBitmap) Reserve(_ context.Context, n int, record func([]uint64) bool, _ func() error) ([]uint64, error) {
	if b.gate != nil {
		<-b.gate
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	n = min(n, len(b.free))
	picked := slices.Clone(b.free[:n])
	if !record(picked) {
		return nil, errors.New("not recorded")
	}
	b.free = b.free[n:]
	return picked, nil
}

func (b *fakeBitmap) FreeCount() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return uint64(len(b.free))
}

func (b *fakeBitmap) TotalBlocks() uint64 { return b.total }

func (b *fakeBitmap) give(blocks []uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.free = append(b.free, blocks...)
	slices.Sort(b.free)
}

// fakeJournal records the order of commits, completions and frees.
type fakeJournal struct {
	mu           sync.Mutex
	seq          uint64
	events       []string
	pending      map[uint64]bool
	lag          uint32
	commitErr    error
	completeErr  error
	aborted      int
	freeingBlock chan struct{} // when set, frees wait on it, as for a busy bitmap lock
}

func (j *fakeJournal) event(e string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.events = append(j.events, e)
}

func (j *fakeJournal) take() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	e := j.events
	j.events = nil
	return e
}

func (j *fakeJournal) Begin() *journal.Transaction { return journal.NewTransaction(0, [16]byte{1}, 1) }

func (j *fakeJournal) Commit(*journal.Transaction) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.commitErr != nil {
		return 0, j.commitErr
	}
	j.seq++
	if j.pending == nil {
		j.pending = map[uint64]bool{}
	}
	j.pending[j.seq] = true
	j.events = append(j.events, "commit")
	return j.seq, nil
}

func (j *fakeJournal) Abort(*journal.Transaction) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.aborted++
}

func (j *fakeJournal) CompleteIntentDurable(seq uint64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.pending[seq] {
		return nil
	}
	if j.completeErr != nil {
		return j.completeErr
	}
	delete(j.pending, seq)
	j.events = append(j.events, "complete")
	return nil
}

func (j *fakeJournal) IntentLag(seq uint64) (uint32, uint32, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.lag, 1000, j.pending[seq]
}

func (j *fakeJournal) set(f func(*fakeJournal)) {
	j.mu.Lock()
	defer j.mu.Unlock()
	f(j)
}

func newManager(size int, bm *fakeBitmap, j *fakeJournal) *Manager {
	return New(Config{Size: size, Idle: time.Hour}, bm, j, func(_ context.Context, blocks []uint64) {
		if j.freeingBlock != nil {
			<-j.freeingBlock
		}
		j.event("free")
		bm.give(blocks)
	}, func() int { return 1 })
}

func take(t *testing.T, m *Manager) uint64 {
	t.Helper()
	b, ok := m.Take(context.Background())
	if !ok {
		t.Fatal("Take found no reserved block")
	}
	return b
}

func expectEvents(t *testing.T, j *fakeJournal, what string, want ...string) {
	t.Helper()
	if got := j.take(); !slices.Equal(got, want) {
		t.Errorf("%s did %v, want %v", what, got, want)
	}
}

// A reserve holds no more than the configured size, nor a sixteenth of the free
// blocks per node, and none at all below two reserves per node or 1% of the blocks.
func TestReserveSize(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name        string
		total, used uint64
		size        int
		want        int
	}{
		{"configured size", 10000, 0, 64, 64},
		{"sixteenth of free", 10000, 9000, 128, 62},
		{"below two reserves", 10000, 9900, 64, 0},
		{"below 1%", 100000, 99050, 8, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bm := newFakeBitmap(tc.total, tc.used)
			m := newManager(tc.size, bm, &fakeJournal{})
			_, ok := m.Take(ctx)
			got := 0
			if ok {
				got = m.Stats().Blocks + 1
			}
			if got != tc.want {
				t.Errorf("reserve of %d blocks, want %d", got, tc.want)
			}
		})
	}
}

// Giving the reserve back ends its record before it frees anything, so a crash
// in between leaks blocks instead of letting replay free them twice.
func TestReturnEndsRecordBeforeFreeing(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(8, bm, j)
	b := take(t, m)
	if err := m.Logged([]uint64{b}, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	j.take()
	m.ReturnAll(context.Background())
	expectEvents(t, j, "return", "complete", "free")
	if got := bm.FreeCount(); got != 10000-1 {
		t.Errorf("%d free blocks after the return, want %d", got, 10000-1)
	}
}

// Returning the reserve while a write holds one of its blocks ends the record
// anyway. If the write then hands the block back, it is freed without a record.
func TestBlockHandedBackAfterReturnIsFreed(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(8, bm, j)
	b := take(t, m)
	j.take()
	m.ReturnAll(context.Background())
	expectEvents(t, j, "return", "complete", "free")
	m.Untake(b)
	m.ReturnAll(context.Background())
	expectEvents(t, j, "second return", "free")
	if bm.FreeCount() != 10000 {
		t.Errorf("%d free blocks, want all 10000", bm.FreeCount())
	}
}

// A block whose use commit failed may be mapped on disk, so it never goes back
// to the reserve or the bitmap.
func TestFailedLogLeavesBlockAllocated(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(8, bm, j)
	b := take(t, m)
	if err := m.Logged([]uint64{b}, func() error { return errors.New("commit failed") }); err == nil {
		t.Fatal("Logged hid the commit error")
	}
	m.Untake(b)
	if got := m.Stats().Blocks; got != 7 {
		t.Errorf("%d blocks in reserve, want 7", got)
	}
	m.ReturnAll(context.Background())
	if got := bm.FreeCount(); got != 10000-1 {
		t.Errorf("%d free blocks after the return, want %d", got, 10000-1)
	}
}

// A commit that hit a full journal wrote nothing, so its blocks go back to the reserve.
func TestJournalFullLogPutsBlockBack(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(8, bm, j)
	b := take(t, m)
	if err := m.Logged([]uint64{b}, func() error { return journal.ErrJournalFull }); !errors.Is(err, journal.ErrJournalFull) {
		t.Fatalf("Logged returned %v, want the journal-full error", err)
	}
	if got := m.Stats().Blocks; got != 8 {
		t.Errorf("%d blocks in reserve, want 8", got)
	}
}

// The cached free count stops at zero instead of wrapping when a refill
// subtracts blocks a fresh count already left out.
func TestFreeCountNeverWraps(t *testing.T) {
	m := newManager(8, newFakeBitmap(10000, 0), &fakeJournal{})
	m.freeCount.Store(3)
	m.adjustFree(-8)
	if got := m.freeCount.Load(); got != 0 {
		t.Errorf("free count %d after subtracting more than it held, want 0", got)
	}
}

// hookJournal runs onCommit before each commit.
type hookJournal struct {
	*fakeJournal
	onCommit func() error
}

func (h *hookJournal) Commit(txn *journal.Transaction) (uint64, error) {
	if h.onCommit != nil {
		if err := h.onCommit(); err != nil {
			return 0, err
		}
	}
	return h.fakeJournal.Commit(txn)
}

// A refresh that fails after a write used the last blocks of a record it was
// ending reopens the record, which then ends as fully used.
func TestRecordReopenedAfterFailedRefreshEnds(t *testing.T) {
	ctx := context.Background()
	bm, fj := newFakeBitmap(1000, 0), &fakeJournal{}
	j := &hookJournal{fakeJournal: fj}
	m := New(Config{Size: 2}, bm, j, func(_ context.Context, blocks []uint64) { bm.give(blocks) }, func() int { return 1 })
	if !m.refill(ctx) {
		t.Fatal("first refill failed")
	}
	a1, _ := m.TakeReady()
	a2, _ := m.TakeReady()
	seqA := fj.seq
	if !m.refill(ctx) {
		t.Fatal("second refill failed")
	}
	fj.set(func(j *fakeJournal) { j.lag = 500 })
	j.onCommit = func() error {
		j.onCommit = nil
		if err := m.Logged([]uint64{a1, a2}, func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		return journal.ErrJournalFull
	}
	m.Refresh(ctx)
	fj.set(func(j *fakeJournal) { j.lag = 0 })
	m.tidy(ctx)
	if _, _, pending := fj.IntentLag(seqA); pending {
		t.Fatalf("record %d, all of whose blocks were used, never ends", seqA)
	}
}

// An allocation waiting for peers' reserves never waits for endMu, and record
// ends never wait for frees: a holder of either may be stuck on the bitmap lock.
func TestWaitForPeersAndEndsDoNotWaitForFrees(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(8, bm, j)
	m.Untake(take(t, m))

	j.freeingBlock = make(chan struct{})
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		m.ReturnAll(context.Background())
	}()
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.tidy(context.Background())
		m.Refresh(context.Background())
		m.endMu.Lock()
		m.WaitForPeers(context.Background(), 0)
		m.endMu.Unlock()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a record end or WaitForPeers waited for a free")
	}
	close(j.freeingBlock)
	<-returned
}

// When ending a record fails, its blocks stay reserved, and a later return ends it and frees them.
func TestFailedEndKeepsBlocks(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(8, bm, j)
	m.Untake(take(t, m))
	j.take()
	j.set(func(j *fakeJournal) { j.completeErr = journal.ErrJournalFull })
	m.ReturnAll(context.Background())
	expectEvents(t, j, "failed return")
	if got := m.Stats().Blocks; got != 8 {
		t.Errorf("%d blocks in reserve after a failed return, want 8", got)
	}
	j.set(func(j *fakeJournal) { j.completeErr = nil })
	m.ReturnAll(context.Background())
	expectEvents(t, j, "retried return", "complete", "free")
}

// A record is renewed once it is a quarter of the journal behind the tail. If the
// old record cannot be ended, nothing is freed until it is, since it still lists the moved blocks.
func TestRefreshRenewsAnOldRecord(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(8, bm, j)
	m.Untake(take(t, m))
	j.take()
	j.set(func(j *fakeJournal) { j.lag = 100 })
	m.Refresh(context.Background())
	expectEvents(t, j, "refresh of a recent record")

	j.set(func(j *fakeJournal) { j.lag, j.completeErr = 300, errors.New("sync failed") })
	m.Refresh(context.Background())
	expectEvents(t, j, "refresh with a failing end", "commit")
	j.set(func(j *fakeJournal) { j.lag = 0 })
	m.ReturnAll(context.Background())
	expectEvents(t, j, "return while the old record is unfinished")
	if got := m.Stats().Blocks; got != 8 {
		t.Errorf("%d blocks in reserve, want 8", got)
	}

	j.set(func(j *fakeJournal) { j.completeErr = nil })
	m.ReturnAll(context.Background())
	expectEvents(t, j, "return after the end works", "complete", "complete", "free")
}

// A record whose blocks are all handed out and far behind the tail ends, so a
// write that waits long on a lock cannot hold the journal head back.
func TestRefreshEndsRecordOfHandedOutBlocks(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(2, bm, j)
	take(t, m)
	take(t, m)
	j.take()
	j.set(func(j *fakeJournal) { j.lag = 300 })
	m.Refresh(context.Background())
	expectEvents(t, j, "refresh", "complete")
}

// A record whose blocks were all used ends at the next tidy.
func TestUsedRecordEnds(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(2, bm, j)
	blocks := []uint64{take(t, m), take(t, m)}
	if err := m.Logged(blocks, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	j.take()
	m.tidy(context.Background())
	expectEvents(t, j, "tidy", "complete")
}

// A refill whose record hit a full journal marks nothing, since nothing was
// written. Any other commit error leaves the blocks allocated, since the record may be on disk.
func TestRefillRecordFailure(t *testing.T) {
	for _, tc := range []struct {
		err  error
		free uint64
	}{
		{journal.ErrJournalFull, 10000},
		{errors.New("sync failed"), 10000 - 8},
	} {
		bm, j := newFakeBitmap(10000, 0), &fakeJournal{commitErr: tc.err}
		m := newManager(8, bm, j)
		if _, ok := m.Take(context.Background()); ok {
			t.Fatalf("%v: Take handed out a block without a record", tc.err)
		}
		if got := bm.FreeCount(); got != tc.free {
			t.Errorf("%v: %d free blocks, want %d", tc.err, got, tc.free)
		}
		if j.aborted != 1 {
			t.Errorf("%v: %d transactions aborted, want 1", tc.err, j.aborted)
		}
	}
}

// Writes call TakeReady, Untake and Logged with an inode lock held, and statfs
// calls Stats. None of them may wait while a refill or a return waits for the
// bitmap lock, or a node that dies holding that lock hangs the cluster.
func TestWritersDoNotWaitForBitmapLock(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(2, bm, j)
	first, second := take(t, m), take(t, m)

	writersFinish := func(what string) {
		t.Helper()
		done := make(chan struct{})
		go func() {
			defer close(done)
			m.Stats()
			m.TakeReady()
			m.Untake(first)
			if b, ok := m.TakeReady(); ok {
				_ = m.Logged([]uint64{b}, func() error { return nil })
			}
			_ = m.Logged([]uint64{second}, func() error { return nil })
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("writers waited while %s", what)
		}
	}

	bm.gate = make(chan struct{})
	refilled := make(chan struct{})
	go func() {
		defer close(refilled)
		m.Take(context.Background())
	}()
	time.Sleep(50 * time.Millisecond)
	writersFinish("a refill waited for the bitmap lock")
	close(bm.gate)
	<-refilled
	bm.gate = nil

	j.freeingBlock = make(chan struct{})
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		m.ReturnAll(context.Background())
	}()
	time.Sleep(50 * time.Millisecond)
	first, second = take(t, m), take(t, m)
	writersFinish("a return waited for the bitmap lock")
	close(j.freeingBlock)
	<-returned
}

// A flood of peer requests gives the reserve back at most once per tick, even
// while a writer keeps refilling it.
func TestPeerRequestsCoalesce(t *testing.T) {
	bm, j := newFakeBitmap(100000, 0), &fakeJournal{}
	m := newManager(8, bm, j)
	m.Start()
	defer m.Stop(context.Background())
	deadline := time.Now().Add(2*tick + tick/2)
	for time.Now().Before(deadline) {
		m.Untake(take(t, m))
		m.AskReturn()
		time.Sleep(time.Millisecond)
	}
	if got := m.Stats().Returns; got < 1 || got > 3 {
		t.Errorf("%d returns in 2.5 ticks of requests, want 1 to 3", got)
	}
}
