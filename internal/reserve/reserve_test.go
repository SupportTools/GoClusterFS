package reserve

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/journal"
)

type fakeBitmap struct {
	total uint64
	free  []uint64
}

func newFakeBitmap(total, used uint64) *fakeBitmap {
	b := &fakeBitmap{total: total}
	for i := used; i < total; i++ {
		b.free = append(b.free, i)
	}
	return b
}

func (b *fakeBitmap) Reserve(_ context.Context, n int, record func([]uint64) error) ([]uint64, error) {
	n = min(n, len(b.free))
	picked := slices.Clone(b.free[:n])
	if err := record(picked); err != nil {
		return nil, err
	}
	b.free = b.free[n:]
	return picked, nil
}

func (b *fakeBitmap) FreeCount() uint64   { return uint64(len(b.free)) }
func (b *fakeBitmap) TotalBlocks() uint64 { return b.total }
func (b *fakeBitmap) give(_ context.Context, blocks []uint64) {
	b.free = append(b.free, blocks...)
	slices.Sort(b.free)
}

// fakeJournal records the order of commits and completions.
type fakeJournal struct {
	seq       uint64
	events    []string
	completed map[uint64]bool
	lag       uint32
	failNext  bool
}

func (j *fakeJournal) Begin() *journal.Transaction { return journal.NewTransaction(0, [16]byte{1}, 1) }
func (j *fakeJournal) Commit(*journal.Transaction) (uint64, error) {
	if j.failNext {
		j.failNext = false
		return 0, errors.New("commit failed")
	}
	j.seq++
	j.events = append(j.events, "commit")
	return j.seq, nil
}
func (j *fakeJournal) CompleteIntent(seq uint64) error {
	if j.completed == nil {
		j.completed = map[uint64]bool{}
	}
	j.completed[seq] = true
	j.events = append(j.events, "complete")
	return nil
}
func (j *fakeJournal) IntentLag(seq uint64) (uint32, uint32, bool) {
	return j.lag, 1000, !j.completed[seq] && seq != 0
}

func newManager(size int, bm *fakeBitmap, j *fakeJournal) *Manager {
	return New(Config{Size: size, Idle: time.Hour}, bm, j, func(ctx context.Context, blocks []uint64) {
		j.events = append(j.events, "free")
		bm.give(ctx, blocks)
	}, func() int { return 1 })
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
	b, _ := m.Take(context.Background())
	if err := m.Logged([]uint64{b}, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	j.events = nil
	m.ReturnAll(context.Background())
	if want := []string{"complete", "free"}; !slices.Equal(j.events, want) {
		t.Errorf("return did %v, want %v", j.events, want)
	}
	if got := bm.FreeCount(); got != 10000-1 {
		t.Errorf("%d free blocks after the return, want %d", got, 10000-1)
	}
}

// A block a write took and has not logged keeps a record: returning the
// reserve writes a new record for it before ending the old one and freeing the rest.
func TestReturnKeepsHandedOutBlocksRecorded(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(8, bm, j)
	b, _ := m.Take(context.Background())
	j.events = nil
	m.ReturnAll(context.Background())
	if want := []string{"commit", "complete", "free"}; !slices.Equal(j.events, want) {
		t.Errorf("return did %v, want %v", j.events, want)
	}
	m.Untake(b)
	j.events = nil
	m.ReturnAll(context.Background())
	if want := []string{"complete", "free"}; !slices.Equal(j.events, want) || bm.FreeCount() != 10000 {
		t.Errorf("second return did %v with %d free, want %v with all free", j.events, bm.FreeCount(), []string{"complete", "free"})
	}
}

// A write whose commit fails keeps its blocks in the reserve to put back.
func TestFailedLogKeepsBlocksOut(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(8, bm, j)
	b, _ := m.Take(context.Background())
	if err := m.Logged([]uint64{b}, func() error { return errors.New("commit failed") }); err == nil {
		t.Fatal("Logged hid the commit error")
	}
	m.Untake(b)
	if got := m.Stats().Blocks; got != 8 {
		t.Errorf("%d blocks in reserve after putting the block back, want 8", got)
	}
}

// The record is written again once it is a quarter of the journal behind the tail.
func TestRefreshRenewsAnOldRecord(t *testing.T) {
	bm, j := newFakeBitmap(10000, 0), &fakeJournal{}
	m := newManager(8, bm, j)
	m.Take(context.Background())
	j.events, j.lag = nil, 100
	m.Refresh(context.Background())
	if len(j.events) != 0 {
		t.Errorf("refresh of a recent record did %v, want nothing", j.events)
	}
	j.lag = 300
	m.Refresh(context.Background())
	if want := []string{"commit", "complete"}; !slices.Equal(j.events, want) {
		t.Errorf("refresh of an old record did %v, want %v", j.events, want)
	}
}
