package journal

import (
	"slices"
	"testing"
)

type reserveJournal struct {
	t *testing.T
	j *Journal
}

func newReserveJournal(t *testing.T) *reserveJournal {
	return &reserveJournal{t: t, j: newTestJournal(t)}
}

func (r *reserveJournal) commit(complete bool, log func(*Transaction) error) uint64 {
	r.t.Helper()
	txn := r.j.Begin()
	if err := log(txn); err != nil {
		r.t.Fatal(err)
	}
	seq, err := r.j.Commit(txn)
	if err != nil {
		r.t.Fatal(err)
	}
	if complete {
		r.complete(seq)
	}
	return seq
}

func (r *reserveJournal) complete(seq uint64) {
	r.t.Helper()
	if err := r.j.CompleteIntent(seq); err != nil {
		r.t.Fatal(err)
	}
}

func (r *reserveJournal) reserve(complete bool, blocks ...uint64) uint64 {
	return r.commit(complete, func(txn *Transaction) error { return txn.LogBlockReserve(blocks) })
}

func (r *reserveJournal) write(complete bool, disk uint64, length uint32) {
	r.commit(complete, func(txn *Transaction) error {
		return txn.LogExtentAdd(&ExtentAddPayload{InodeNum: 20, FileBlock: disk, DiskBlock: disk, Length: length, MetaGeneration: 1})
	})
}

func (r *reserveJournal) unused() []uint64 {
	r.t.Helper()
	region, err := OpenRegionForRecovery(r.j.region.dev, r.j.region.regionOffset, r.j.region.regionSize)
	if err != nil {
		r.t.Fatal(err)
	}
	blocks, _, err := ReservedUnused(region)
	if err != nil {
		r.t.Fatal(err)
	}
	return blocks
}

func blockRange(first, last uint64) []uint64 {
	var out []uint64
	for b := first; b <= last; b++ {
		out = append(out, b)
	}
	return out
}

// Blocks a pending reserve record holds are unused unless a later record maps
// or uses them, whether or not that record's intent completed. A use logged
// before the reserve record was of an earlier occupant of the block.
func TestReservedUnusedSubtractsLaterUses(t *testing.T) {
	r := newReserveJournal(t)
	r.write(true, 19, 1)
	r.reserve(false, blockRange(10, 19)...)
	r.write(true, 12, 2)
	r.commit(false, func(txn *Transaction) error { return txn.LogBlockUse([]uint64{15}) })
	r.write(false, 17, 1)
	if got, want := r.unused(), []uint64{10, 11, 14, 16, 18, 19}; !slices.Equal(got, want) {
		t.Errorf("unused reserved blocks %v, want %v", got, want)
	}
}

// A completed reserve record was used up or given back. When a node renews its
// reserve, only the renewed record's blocks count, less later uses.
func TestReservedUnusedIgnoresCompletedReserves(t *testing.T) {
	r := newReserveJournal(t)
	r.reserve(true, 1, 2, 3, 4)
	first := r.reserve(false, blockRange(10, 17)...)
	r.write(true, 10, 2)
	r.reserve(false, blockRange(12, 17)...)
	r.complete(first)
	r.write(false, 12, 1)
	if got, want := r.unused(), blockRange(13, 17); !slices.Equal(got, want) {
		t.Errorf("unused reserved blocks %v, want %v", got, want)
	}
}

// A reserved block that a write used, a truncate freed and another node may
// have allocated since must not be freed by replay.
func TestReservedUnusedKeepsUsedThenFreedBlock(t *testing.T) {
	r := newReserveJournal(t)
	r.reserve(false, 30, 31)
	r.write(true, 30, 1)
	r.commit(true, func(txn *Transaction) error {
		return txn.LogExtentFree(&ExtentFreePayload{InodeNum: 20, MetaGeneration: 2, Size: 0})
	})
	if got, want := r.unused(), []uint64{31}; !slices.Equal(got, want) {
		t.Errorf("unused reserved blocks %v, want %v", got, want)
	}
}

// Recovery passes the unused reserved blocks to release once, after the
// transactions, and resets the journal so no later replay frees them again.
func TestRecoverReleasingFreesUnusedReservedOnce(t *testing.T) {
	r := newReserveJournal(t)
	r.reserve(false, 40, 41, 42)
	r.write(false, 41, 1)
	region, err := OpenRegionForRecovery(r.j.region.dev, r.j.region.regionOffset, r.j.region.regionSize)
	if err != nil {
		t.Fatal(err)
	}
	var released [][]uint64
	var replayed int
	stats, err := RecoverReleasing(region, func(*RecoveredTxn) error { replayed++; return nil }, nil,
		func(blocks []uint64) error { released = append(released, blocks); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if want := [][]uint64{{40, 42}}; len(released) != 1 || !slices.Equal(released[0], want[0]) {
		t.Errorf("released %v, want %v", released, want)
	}
	if replayed != 2 || stats.ReservedFreed != 2 {
		t.Errorf("replayed %d transactions and freed %d reserved blocks, want 2 and 2", replayed, stats.ReservedFreed)
	}
	if region.NeedsRecovery() {
		t.Error("journal still needs recovery after the replay")
	}
	if got := r.unused(); len(got) != 0 {
		t.Errorf("a second replay would free %v again", got)
	}
}

func TestBlockRunsRoundTrip(t *testing.T) {
	blocks := []uint64{5, 6, 7, 9, 100, 101}
	got, err := DeserializeBlockRuns(serializeBlockRuns(blocks))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, blocks) {
		t.Errorf("round trip %v, want %v", got, blocks)
	}
	if len(serializeBlockRuns(blocks)) != 4+12*3 {
		t.Errorf("6 blocks in 3 runs took %d bytes, want %d", len(serializeBlockRuns(blocks)), 4+12*3)
	}
}

// Completing a crashed node's reserve intents in its journal stops replay from
// freeing their blocks, so fsck can free them itself.
func TestCompleteReservesStopsReplayFreeingThem(t *testing.T) {
	r := newReserveJournal(t)
	seq := r.reserve(false, 50, 51)
	region, err := OpenRegionForRecovery(r.j.region.dev, r.j.region.regionOffset, r.j.region.regionSize)
	if err != nil {
		t.Fatal(err)
	}
	blocks, reserves, err := ReservedUnused(region)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(blocks, []uint64{50, 51}) || !slices.Equal(reserves, []uint64{seq}) {
		t.Fatalf("reserved %v by records %v, want [50 51] by [%d]", blocks, reserves, seq)
	}
	if err := CompleteReserves(region, reserves); err != nil {
		t.Fatal(err)
	}
	if got := r.unused(); len(got) != 0 {
		t.Errorf("replay would still free %v", got)
	}
}
