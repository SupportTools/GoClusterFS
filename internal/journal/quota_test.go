package journal

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/node"
)

func commitQuota(t *testing.T, j *Journal, complete bool, log func(*Transaction) error) (uint64, *Transaction) {
	t.Helper()
	txn := j.Begin()
	if err := log(txn); err != nil {
		t.Fatal(err)
	}
	seq, err := j.Commit(txn)
	if err != nil {
		t.Fatal(err)
	}
	if complete {
		if err := j.CompleteIntent(seq); err != nil {
			t.Fatal(err)
		}
	}
	return seq, txn
}

func delta(t *testing.T, j *Journal, complete bool, project uint32, blocks, inodes int64) (uint64, *Transaction) {
	t.Helper()
	return commitQuota(t, j, complete, func(txn *Transaction) error {
		return txn.LogQuotaDelta(&QuotaDeltaPayload{Project: project, Blocks: blocks, Inodes: inodes})
	})
}

// flush commits a flush of project's whole ledger from usage old, as the flush loop does under the quota lock.
func flush(t *testing.T, j *Journal, complete bool, project uint32, oldBlocks, oldInodes uint64) QuotaUpdatePayload {
	t.Helper()
	var logged QuotaUpdatePayload
	txn := j.Begin()
	seq, err := j.CommitQuota(txn, func(pending func(uint32) QuotaDeltaPayload) error {
		d := pending(project)
		logged = QuotaUpdatePayload{Project: project, CoveredBlocks: d.Blocks, CoveredInodes: d.Inodes,
			OldBlocks: oldBlocks, OldInodes: oldInodes,
			NewBlocks: uint64(int64(oldBlocks) + d.Blocks), NewInodes: uint64(int64(oldInodes) + d.Inodes)}
		return txn.LogQuotaFlush(&logged)
	})
	if err != nil {
		t.Fatal(err)
	}
	if complete {
		if err := j.CompleteIntent(seq); err != nil {
			t.Fatal(err)
		}
	}
	return logged
}

func quotaAfterCrash(t *testing.T, j *Journal) (*Region, *QuotaReplay) {
	t.Helper()
	region, err := OpenRegionForRecovery(j.region.dev, j.region.regionOffset, j.region.regionSize)
	if err != nil {
		t.Fatal(err)
	}
	q, err := ReadQuotaReplay(region)
	if err != nil {
		t.Fatal(err)
	}
	return region, q
}

func pending(j *Journal, project uint32) QuotaDeltaPayload {
	return j.QuotaPending()[project]
}

// A flush covers exactly the deltas committed before it, so the ledger keeps only what came after.
func TestQuotaFlushCoversTheLedger(t *testing.T) {
	j := newTestJournal(t)
	delta(t, j, true, 1, 3, 1)
	delta(t, j, true, 2, 1, 0)
	delta(t, j, true, 1, 2, 0)
	if f := flush(t, j, true, 1, 10, 4); f.CoveredBlocks != 5 || f.CoveredInodes != 1 || f.NewBlocks != 15 || f.NewInodes != 5 {
		t.Errorf("the flush logged %+v; want it to cover 5 blocks and 1 inode, from 10/4 to 15/5", f)
	}
	delta(t, j, true, 1, -1, 0)
	if got := j.QuotaPending(); len(got) != 2 || got[1] != (QuotaDeltaPayload{Blocks: -1}) || got[2] != (QuotaDeltaPayload{Blocks: 1}) {
		t.Errorf("pending after the flush = %+v; want project 1 at -1 block and project 2 at 1 block", got)
	}
}

// A project's hold keeps the head at its first delta no durable flush covered. A durable flush moves the hold to the
// first delta after it, so steady writes do not pin the head for good, and drops it once nothing is pending.
func TestQuotaHoldsMoveToTheFirstDeltaAfterAFlush(t *testing.T) {
	l := newQuotaLedger()
	change := func(blocks int64, flush bool) []quotaChange {
		return []quotaChange{{project: 1, blocks: blocks, flush: flush}}
	}
	l.apply(5, 100, change(2, false), 1)
	l.apply(6, 200, change(3, false), 1)
	if h := l.holds[1]; h != (quotaHold{5, 100}) {
		t.Fatalf("hold after two deltas = %+v, want the first at 5", h)
	}
	l.apply(8, 300, change(-5, true), 1)
	l.apply(9, 400, change(1, false), 1)
	l.apply(10, 500, change(1, false), 1)
	l.settle(change(-5, true))
	if h := l.holds[1]; h != (quotaHold{9, 400}) {
		t.Errorf("hold after the flush settled = %+v, want the first delta after it, at 9", h)
	}
	l.apply(11, 600, change(1, false), -1)
	if h := l.holds[1]; h != (quotaHold{9, 400}) {
		t.Errorf("taking back a delta moved the hold to %+v", h)
	}
	l.apply(12, 700, change(-1, true), 1)
	l.settle(change(-1, true))
	if _, held := l.holds[1]; held {
		t.Errorf("a flush left project 1 with %+v pending and its hold in place", l.pending[1])
	}
	if _, kept := l.pending[1]; kept {
		t.Error("a project with nothing pending stays in the ledger")
	}
}

// A checkpoint keeps the records of deltas no flush covered, even once their intents complete, so a replay after a
// crash still finds them. After a flush covers them, a checkpoint drops them.
func TestQuotaHoldKeepsUnflushedDeltasThroughACheckpoint(t *testing.T) {
	j := newTestJournal(t)
	delta(t, j, true, 7, 4, 1)
	commitQuota(t, j, true, func(txn *Transaction) error { return txn.LogInodeCreate(testInodePayload(100)) })
	if _, err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, q := quotaAfterCrash(t, j); q.Running[7] != (QuotaDeltaPayload{Project: 7, Blocks: 4, Inodes: 1}) {
		t.Errorf("after a checkpoint and a crash the replay finds %+v, want project 7's 4 blocks and 1 inode", q.Running)
	}
	flush(t, j, true, 7, 0, 0)
	if _, err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if head, tail := j.Region().HeadOffset(), j.Region().TailOffset(); head != tail {
		t.Errorf("after a flush covered every delta the checkpoint stopped at %d, short of the tail at %d", head, tail)
	}
}

// The replay reads, per project, the deltas after its last flush, completed or not, and the flushes that did not
// complete. A transaction whose commit failed counts for neither, as the ledger took it back.
func TestReadQuotaReplay(t *testing.T) {
	j := newTestJournal(t)
	delta(t, j, true, 1, 5, 1)
	flush(t, j, true, 1, 10, 2)
	delta(t, j, true, 1, 2, 0)
	delta(t, j, false, 2, 0, -1)
	delta(t, j, true, 3, 6, 0)
	unfinished := flush(t, j, false, 3, 7, 0)
	seq, failed := delta(t, j, false, 5, 4, 0)
	j.dropIntent(seq, failed)

	if got := pending(j, 5); got.Blocks != 0 {
		t.Errorf("the failed commit's delta is still pending: %+v", got)
	}
	_, q := quotaAfterCrash(t, j)
	wantRunning := map[uint32]QuotaDeltaPayload{1: {Project: 1, Blocks: 2}, 2: {Project: 2, Inodes: -1}}
	if !maps.Equal(q.Running, wantRunning) {
		t.Errorf("running = %+v, want %+v", q.Running, wantRunning)
	}
	if !reflect.DeepEqual(q.Updates, []QuotaUpdatePayload{unfinished}) || q.Folded {
		t.Errorf("updates = %+v, folded %v; want only the unfinished flush %+v and no fold", q.Updates, q.Folded, unfinished)
	}
}

// The fold writes each unfinished update again, landed or not, adds the running deltas, stops at zero, and leaves out
// projects it does not change.
func TestQuotaReplayFold(t *testing.T) {
	q := &QuotaReplay{
		Running: map[uint32]QuotaDeltaPayload{1: {Project: 1, Blocks: 3}, 2: {Project: 2, Blocks: -5}, 4: {Project: 4, Inodes: 1}},
		Updates: []QuotaUpdatePayload{
			{Project: 1, OldBlocks: 10, OldInodes: 1, NewBlocks: 12, NewInodes: 1},
			{Project: 3, OldBlocks: 7, NewBlocks: 9},
			{Project: 5, OldBlocks: 1, NewBlocks: 2},
		},
	}
	table := map[uint32][2]uint64{1: {10, 1}, 2: {3, 0}, 3: {9, 0}, 5: {7, 0}}
	folds, err := q.Fold(func(p uint32) (uint64, uint64, error) { return table[p][0], table[p][1], nil })
	if err != nil {
		t.Fatal(err)
	}
	want := []QuotaUpdatePayload{
		{Project: 1, OldBlocks: 10, OldInodes: 1, NewBlocks: 15, NewInodes: 1},
		{Project: 2, OldBlocks: 3},
		{Project: 4, NewInodes: 1},
		{Project: 5, OldBlocks: 7, NewBlocks: 2},
	}
	if !reflect.DeepEqual(folds, want) {
		t.Errorf("folds = %+v\nwant    %+v", folds, want)
	}
}

// A fold appended to the journal stands for every quota record before it: a replay that stopped after appending it
// redoes only its writes that did not land. Once the fold completes, or replay began, the journal owes nothing.
func TestQuotaFoldInTheJournalStandsForTheRecordsBefore(t *testing.T) {
	j := newTestJournal(t)
	delta(t, j, true, 1, 5, 0)
	delta(t, j, true, 2, 1, 1)
	region, q := quotaAfterCrash(t, j)
	table := map[uint32][2]uint64{1: {10, 0}}
	used := func(p uint32) (uint64, uint64, error) { return table[p][0], table[p][1], nil }
	folds, err := q.Fold(used)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := AppendQuotaFold(region, folds)
	if err != nil {
		t.Fatal(err)
	}
	table[1] = [2]uint64{15, 0}

	region, q = quotaAfterCrash(t, j)
	if len(q.Running) != 0 || !reflect.DeepEqual(q.Updates, folds) || !q.Folded || q.FoldSeq != seq {
		t.Fatalf("after the fold: running %+v, updates %+v, folded %v at %d; want only the fold %+v at %d", q.Running, q.Updates, q.Folded, q.FoldSeq, folds, seq)
	}
	redo, err := q.Fold(used)
	if err != nil {
		t.Fatal(err)
	}
	if want := []QuotaUpdatePayload{{Project: 2, NewBlocks: 1, NewInodes: 1}}; !reflect.DeepEqual(redo, want) {
		t.Errorf("the redo writes %+v, want only project 2's change, which did not land: %+v", redo, want)
	}

	if err := CompleteQuotaFold(region, q.FoldSeq); err != nil {
		t.Fatal(err)
	}
	if _, q = quotaAfterCrash(t, j); !q.Folded || q.Owes() {
		t.Errorf("after the fold completed the journal reads as %+v; want folded with nothing owed", q)
	}

	if err := region.MarkRecovering(); err != nil {
		t.Fatal(err)
	}
	if q, err := ReadQuotaReplay(region); err != nil || !q.Folded || q.Owes() {
		t.Errorf("a journal whose replay began reads as %+v, %v; want folded with nothing owed", q, err)
	}
	if _, err := Recover(region, CreateTxnRecoveryHandler(&RecoveryContext{Dev: j.region.dev})); err != nil {
		t.Errorf("replaying the folded journal: %v", err)
	}
}

// A peer's replay of a dead journal passes it to FoldQuota, with the slot and its node, before replay begins, and then
// applies none of its quota records.
func TestPeerReplayFoldsQuotaRecordsFirst(t *testing.T) {
	j := newTestJournal(t)
	delta(t, j, true, 1, 5, 0)
	flush(t, j, false, 1, 0, 0)
	info := &node.FailedNodeInfo{SlotIndex: 7, UUID: [16]byte{9}, JournalOffset: j.region.regionOffset, JournalSize: j.region.regionSize}
	var folded []string
	fold := func(_ context.Context, slot int, owner [16]byte, region *Region) error {
		folded = append(folded, fmt.Sprintf("slot %d node %x %s", slot, owner[0], region.State()))
		return nil
	}
	result, err := ReplayJournal(context.Background(), &RecoveryContext{Dev: j.region.dev, FoldQuota: fold}, info)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"slot 7 node 9 Dirty"}; !reflect.DeepEqual(folded, want) {
		t.Errorf("FoldQuota saw %v, want %v", folded, want)
	}
	if result.EntriesReplayed != 0 || result.EntriesSkipped != 1 {
		t.Errorf("replay applied %d and skipped %d entries; want the unfinished flush skipped", result.EntriesReplayed, result.EntriesSkipped)
	}
}

// Stop leaves a journal dirty while a project has usage no flush moved into the table, so a replay folds it.
func TestStopLeavesUnflushedQuotaForReplay(t *testing.T) {
	j := newTestJournal(t)
	delta(t, j, true, 4, 2, 1)
	if err := j.Stop(); err == nil {
		t.Error("Stop with unflushed quota usage returned nil")
	}
	if _, q := quotaAfterCrash(t, j); q.Running[4] != (QuotaDeltaPayload{Project: 4, Blocks: 2, Inodes: 1}) {
		t.Errorf("after the stop the replay finds %+v, want project 4's 2 blocks and 1 inode", q.Running)
	}
}

// The pressure hook runs once the oldest delta no flush covered is half a ring behind, and not while no delta waits.
func TestQuotaPressureHookRunsBeforeHoldsFillTheRing(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	t.Cleanup(cleanup)
	j, err := Init(dev, 0, 256*1024, [16]byte{1}, 1, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	j.SetQuotaPressureHook(func() { calls++ })
	create := func() {
		commitQuota(t, j, true, func(txn *Transaction) error { return txn.LogInodeCreate(testInodePayload(100)) })
	}
	delta(t, j, true, 1, 1, 0)
	for range 64 {
		if calls > 0 {
			break
		}
		create()
	}
	if calls == 0 {
		t.Fatal("the hook never ran while a delta held the head")
	}
	flush(t, j, true, 1, 0, 0)
	if _, err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	calls = 0
	for range 64 {
		create()
		if _, err := j.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 0 {
		t.Errorf("the hook ran %d times with no delta waiting", calls)
	}
}
