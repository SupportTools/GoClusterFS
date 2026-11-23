package goclusterfs

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
)

// quotaProject makes directory name a project of fsys, whose journal is set, and returns it.
func quotaProject(t *testing.T, fsys *FS, name string) *Dir {
	t.Helper()
	d := mkdirIn(t, fsys, name)
	if errno := d.Setxattr(context.Background(), quotaXattr, []byte("bytes=1073741824"), 0); errno != 0 {
		t.Fatalf("set the quota of %s: %v", name, errno)
	}
	fsys.lockMgr.ReleaseCached()
	return d
}

// commitDelta logs a usage change of project as a charge does, with its intent completed.
func commitDelta(t *testing.T, fsys *FS, project uint32, blocks, inodes int64) {
	t.Helper()
	txn := fsys.journal.Begin()
	if err := txn.LogQuotaDelta(&journal.QuotaDeltaPayload{Project: project, Blocks: blocks, Inodes: inodes}); err != nil {
		t.Fatal(err)
	}
	seq, err := fsys.journal.Commit(txn)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.journal.CompleteIntent(seq); err != nil {
		t.Fatal(err)
	}
}

func tableUsage(t *testing.T, fsys *FS, project uint32) [2]uint64 {
	t.Helper()
	tbl, err := fsys.quotaTable()
	if err != nil {
		t.Fatal(err)
	}
	r, err := tbl.Record(project)
	if err != nil {
		t.Fatal(err)
	}
	return [2]uint64{r.UsedBlocks, r.UsedInodes}
}

func flushQuota(t *testing.T, fsys *FS) {
	t.Helper()
	if err := fsys.flushQuota(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// adoptCrashed keeps the locks of crashed node owner for a replay by fsys, as mount does, until the returned func.
func adoptCrashed(t *testing.T, fsys *FS, owner [16]byte) func() {
	t.Helper()
	adopted, err := fsys.lockMgr.AdoptOwner(owner, int(owner[15]))
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		if err := fsys.lockMgr.ReleaseAdopted(adopted); err != nil {
			t.Fatal(err)
		}
	}
}

// foldCrashed folds the quota usage of the crashed node's journal at offset into the table, as replay does.
func foldCrashed(t *testing.T, fsys *FS, offset, size uint64) {
	t.Helper()
	region, err := journal.OpenRegionForRecovery(fsys.dev, offset, size)
	if err != nil {
		t.Fatal(err)
	}
	owner := region.Superblock().NodeUUID
	defer adoptCrashed(t, fsys, owner)()
	if err := fsys.quotaGuard.Fold(lock.ForReplay(context.Background()), fsys.quotaTable, int(owner[15]), int(fsys.nodeUUID[15]), owner, region); err != nil {
		t.Fatalf("fold: %v", err)
	}
}

// A flush moves the ledger into the table and lets the checkpoint pass every delta. Reading the quota counts this
// node's unflushed usage too.
func TestQuotaFlushMovesTheLedgerIntoTheTable(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	bs := fsys.dataBlockSize()

	commitDelta(t, fsys, 1, 3, 2)
	if got, want := quotaOf(t, pvc), fmt.Sprintf(" used_bytes=%d used_inodes=2", 3*bs); !strings.HasSuffix(got, want) {
		t.Errorf("before the flush the quota reads %q, want it to end %q", got, want)
	}
	if got := tableUsage(t, fsys, 1); got != [2]uint64{} {
		t.Errorf("before the flush the table holds %v", got)
	}
	flushQuota(t, fsys)
	if got := tableUsage(t, fsys, 1); got != [2]uint64{3, 2} || len(fsys.journal.QuotaPending()) != 0 {
		t.Errorf("after the flush the table holds %v and the ledger %v; want 3 blocks and 2 inodes moved", got, fsys.journal.QuotaPending())
	}
	if got, want := quotaOf(t, pvc), fmt.Sprintf(" used_bytes=%d used_inodes=2", 3*bs); !strings.HasSuffix(got, want) {
		t.Errorf("after the flush the quota reads %q, want it to end %q", got, want)
	}
	commitDelta(t, fsys, 1, -1, 0)
	flushQuota(t, fsys)
	if got := tableUsage(t, fsys, 1); got != [2]uint64{2, 2} {
		t.Errorf("after a second flush the table holds %v, want 2 blocks and 2 inodes", got)
	}
	if _, err := fsys.journal.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if head, tail := fsys.journal.Region().HeadOffset(), fsys.journal.Region().TailOffset(); head != tail {
		t.Errorf("after the flushes a checkpoint stopped at %d, short of the tail at %d", head, tail)
	}
}

// The flush at unmount empties the ledger, so the journal stops clean.
func TestStoppingTheQuotaFlushEmptiesTheLedger(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	quotaProject(t, fsys, "pvc")
	fsys.StartQuotaFlush(time.Hour)
	commitDelta(t, fsys, 1, 4, 1)
	fsys.stopQuotaFlush()
	if got := tableUsage(t, fsys, 1); got != [2]uint64{4, 1} {
		t.Errorf("after the last flush the table holds %v, want 4 blocks and 1 inode", got)
	}
	if err := fsys.journal.Stop(); err != nil {
		t.Errorf("the journal did not stop clean after the last flush: %v", err)
	}
}

// After a crash, the replaying node folds the deltas no flush covered into the table, on top of what flushes wrote.
func TestUnflushedQuotaUsageIsFoldedAfterACrash(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	fsys = withBridge(fsys)
	quotaProject(t, fsys, "pvc")
	commitDelta(t, fsys, 1, 5, 1)
	flushQuota(t, fsys)
	commitDelta(t, fsys, 1, 2, 1)

	peer := openFormattedFS(t, path)
	owner := fsys.nodeUUID
	defer adoptCrashed(t, peer, owner)()
	fold := func(region *journal.Region) error {
		return peer.quotaGuard.Fold(lock.ForReplay(context.Background()), peer.quotaTable, int(owner[15]), int(peer.nodeUUID[15]), owner, region)
	}
	rctx := &journal.RecoveryContext{Dev: peer.dev, InodeTbl: peer.inodeTbl, BitmapMgr: peer.bitmapMgr, DirMgr: peer.dirMgr, ExtentMgr: peer.extentMgr}
	if _, err := journal.ScanFoldAndRecover(peer.dev, offset, size, fold, journal.CreateTxnRecoveryHandler(rctx), nil); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got := tableUsage(t, peer, 1); got != [2]uint64{7, 2} {
		t.Errorf("after the replay the table holds %v, want 7 blocks and 2 inodes", got)
	}
}

// A replayer that dies after appending its fold to the journal, before it wrote the usage, leaves the fold for the
// next replay, which writes it once. A completed fold owes nothing, so later flushes stay.
func TestAQuotaFoldCutShortIsRedoneOnce(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	fsys = withBridge(fsys)
	quotaProject(t, fsys, "pvc")
	commitDelta(t, fsys, 1, 5, 1)

	region, err := journal.OpenRegionForRecovery(fsys.dev, offset, size)
	if err != nil {
		t.Fatal(err)
	}
	q, err := journal.ReadQuotaReplay(region)
	if err != nil {
		t.Fatal(err)
	}
	folds, err := q.Fold(func(uint32) (uint64, uint64, error) { return 0, 0, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.AppendQuotaFold(region, folds); err != nil {
		t.Fatal(err)
	}

	peer := openFormattedFS(t, path)
	foldCrashed(t, peer, offset, size)
	foldCrashed(t, peer, offset, size)
	if got := tableUsage(t, peer, 1); got != [2]uint64{5, 1} {
		t.Errorf("after the fold was redone twice the table holds %v, want 5 blocks and 1 inode", got)
	}
	tbl, err := peer.quotaTable()
	if err != nil {
		t.Fatal(err)
	}
	if err := tbl.SetUsage(1, 6, 1); err != nil {
		t.Fatal(err)
	}
	foldCrashed(t, peer, offset, size)
	if got := tableUsage(t, peer, 1); got != [2]uint64{6, 1} {
		t.Errorf("a completed fold wrote the table again: %v, want the later 6 blocks", got)
	}
}

// While this node keeps a dead node's quota lock for its replay, its own lock passes over the claim, so a flush skips
// its turn: the dead node's journal may hold a flush the replay has not folded yet.
func TestQuotaFlushWaitsOutAdoptedQuotaLock(t *testing.T) {
	fsys, path, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	quotaProject(t, fsys, "pvc")
	dead := openFormattedComponents(t, path)
	if _, err := dead.quotaGuard.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	adopted, err := fsys.lockMgr.AdoptOwner(dead.nodeUUID, int(dead.nodeUUID[15]))
	if err != nil {
		t.Fatal(err)
	}
	commitDelta(t, fsys, 1, 3, 0)
	flushQuota(t, fsys)
	if got := tableUsage(t, fsys, 1); got != [2]uint64{} || fsys.journal.QuotaPending()[1].Blocks != 3 {
		t.Errorf("a flush during the replay wrote %v and left %v pending; want nothing written", got, fsys.journal.QuotaPending())
	}
	if err := fsys.lockMgr.ReleaseAdopted(adopted); err != nil {
		t.Fatal(err)
	}
	flushQuota(t, fsys)
	if got := tableUsage(t, fsys, 1); got != [2]uint64{3, 0} {
		t.Errorf("after the replay the flush wrote %v, want 3 blocks", got)
	}
}
