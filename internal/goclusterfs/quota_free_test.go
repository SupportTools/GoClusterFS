package goclusterfs

import (
	"context"
	"fmt"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/quota"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

func record(t *testing.T, fsys *FS, id uint32) *quota.Record {
	t.Helper()
	tbl, err := fsys.quotaTable()
	if err != nil {
		t.Fatal(err)
	}
	rec, err := tbl.Record(id)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

func putRecord(t *testing.T, fsys *FS, id uint32, rec *quota.Record) {
	t.Helper()
	tbl, err := fsys.quotaTable()
	if err != nil {
		t.Fatal(err)
	}
	if err := tbl.PutRecord(id, rec); err != nil {
		t.Fatal(err)
	}
}

// reuseRecord makes the record of project id hold a later project, as a peer that freed it and gave it to a new
// directory would, and returns the later project's ID.
func reuseRecord(t *testing.T, fsys *FS, id uint32, blocks, inodes uint64) uint32 {
	t.Helper()
	later := id + 1<<quota.IndexBits
	putRecord(t, fsys, later, &quota.Record{ID: later, Flags: quota.RecordInUse, UsedBlocks: blocks, UsedInodes: inodes})
	return later
}

// A project's record is freed at the flush after its root directory is removed with nothing left in the project. The
// next new project takes the record after the last one handed out, not the freed one, and the project that reuses a
// record gets a new ID.
func TestQuotaRecordFreedOnceItsProjectIsGone(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	ctx := context.Background()
	pvc := quotaProject(t, fsys, "pvc")
	f := createIn(t, pvc, "f")
	writeAt(t, f, pattern(1, 100), 0)
	if errno := f.Release(ctx, nil); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
	if errno := pvc.Unlink(ctx, "f"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	flushQuota(t, fsys)
	if rec := record(t, fsys, 1); !rec.Owns(1) {
		t.Fatalf("with its root directory still there project 1's record is free: %+v", rec)
	}
	if errno := fsys.Rmdir(ctx, "pvc"); errno != 0 {
		t.Fatalf("rmdir: %v", errno)
	}
	flushQuota(t, fsys)
	if rec := record(t, fsys, 1); rec.Flags&quota.RecordInUse != 0 || rec.ID != 1 {
		t.Fatalf("after its root directory went project 1's record is %+v; want it free, naming project 1 last", rec)
	}
	if got := quotaOf(t, quotaProject(t, fsys, "second")); !strings.HasPrefix(got, "project=2 ") {
		t.Errorf("the next project after a free reads %q; want record 2, round-robin, not the freed record 1", got)
	}

	tbl, err := fsys.quotaTable()
	if err != nil {
		t.Fatal(err)
	}
	h, err := tbl.Header()
	if err != nil {
		t.Fatal(err)
	}
	h.NextProjectID = 1
	if err := tbl.PutHeader(h); err != nil {
		t.Fatal(err)
	}
	next := quotaProject(t, fsys, "next")
	if got, want := quotaOf(t, next), fmt.Sprintf("project=%d ", 1+1<<quota.IndexBits); !strings.HasPrefix(got, want) {
		t.Errorf("the project that reused the record reads %q, want it to start with %q", got, want)
	}
}

// A file unlinked while open stays in its project after the project's root directory goes, so the record stays until
// the file's last close.
func TestQuotaRecordKeptWhileAnOrphanIsInIt(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	ctx := context.Background()
	pvc := quotaProject(t, fsys, "pvc")
	f := createIn(t, pvc, "f")
	writeAt(t, f, pattern(1, 100), 0)
	if errno := pvc.Unlink(ctx, "f"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	if errno := fsys.Rmdir(ctx, "pvc"); errno != 0 {
		t.Fatalf("rmdir: %v", errno)
	}
	flushQuota(t, fsys)
	if rec := record(t, fsys, 1); !rec.Owns(1) || rec.UsedInodes != 1 {
		t.Fatalf("with an open file left in it project 1's record is %+v; want it in use with that file's inode", rec)
	}
	if errno := f.Release(ctx, nil); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
	flushQuota(t, fsys)
	if rec := record(t, fsys, 1); rec.Flags&quota.RecordInUse != 0 {
		t.Errorf("after the open file's last close project 1's record is %+v; want it free", rec)
	}
}

// A crashed node's journal may still hold changes of a project whose record was freed and given to a later project
// by the time a peer folds it. The fold drops them, and the later project's usage stays as it was; another project's
// changes in the same journal still fold.
func TestStaleQuotaChangesAreDroppedAtTheFold(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	fsys = withBridge(fsys)
	quotaProject(t, fsys, "old")
	quotaProject(t, fsys, "other")
	flushQuota(t, fsys)
	commitDelta(t, fsys, 1, 5, 1)
	commitDelta(t, fsys, 2, 3, 1)

	peer := openFormattedFS(t, path)
	later := reuseRecord(t, peer, 1, 7, 2)
	other := tableUsage(t, peer, 2)
	owner := fsys.nodeUUID
	defer adoptCrashed(t, peer, owner)()
	fold := func(region *journal.Region) error {
		return peer.quotaGuard.Fold(lock.ForReplay(context.Background()), peer.quotaTable, int(owner[15]), int(peer.nodeUUID[15]), owner, region)
	}
	rctx := &journal.RecoveryContext{Dev: peer.dev, InodeTbl: peer.inodeTbl, BitmapMgr: peer.bitmapMgr, DirMgr: peer.dirMgr, ExtentMgr: peer.extentMgr}
	if _, err := journal.ScanFoldAndRecover(peer.dev, offset, size, fold, journal.CreateTxnRecoveryHandler(rctx), nil); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if rec := record(t, peer, later); !rec.Owns(later) || rec.UsedBlocks != 7 || rec.UsedInodes != 2 {
		t.Errorf("after the fold the record holds %+v; want project %d with its 7 blocks and 2 inodes untouched", rec, later)
	}
	if got, want := tableUsage(t, peer, 2), [2]uint64{other[0] + 3, other[1] + 1}; got != want {
		t.Errorf("after the fold project 2 uses %v, want %v", got, want)
	}
}

// A node whose ledger still holds changes of a project whose record a peer gave to a later project drops them at its
// flush, and lets go of the journal they pinned.
func TestQuotaFlushDropsChangesOfAReusedRecord(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	quotaProject(t, fsys, "old")
	flushQuota(t, fsys)
	commitDelta(t, fsys, 1, 5, 1)
	later := reuseRecord(t, fsys, 1, 7, 2)
	flushQuota(t, fsys)
	if rec := record(t, fsys, later); !rec.Owns(later) || rec.UsedBlocks != 7 || rec.UsedInodes != 2 {
		t.Errorf("after the flush the record holds %+v; want project %d with its 7 blocks and 2 inodes untouched", rec, later)
	}
	if pending := fsys.journal.QuotaPending(); len(pending) != 0 {
		t.Errorf("after the flush %v is still pending", pending)
	}
	if _, err := fsys.journal.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if head, tail := fsys.journal.Region().HeadOffset(), fsys.journal.Region().TailOffset(); head != tail {
		t.Errorf("a checkpoint stopped at %d, short of the tail at %d: the dropped changes still pin the journal", head, tail)
	}
}

// A project whose usage drifted down to nothing keeps its record while its root directory is still in it.
func TestQuotaRecordKeptWhileItsRootIsThere(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	quotaProject(t, fsys, "pvc")
	flushQuota(t, fsys)
	used := tableUsage(t, fsys, 1)
	commitDelta(t, fsys, 1, -int64(used[0]), -int64(used[1]))
	flushQuota(t, fsys)
	if rec := record(t, fsys, 1); !rec.Owns(1) || rec.UsedBlocks != 0 || rec.UsedInodes != 0 {
		t.Errorf("with its usage at nothing and its root directory there project 1's record is %+v; want it kept", rec)
	}
}

// A directory whose project's record a later project took has no project to the runtime: no limits, no quota to read,
// and statfs reports the volume.
func TestQuotaProjectWithAReusedRecordHasNoLimits(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	flushQuota(t, fsys)
	later := uint32(1 + 1<<quota.IndexBits)
	putRecord(t, fsys, later, &quota.Record{ID: later, Flags: quota.RecordInUse, LimitBlocks: 1, LimitInodes: 1, UsedBlocks: 1, UsedInodes: 1})
	if _, err := fsys.projectRecord(1, true); err != nil {
		t.Fatal(err)
	}

	writeAt(t, createIn(t, pvc, "f"), pattern(1, int(fsys.dataBlockSize())), 0)
	if _, errno := pvc.Getxattr(context.Background(), quotaXattr, make([]byte, 256)); errno != syscall.ENODATA {
		t.Errorf("reading the quota: %v, want ENODATA", errno)
	}
	var vol, got fuse.StatfsOut
	if errno := fsys.Statfs(context.Background(), &vol); errno != 0 {
		t.Fatal(errno)
	}
	if errno := pvc.Statfs(context.Background(), &got); errno != 0 || got.Blocks != vol.Blocks {
		t.Errorf("statfs reports %d blocks, %v; want the volume's %d", got.Blocks, errno, vol.Blocks)
	}
}

// A flush that cannot read a project's root directory takes it as still in the project, and keeps the record.
func TestQuotaRootThatCannotBeReadKeepsItsRecord(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	rec := record(t, fsys, 1)
	sector := int64(sb.InodeTableOffset + uint64(pvc.inodeNum)*inode.InodeSize&^(storage.SectorSize-1))
	fsys.dev.SetReadFault(func(off int64) error {
		if off == sector {
			return syscall.EIO
		}
		return nil
	})
	carries := fsys.rootCarries(rec, 1)
	fsys.dev.SetReadFault(func(int64) error { return nil })
	if !carries {
		t.Error("with its root directory unreadable project 1's root counts as gone; want it kept in the project")
	}
}
