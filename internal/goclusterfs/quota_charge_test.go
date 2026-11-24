package goclusterfs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// usage is a project's usage in blocks and inodes as reading its quota on directory d shows it, which counts this
// node's unflushed changes.
func usage(t *testing.T, fsys *FS, d *Dir) [2]uint64 {
	t.Helper()
	var project, limitBytes, limitInodes, usedBytes, usedInodes uint64
	if _, err := fmt.Sscanf(quotaOf(t, d), "project=%d bytes=%d inodes=%d used_bytes=%d used_inodes=%d",
		&project, &limitBytes, &limitInodes, &usedBytes, &usedInodes); err != nil {
		t.Fatal(err)
	}
	return [2]uint64{usedBytes / fsys.dataBlockSize(), usedInodes}
}

// Every file operation charges its project the blocks it maps and the inodes it makes, and gives back what it frees:
// a write charges only the blocks it newly maps, a truncate and an unlink give back what they free, and an unlinked
// file still open keeps its usage until its last close.
func TestQuotaChargesFileOperations(t *testing.T) {
	fsys, _, _, _ := journaledImage(t, newFormattedImageAt(t, superblock.MinDeviceSize+64<<20, superblock.MinBlockSize))
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	ctx := context.Background()
	bs := int64(fsys.dataBlockSize())
	step := func(what string, blocks, inodes uint64) {
		t.Helper()
		if got, want := usage(t, fsys, pvc), [2]uint64{blocks, inodes}; got != want {
			t.Errorf("after %s the project uses %v blocks and inodes, want %v", what, got, want)
		}
	}

	f := createIn(t, pvc, "f")
	step("a create", 0, 1)
	writeAt(t, f, pattern(1, int(3*bs)), 0)
	step("a 3-block write", 3, 1)
	writeAt(t, f, pattern(2, int(bs)), 0)
	step("an overwrite", 3, 1)
	writeAt(t, f, pattern(3, 10), 5*bs)
	step("a write past a hole", 4, 1)
	if errno := f.Allocate(ctx, nil, uint64(6*bs), uint64(2*bs), 0); errno != 0 {
		t.Fatalf("fallocate: %v", errno)
	}
	step("a 2-block fallocate", 6, 1)
	in := &fuse.SetAttrIn{}
	in.Valid, in.Size = fuse.FATTR_SIZE, uint64(bs+1)
	if errno := f.Setattr(ctx, nil, in, &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("truncate: %v", errno)
	}
	step("a truncate to 2 blocks", 2, 1)

	sub := mkdirIn(t, pvc, "sub")
	step("a mkdir", 3, 2)
	// The flushes keep the ledger's holds from filling the journal, as the flush loop does.
	for i := range 80 {
		if errno := createIn(t, sub, fmt.Sprintf("%0200d", i)).Release(ctx, nil); errno != 0 {
			t.Fatalf("release: %v", errno)
		}
		flushQuota(t, fsys)
	}
	for i := range 80 {
		if errno := sub.Unlink(ctx, fmt.Sprintf("%0200d", i)); errno != 0 {
			t.Fatalf("unlink: %v", errno)
		}
		flushQuota(t, fsys)
	}
	subIno, err := fsys.inodeTbl.Read(sub.inodeNum)
	if err != nil || subIno.AllocatedBlocks < 2 {
		t.Fatalf("80 long names did not grow the directory: %+v, %v", subIno, err)
	}
	grown := subIno.AllocatedBlocks - 1
	step("growing a directory", 3+grown, 2)
	if _, errno := pvc.Symlink(ctx, "short", "s", &fuse.EntryOut{}); errno != 0 {
		t.Fatalf("symlink: %v", errno)
	}
	step("a short symlink", 3+grown, 3)
	if _, errno := pvc.Symlink(ctx, strings.Repeat("t", 200), "l", &fuse.EntryOut{}); errno != 0 {
		t.Fatalf("long symlink: %v", errno)
	}
	step("a long symlink", 4+grown, 4)

	if errno := pvc.Unlink(ctx, "f"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	step("unlinking a file still open", 4+grown, 4)
	if errno := f.Release(ctx, nil); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
	step("its last close", 2+grown, 3)
	mkdirIn(t, pvc, "empty")
	step("a second mkdir", 3+grown, 4)
	if errno := pvc.Rename(ctx, "sub", pvc, "empty", 0); errno != 0 {
		t.Fatalf("rename over a directory: %v", errno)
	}
	step("a rename over a directory", 2+grown, 3)

	flushQuota(t, fsys)
	if _, err := fsys.journal.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if head, tail := fsys.journal.Region().HeadOffset(), fsys.journal.Region().TailOffset(); head != tail {
		t.Errorf("after a flush a checkpoint stopped at %d, short of the tail at %d: an intent stays open", head, tail)
	}
}

// checkUsage compares a project's usage with what its files map, after retries a test forced.
func checkUsage(t *testing.T, fsys *FS, pvc *Dir, inodes uint64, files ...*File) {
	t.Helper()
	var blocks uint64
	for _, f := range files {
		ino, err := fsys.inodeTbl.Read(f.inodeNum)
		if err != nil {
			t.Fatal(err)
		}
		blocks += ino.AllocatedBlocks
	}
	if got, want := usage(t, fsys, pvc), [2]uint64{blocks, inodes}; got != want {
		t.Errorf("the project uses %v blocks and inodes, want what its files map: %v", got, want)
	}
}

// A create that loses its inode slot to a peer after its commit gives back its charge and retries on another slot.
func TestQuotaCreateLosingItsSlotIsChargedOnce(t *testing.T) {
	fsys, path, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	peer := openFormattedComponents(t, path)
	ctx := context.Background()
	taken := 0
	afterCommit = func(op string) {
		if op != "create" || taken > 0 {
			return
		}
		taken++
		num, err := fsys.inodeTbl.FindFreeSlot()
		if err == nil {
			_, err = peer.inodeTbl.ClaimSlot(ctx, num)
		}
		if err != nil {
			t.Errorf("the peer taking the create's slot: %v", err)
		}
	}
	t.Cleanup(func() { afterCommit = nil })
	f := createIn(t, pvc, "f")
	afterCommit = nil
	if taken != 1 {
		t.Fatal("the create never reached its commit")
	}
	checkUsage(t, fsys, pvc, 1, f)
}

// A write whose inode write hits a tree conflict after its commit gives back its charge before it retries.
func TestQuotaWriteRetriedAfterATreeConflictIsChargedOnce(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	f := createIn(t, pvc, "f")
	conflicts := 0
	testHookBeforeInodeCommit = func() error {
		if conflicts++; conflicts == 1 {
			return fmt.Errorf("injected: %w", extent.ErrTreeConflict)
		}
		return nil
	}
	t.Cleanup(func() { testHookBeforeInodeCommit = nil })
	writeAt(t, f, pattern(6, int(2*fsys.dataBlockSize())), 0)
	testHookBeforeInodeCommit = nil
	if conflicts < 2 {
		t.Fatalf("the write committed its inode %d times; want a conflict, then a retry", conflicts)
	}
	checkUsage(t, fsys, pvc, 1, f)
}

// A truncate whose inode changed under it after its commit gives back its credit before it retries.
func TestQuotaTruncateRetriedAfterARaceIsCreditedOnce(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	ctx := context.Background()
	bs := int64(fsys.dataBlockSize())
	f := createIn(t, pvc, "f")
	writeAt(t, f, pattern(7, int(3*bs)), 0)
	raced := raceSetattr(t, func() syscall.Errno {
		if _, err := fsys.updateInode(ctx, f.inodeNum, func(ino *inode.Inode) error {
			ino.Mtime++
			return nil
		}); err != nil {
			return syscall.EIO
		}
		return 0
	})
	in := &fuse.SetAttrIn{}
	in.Valid, in.Size = fuse.FATTR_SIZE, uint64(bs)
	if errno := f.Setattr(ctx, nil, in, &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("truncate: %v", errno)
	}
	if errno := <-raced; errno != 0 {
		t.Fatalf("the racing inode change: %v", errno)
	}
	checkUsage(t, fsys, pvc, 1, f)
}

// An operation that fails after its commit gives back what it charged: a mkdir whose inode write fails, and a write
// whose inode write fails after it logged its new blocks, which keeps only what a mid-write persist mapped.
func TestQuotaGivesBackFailedOperations(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	ctx := context.Background()
	f := createIn(t, pvc, "f")

	testHookBeforeMkdirInodeWrite = func() error { return errors.New("injected mkdir failure") }
	_, errno := pvc.Mkdir(ctx, "d", 0o755, &fuse.EntryOut{})
	testHookBeforeMkdirInodeWrite = nil
	if errno != syscall.EIO {
		t.Fatalf("mkdir with a failing inode write: %v, want EIO", errno)
	}
	testHookBeforeInodeCommit = func() error { return errors.New("injected inode write failure") }
	_, errno = f.Write(ctx, nil, pattern(8, int(2*fsys.dataBlockSize())), 0)
	testHookBeforeInodeCommit = nil
	if errno != syscall.EIO {
		t.Fatalf("write with a failing inode write: %v, want EIO", errno)
	}
	ino, err := fsys.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := usage(t, fsys, pvc), [2]uint64{ino.AllocatedBlocks, 1}; got != want {
		t.Errorf("after the failed mkdir and write the project uses %v blocks and inodes, want the file's %v", got, want)
	}
}

// Charges ride in the journal with the operations, so a crash before any flush loses none: the replay folds a create,
// a write and a truncate into the table.
func TestQuotaChargesSurviveACrash(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	ctx := context.Background()
	bs := int64(fsys.dataBlockSize())
	f := createIn(t, pvc, "f")
	writeAt(t, f, pattern(5, int(3*bs)), 0)
	in := &fuse.SetAttrIn{}
	in.Valid, in.Size = fuse.FATTR_SIZE, uint64(bs)
	if errno := f.Setattr(ctx, nil, in, &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("truncate: %v", errno)
	}
	if got := tableUsage(t, fsys, 1); got != [2]uint64{} {
		t.Fatalf("before any flush the table holds %v", got)
	}

	peer := openFormattedFS(t, path)
	foldCrashed(t, peer, offset, size)
	if got := tableUsage(t, peer, 1); got != [2]uint64{1, 1} {
		t.Errorf("after the crash the table holds %v blocks and inodes, want the file's 1 block and its inode", got)
	}
}

// waitUsage waits for a project's usage to reach want, as a background job such as a snapshot delete changes it.
func waitUsage(t *testing.T, fsys *FS, pvc *Dir, want [2]uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := usage(t, fsys, pvc)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the project uses %v blocks and inodes, want %v", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A snapshot's data inode counts against the project of the file it was taken of, with every block it shares, and
// deleting the snapshot gives them back.
func TestQuotaChargesSnapshots(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	f := createIn(t, pvc, "f")
	writeAt(t, f, pattern(9, int(2*fsys.dataBlockSize())), 0)
	setxattr(t, f, "user.snapshot", "s1")
	if got := usage(t, fsys, pvc); got != [2]uint64{4, 2} {
		t.Errorf("after a snapshot the project uses %v blocks and inodes, want the file's and the snapshot's 2 blocks and 1 inode each", got)
	}
	if errno := f.Setxattr(context.Background(), "user.snapshot", []byte("s1"), 0); errno != syscall.EEXIST {
		t.Fatalf("a second snapshot named s1: %v, want EEXIST", errno)
	}
	if got := usage(t, fsys, pvc); got != [2]uint64{4, 2} {
		t.Errorf("after a snapshot refused as a duplicate the project uses %v blocks and inodes, want 4 and 2 as before", got)
	}
	setxattr(t, f, "user.snapshot.delete", "s1")
	waitUsage(t, fsys, pvc, [2]uint64{2, 1})
}

// A file unlinked while open, whose node crashes before the unlink's changes land, is freed by the replay with no
// record of its own: the replaying node gives back its blocks and inode.
func TestQuotaCreditsAnOrphanTheReplayFrees(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	ctx := context.Background()
	f := createIn(t, pvc, "f")
	writeAt(t, f, pattern(4, 100), 0)
	crashed := crashAfterCommit(t, fsys, "unlink")
	go func() { _ = pvc.Unlink(ctx, "f") }()
	<-crashed

	peer := openFormattedFS(t, path)
	var err error
	if peer.journal, err = journal.Init(peer.dev, offset+size, size, peer.nodeUUID, 1, journal.DefaultConfig()); err != nil {
		t.Fatal(err)
	}
	defer adoptCrashed(t, peer, fsys.nodeUUID)()
	owner := fsys.nodeUUID
	fold := func(region *journal.Region) error {
		return peer.quotaGuard.Fold(lock.ForReplay(ctx), peer.quotaTable, int(owner[15]), int(peer.nodeUUID[15]), owner, region)
	}
	rctx := &journal.RecoveryContext{Dev: peer.dev, InodeTbl: peer.inodeTbl, BitmapMgr: peer.bitmapMgr, DirMgr: peer.dirMgr,
		ExtentMgr: peer.extentMgr, Charge: peer.ChargeQuota}
	if _, err := journal.ScanFoldAndRecover(peer.dev, offset, size, fold, journal.CreateTxnRecoveryHandler(rctx), nil); err != nil {
		t.Fatalf("replay: %v", err)
	}
	flushQuota(t, peer)
	if got := tableUsage(t, peer, 1); got != [2]uint64{} {
		t.Errorf("after the replay freed the orphan the table holds %v blocks and inodes, want none", got)
	}
}

// A symlink that loses its inode slot to a peer after its commit gives back its charge and retries on another slot.
func TestQuotaSymlinkLosingItsSlotIsChargedOnce(t *testing.T) {
	fsys, path, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	peer := openFormattedComponents(t, path)
	ctx := context.Background()
	taken := 0
	afterCommit = func(op string) {
		if op != "symlink" || taken > 0 {
			return
		}
		taken++
		num, err := fsys.inodeTbl.FindFreeSlot()
		if err == nil {
			_, err = peer.inodeTbl.ClaimSlot(ctx, num)
		}
		if err != nil {
			t.Errorf("the peer taking the symlink's slot: %v", err)
		}
	}
	t.Cleanup(func() { afterCommit = nil })
	if _, errno := pvc.Symlink(ctx, strings.Repeat("t", 200), "l", &fuse.EntryOut{}); errno != 0 {
		t.Fatalf("symlink: %v", errno)
	}
	afterCommit = nil
	if taken != 1 {
		t.Fatal("the symlink never reached its commit")
	}
	if got := usage(t, fsys, pvc); got != [2]uint64{1, 1} {
		t.Errorf("after the retried symlink the project uses %v blocks and inodes, want its 1 block and 1 inode", got)
	}
}

// A rename over a file frees the replaced file as an orphan after its own link drop. A crash between the free's commit
// and the free leaves the free's record for the replay; the rename's intent closed when its link drop landed, so the
// replay does not free the file a second time with a second credit.
func TestQuotaOrphanFreedOnceAfterACrash(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	ctx := context.Background()
	for _, name := range []string{"a", "b"} {
		f := createIn(t, pvc, name)
		writeAt(t, f, pattern(2, 100), 0)
		if errno := f.Release(ctx, nil); errno != 0 {
			t.Fatalf("release %s: %v", name, errno)
		}
	}
	crashed := crashAfterCommit(t, fsys, "orphan free")
	go func() { _ = pvc.Rename(ctx, "a", pvc, "b", 0) }()
	<-crashed

	peer := openFormattedFS(t, path)
	var err error
	if peer.journal, err = journal.Init(peer.dev, offset+size, size, peer.nodeUUID, 1, journal.DefaultConfig()); err != nil {
		t.Fatal(err)
	}
	defer adoptCrashed(t, peer, fsys.nodeUUID)()
	owner := fsys.nodeUUID
	fold := func(region *journal.Region) error {
		return peer.quotaGuard.Fold(lock.ForReplay(ctx), peer.quotaTable, int(owner[15]), int(peer.nodeUUID[15]), owner, region)
	}
	rctx := &journal.RecoveryContext{Dev: peer.dev, InodeTbl: peer.inodeTbl, BitmapMgr: peer.bitmapMgr, DirMgr: peer.dirMgr,
		ExtentMgr: peer.extentMgr, Charge: peer.ChargeQuota}
	if _, err := journal.ScanFoldAndRecover(peer.dev, offset, size, fold, journal.CreateTxnRecoveryHandler(rctx), nil); err != nil {
		t.Fatalf("replay: %v", err)
	}
	flushQuota(t, peer)
	if got := tableUsage(t, peer, 1); got != [2]uint64{1, 1} {
		t.Errorf("after the replay the table holds %v blocks and inodes, want the surviving file's 1 block and 1 inode", got)
	}
}
