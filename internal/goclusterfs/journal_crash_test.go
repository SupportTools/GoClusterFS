package goclusterfs

import (
	"context"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// journaledFS opens a formatted image with a root directory and a journal in slot 0's region.
func journaledFS(t *testing.T) (fsys *FS, path string, offset, size uint64) {
	t.Helper()
	path = newFormattedImage(t)
	fsys = openFormattedFS(t, path)
	if err := fsys.InitRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	offset, size = sb.JournalRegionOffset, sb.JournalSlotSize()
	if fsys.journal, err = journal.Init(fsys.dev, offset, size, fsys.nodeUUID, 1, journal.DefaultConfig()); err != nil {
		t.Fatal(err)
	}
	return fsys, path, offset, size
}

// crashAfterCommit makes the next op stop after its commit, as a crash would, once a checkpoint has run.
func crashAfterCommit(t *testing.T, fsys *FS, op string) <-chan struct{} {
	t.Helper()
	crashed := make(chan struct{})
	afterCommit = func(got string) {
		if got != op {
			return
		}
		if _, err := fsys.journal.Checkpoint(); err != nil {
			t.Errorf("checkpoint: %v", err)
		}
		close(crashed)
		select {}
	}
	t.Cleanup(func() { afterCommit = nil })
	return crashed
}

// remountAndReplay opens the image as the next mount would and replays the journal.
func remountAndReplay(t *testing.T, path string, offset, size uint64) *FS {
	t.Helper()
	fsys := openFormattedFS(t, path)
	replay := journal.CreateTxnRecoveryHandler(&journal.RecoveryContext{
		Dev: fsys.dev, InodeTbl: fsys.inodeTbl, BitmapMgr: fsys.bitmapMgr,
		DirMgr: fsys.dirMgr, ExtentMgr: fsys.extentMgr,
	})
	if _, err := journal.ScanAndRecover(fsys.dev, offset, size, replay); err != nil {
		t.Fatalf("replay: %v", err)
	}
	return fsys
}

func expectCleanFsck(t *testing.T, fsys *FS) {
	t.Helper()
	if r := runFsck(t, fsys, fsck.DefaultOptions()); len(r.Problems) > 0 {
		t.Fatalf("fsck after replay:%s", problemList(r))
	}
}

// A checkpoint between a create's commit and its changes, then a crash: the next mount must still replay the create.
func TestCreateReplaysAfterCheckpointAndCrash(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	crashed := crashAfterCommit(t, fsys, "create")
	go fsys.Create(context.Background(), "crashed.txt", 0, 0o644, &fuse.EntryOut{})
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	lookupFile(t, remounted, "crashed.txt")
	expectCleanFsck(t, remounted)
}

// A crash after a truncate's commit: replay must not free blocks that files still map.
func TestTruncateReplayKeepsMappedBlocks(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	victim := createFile(t, fsys, "victim.bin")
	writeAt(t, victim, pattern(1, 3*BlockSize), 0)
	other := createFile(t, fsys, "other.bin")
	writeAt(t, other, pattern(2, 2*BlockSize), 0)

	crashed := crashAfterCommit(t, fsys, "setattr")
	go tryTruncate(victim, BlockSize)
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	expectCleanFsck(t, remounted)
	checkFile(t, lookupFile(t, remounted, "other.bin"), pattern(2, 2*BlockSize))
}
