package goclusterfs

import (
	"context"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// A checkpoint between a create's commit and its changes, then a crash: the next mount must still replay the create.
func TestCreateReplaysAfterCheckpointAndCrash(t *testing.T) {
	path := newFormattedImage(t)
	fsys := openFormattedFS(t, path)
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatal(err)
	}
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	offset, size := sb.JournalRegionOffset, sb.JournalSlotSize()
	j, err := journal.Init(fsys.dev, offset, size, fsys.nodeUUID, 1, journal.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	fsys.journal = j

	crashed := make(chan struct{})
	afterCreateCommit = func() {
		if _, err := j.Checkpoint(); err != nil {
			t.Errorf("checkpoint: %v", err)
		}
		close(crashed)
		select {}
	}
	t.Cleanup(func() { afterCreateCommit = nil })
	go fsys.Create(ctx, "crashed.txt", 0, 0o644, &fuse.EntryOut{})
	<-crashed

	remounted := openFormattedFS(t, path)
	replay := journal.CreateTxnRecoveryHandler(&journal.RecoveryContext{
		Dev: remounted.dev, InodeTbl: remounted.inodeTbl, BitmapMgr: remounted.bitmapMgr,
		DirMgr: remounted.dirMgr, ExtentMgr: remounted.extentMgr,
	})
	result, err := journal.ScanAndRecover(remounted.dev, offset, size, replay)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	t.Logf("replay: %+v", result)
	lookupFile(t, remounted, "crashed.txt")
	if r := runFsck(t, remounted, fsck.DefaultOptions()); len(r.Problems) > 0 {
		t.Fatalf("fsck after replay:%s", problemList(r))
	}
}
