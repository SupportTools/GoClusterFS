package goclusterfs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
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

// remountAndReplay opens the image as a new node and replays the crashed node's
// journal, keeping that node's locks through the replay as mount does.
func remountAndReplay(t *testing.T, path string, offset, size uint64) *FS {
	t.Helper()
	fsys := openFormattedFS(t, path)
	region, err := journal.OpenRegionForRecovery(fsys.dev, offset, size)
	if err != nil {
		t.Fatalf("open the crashed node's journal: %v", err)
	}
	crashed := region.Superblock().NodeUUID
	// openFormattedFS puts each node's slot in the last byte of its UUID.
	adopted, err := fsys.lockMgr.AdoptOwner(crashed, int(crashed[15]))
	if err != nil {
		t.Fatalf("take over the crashed node's locks: %v", err)
	}
	replay := journal.CreateTxnRecoveryHandler(&journal.RecoveryContext{
		Dev: fsys.dev, InodeTbl: fsys.inodeTbl, BitmapMgr: fsys.bitmapMgr,
		DirMgr: fsys.dirMgr, ExtentMgr: fsys.extentMgr,
	})
	if _, err := journal.ScanAndRecover(fsys.dev, offset, size, replay); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if err := fsys.lockMgr.ReleaseAdopted(adopted); err != nil {
		t.Fatalf("release the crashed node's locks: %v", err)
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
	go func() { _, _, _, _ = fsys.Create(context.Background(), "crashed.txt", 0, 0o644, &fuse.EntryOut{}) }()
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	lookupFile(t, remounted, "crashed.txt")
	expectCleanFsck(t, remounted)
}

// A crash after a truncate's commit: replay cuts the file, frees the truncated blocks, and frees no block a file still maps.
func TestTruncateReplayKeepsMappedBlocks(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	victim := createFile(t, fsys, "victim.bin")
	writeAt(t, victim, pattern(1, 3*BlockSize), 0)
	other := createFile(t, fsys, "other.bin")
	writeAt(t, other, pattern(2, 2*BlockSize), 0)
	var blocks []uint64
	for i := range uint64(3) {
		blocks = append(blocks, physicalBlock(t, fsys, victim.inodeNum, i))
	}

	crashed := crashAfterCommit(t, fsys, "setattr")
	go func() { _ = tryTruncate(victim, BlockSize) }()
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	expectCleanFsck(t, remounted)
	checkFile(t, lookupFile(t, remounted, "victim.bin"), pattern(1, BlockSize))
	checkFile(t, lookupFile(t, remounted, "other.bin"), pattern(2, 2*BlockSize))
	expectAllocated(t, remounted, blocks[:1], true)
	expectAllocated(t, remounted, blocks[1:], false)
}

// A snapshot shares the file's blocks before a truncate crashes after its commit: replay drops the file's reference
// to the truncated blocks and leaves them to the snapshot.
func TestTruncateReplayKeepsSnapshotBlocks(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	victim := createFile(t, fsys, "victim.bin")
	writeAt(t, victim, pattern(1, 3*BlockSize), 0)
	setxattr(t, victim, "user.snapshot", "before")
	var blocks []uint64
	for i := range uint64(3) {
		blocks = append(blocks, physicalBlock(t, fsys, victim.inodeNum, i))
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		shared := true
		for _, b := range blocks {
			shared = shared && refcountOf(t, fsys, b) == 2
		}
		if shared {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the snapshot never shared the file's blocks")
		}
	}

	crashed := crashAfterCommit(t, fsys, "setattr")
	go func() { _ = tryTruncate(victim, BlockSize) }()
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	expectCleanFsck(t, remounted)
	expectAllocated(t, remounted, blocks, true)
	for i, want := range []uint32{2, 1, 1} {
		if got := refcountOf(t, remounted, blocks[i]); got != want {
			t.Errorf("block %d refcount %d after replay, want %d", blocks[i], got, want)
		}
	}
}

// A crash after an unlink's commit in a subdirectory: replay removes the entry and frees the inode and its blocks.
func TestUnlinkReplayFreesFileAndBlocks(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	ctx := context.Background()
	sub, f := newSubdirFile(t, fsys, pattern(3, 2*BlockSize))
	blocks := []uint64{physicalBlock(t, fsys, f.inodeNum, 0), physicalBlock(t, fsys, f.inodeNum, 1)}

	crashed := crashAfterCommit(t, fsys, "unlink")
	go func() { _ = sub.Unlink(ctx, "doomed.bin") }()
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	expectCleanFsck(t, remounted)
	subIno, err := remounted.inodeTbl.Read(sub.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := remounted.dirMgr.Lookup(ctx, subIno, "doomed.bin"); !errors.Is(err, dir.ErrNotFound) {
		t.Errorf("lookup doomed.bin after replay: err = %v, want ErrNotFound", err)
	}
	if ino, err := remounted.inodeTbl.Read(f.inodeNum); err != nil || ino.Magic != 0 {
		t.Errorf("inode %d after replay: magic %#x, err %v; want free", f.inodeNum, ino.Magic, err)
	}
	expectAllocated(t, remounted, blocks, false)
}

func expectAllocated(t *testing.T, fsys *FS, blocks []uint64, want bool) {
	t.Helper()
	for _, b := range blocks {
		if got, err := fsys.bitmapMgr.IsAllocated(b); err != nil || got != want {
			t.Errorf("block %d allocated = %v (err %v), want %v", b, got, err, want)
		}
	}
}

// newSubdir adds an empty directory to the root as Mkdir does.
func newSubdir(t *testing.T, fsys *FS, name string) *Dir {
	t.Helper()
	return newDirIn(t, fsys, inode.InodeRoot, name)
}

// newDirIn adds an empty directory to directory parent as Mkdir does.
func newDirIn(t *testing.T, fsys *FS, parent uint32, name string) *Dir {
	t.Helper()
	ctx := context.Background()
	num, err := fsys.inodeTbl.FindFreeSlot()
	if err != nil {
		t.Fatal(err)
	}
	ino, err := fsys.inodeTbl.ClaimSlot(ctx, num)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.dirMgr.InitDirectory(ctx, ino, uint64(parent)); err != nil {
		t.Fatal(err)
	}
	ino.Mode = inode.S_IFDIR | 0o755
	ino.LinkCount = 2
	if err := fsys.inodeTbl.Write(ctx, ino); err != nil {
		t.Fatal(err)
	}
	parentIno, err := fsys.inodeTbl.Read(parent)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.dirMgr.Add(ctx, parentIno, name, uint64(num), dir.DT_DIR); err != nil {
		t.Fatal(err)
	}
	if _, err := fsys.updateInode(ctx, parent, func(r *inode.Inode) error { r.LinkCount++; return nil }); err != nil {
		t.Fatal(err)
	}
	return &Dir{fs: fsys, inodeNum: num}
}
