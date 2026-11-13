package goclusterfs

import (
	"context"
	"errors"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
)

// An unlink frees its inode and blocks, the node crashes before closing the intent, and a new file takes the inode's
// slot: replay must not free the new file.
func TestUnlinkCrashKeepsFileInReusedSlot(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	sub, f := newSubdirFile(t, fsys, pattern(3, 2*BlockSize))

	crashed := crashAfterCommit(t, fsys, "unlink: blocks freed")
	go func() { _ = sub.Unlink(context.Background(), "doomed.bin") }()
	<-crashed

	other := createFile(t, fsys, "other.bin")
	if other.inodeNum != f.inodeNum {
		t.Fatalf("other.bin took inode %d, want the unlinked file's slot %d", other.inodeNum, f.inodeNum)
	}
	data := pattern(9, 2*BlockSize)
	writeAt(t, other, data, 0)

	remounted := remountAndReplay(t, path, offset, size)
	checkFile(t, lookupFile(t, remounted, "other.bin"), data)
	expectCleanFsck(t, remounted)
}

// A root-directory unlink crashes after its commit: replay frees the file's inode and blocks, not only its entry.
func TestRootUnlinkReplayFreesFileAndBlocks(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	ctx := context.Background()
	f := createFile(t, fsys, "doomed.bin")
	writeAt(t, f, pattern(3, 2*BlockSize), 0)
	blocks := []uint64{physicalBlock(t, fsys, f.inodeNum, 0), physicalBlock(t, fsys, f.inodeNum, 1)}

	crashed := crashAfterCommit(t, fsys, "unlink")
	go func() { _ = fsys.Unlink(ctx, "doomed.bin") }()
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	expectCleanFsck(t, remounted)
	root, err := remounted.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := remounted.dirMgr.Lookup(ctx, root, "doomed.bin"); !errors.Is(err, dir.ErrNotFound) {
		t.Errorf("lookup doomed.bin after replay: err = %v, want ErrNotFound", err)
	}
	if ino, err := remounted.inodeTbl.Read(f.inodeNum); err != nil || ino.Magic != 0 {
		t.Errorf("inode %d after replay: magic %#x, err %v; want free", f.inodeNum, ino.Magic, err)
	}
	expectAllocated(t, remounted, blocks, false)
}

// Rmdir frees the directory's inode, in the root and in a subdirectory.
func TestRmdirFreesDirectoryInode(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	ctx := context.Background()
	sub := newSubdir(t, fsys, "sub")
	inner := newDirIn(t, fsys, sub.inodeNum, "inner")

	if errno := sub.Rmdir(ctx, "inner"); errno != 0 {
		t.Fatalf("rmdir sub/inner: %v", errno)
	}
	if errno := fsys.Rmdir(ctx, "sub"); errno != 0 {
		t.Fatalf("rmdir sub: %v", errno)
	}
	for _, num := range []uint32{inner.inodeNum, sub.inodeNum} {
		if ino, err := fsys.inodeTbl.Read(num); err != nil || ino.Magic != 0 {
			t.Errorf("directory inode %d after rmdir: magic %#x, err %v; want free", num, ino.Magic, err)
		}
	}
	expectCleanFsck(t, fsys)
}

// A write crashes after its commit: replay maps its new blocks and gives the file the write's size.
func TestWriteReplayMapsBlocksAndSize(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	f := createFile(t, fsys, "grown.bin")
	data := pattern(5, 2*BlockSize)

	crashed := crashAfterCommit(t, fsys, "write")
	go func() { _, _ = f.Write(context.Background(), nil, data, 0) }()
	<-crashed
	releaseDeadNodeLock(t, fsys, f.inodeNum)

	remounted := remountAndReplay(t, path, offset, size)
	checkFile(t, lookupFile(t, remounted, "grown.bin"), data)
	expectCleanFsck(t, remounted)
}

// A write crashes after its commit, and another node truncates the file before replay: replay must keep the
// truncate. The write's blocks are left for fsck to free.
func TestWriteReplayKeepsLaterTruncate(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	f := createFile(t, fsys, "cut.bin")

	crashed := crashAfterCommit(t, fsys, "write")
	go func() { _, _ = f.Write(context.Background(), nil, pattern(5, 2*BlockSize), 0) }()
	<-crashed
	releaseDeadNodeLock(t, fsys, f.inodeNum)
	truncateFile(t, lookupFile(t, openFormattedFS(t, path), "cut.bin"), 0)

	remounted := remountAndReplay(t, path, offset, size)
	checkFile(t, lookupFile(t, remounted, "cut.bin"), nil)
	expectRepairedClean(t, remounted)
}

// releaseDeadNodeLock frees the inode lock a crashed operation of fsys held, as a peer's takeover would.
func releaseDeadNodeLock(t *testing.T, fsys *FS, num uint32) {
	t.Helper()
	if err := fsys.lockMgr.Release(lock.LockTypeInode, uint64(num)); err != nil {
		t.Fatal(err)
	}
}
