package goclusterfs

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
)

func expectNoLeak(t *testing.T, fsys *FS, free uint64) {
	t.Helper()
	if got := fsys.bitmapMgr.FreeCount(); got != free {
		t.Errorf("%d free blocks after the failure, want %d", got, free)
	}
	if r := runFsck(t, fsys, fsck.DefaultOptions()); len(r.Problems) > 0 {
		t.Fatalf("fsck after the failure:%s", problemList(r))
	}
}

// A write whose inode write fails gives back the blocks it allocated.
func TestFailedWriteFreesItsBlocks(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	f := createFile(t, fsys, "f.bin")
	free := fsys.bitmapMgr.FreeCount()

	testHookBeforeInodeCommit = func() error { return errors.New("injected inode write failure") }
	t.Cleanup(func() { testHookBeforeInodeCommit = nil })
	if _, errno := f.Write(context.Background(), nil, pattern(8, 4096), 0); errno != syscall.EIO {
		t.Fatalf("write with a failing inode write: errno %v, want EIO", errno)
	}
	testHookBeforeInodeCommit = nil

	expectNoLeak(t, fsys, free)
}

// A mkdir whose inode write fails gives back the new directory's block.
func TestFailedMkdirFreesItsBlock(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	ctx := context.Background()
	free := fsys.bitmapMgr.FreeCount()

	testHookBeforeMkdirInodeWrite = func() error { return errors.New("injected inode write failure") }
	t.Cleanup(func() { testHookBeforeMkdirInodeWrite = nil })
	if _, errno := fsys.Mkdir(ctx, "d", 0o755, &fuse.EntryOut{}); errno != syscall.EIO {
		t.Fatalf("mkdir with a failing inode write: errno %v, want EIO", errno)
	}
	testHookBeforeMkdirInodeWrite = nil

	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := fsys.dirMgr.Lookup(ctx, root, "d"); !errors.Is(err, dir.ErrNotFound) {
		t.Errorf("lookup d after the failed mkdir: err = %v, want ErrNotFound", err)
	}
	expectNoLeak(t, fsys, free)
}
