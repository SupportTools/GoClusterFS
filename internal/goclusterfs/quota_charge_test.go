package goclusterfs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

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
		createIn(t, sub, fmt.Sprintf("%0200d", i)).Release(ctx, nil)
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
