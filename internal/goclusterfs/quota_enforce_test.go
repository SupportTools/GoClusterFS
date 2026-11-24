package goclusterfs

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// limitedProject makes directory name a project of fsys with limits value, as quotaProject does with none to speak of.
func limitedProject(t *testing.T, fsys *FS, name, value string) *Dir {
	t.Helper()
	d := mkdirIn(t, fsys, name)
	if errno := d.Setxattr(context.Background(), quotaXattr, []byte(value), 0); errno != 0 {
		t.Fatalf("set the quota of %s: %v", name, errno)
	}
	fsys.lockMgr.ReleaseCached()
	return d
}

// Whatever would pass a project's block or inode limit fails with EDQUOT: a create, a mkdir, a write or fallocate of
// blocks the file does not map yet, a snapshot. Overwriting mapped blocks, truncating and unlinking always work.
func TestQuotaRefusesWhatPassesTheLimits(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	ctx := context.Background()
	bs := int64(fsys.dataBlockSize())
	// The project's root directory takes 1 block and 1 inode of these.
	pvc := limitedProject(t, fsys, "pvc", "bytes=5242880 inodes=4")

	f := createIn(t, pvc, "f")
	writeAt(t, f, pattern(1, int(3*bs)), 0)
	mkdirIn(t, pvc, "d")
	if _, errno := pvc.Symlink(ctx, "short", "s", &fuse.EntryOut{}); errno != 0 {
		t.Fatalf("a symlink within the limits: %v", errno)
	}
	if _, _, _, errno := pvc.Create(ctx, "g", syscall.O_RDWR, 0o644, &fuse.EntryOut{}); errno != syscall.EDQUOT {
		t.Errorf("a fifth inode under an inode limit of 4: %v, want EDQUOT", errno)
	}
	if _, errno := pvc.Symlink(ctx, "short", "s2", &fuse.EntryOut{}); errno != syscall.EDQUOT {
		t.Errorf("a symlink past the inode limit: %v, want EDQUOT", errno)
	}
	if _, errno := f.Write(ctx, nil, pattern(2, int(bs)), 3*bs); errno != syscall.EDQUOT {
		t.Errorf("a sixth block under a limit of 5: %v, want EDQUOT", errno)
	}
	writeAt(t, f, pattern(3, int(bs)), 0)
	if errno := f.Allocate(ctx, nil, uint64(4*bs), uint64(bs), 0); errno != syscall.EDQUOT {
		t.Errorf("a fallocate past the limit: %v, want EDQUOT", errno)
	}
	if errno := f.Setxattr(ctx, "user.snapshot", []byte("s1"), 0); errno != syscall.EDQUOT {
		t.Errorf("a snapshot of 3 blocks past the limit: %v, want EDQUOT", errno)
	}

	in := &fuse.SetAttrIn{}
	in.Valid, in.Size = fuse.FATTR_SIZE, uint64(bs)
	if errno := f.Setattr(ctx, nil, in, &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("a truncate in a full project: %v", errno)
	}
	writeAt(t, f, pattern(4, int(2*bs)), bs)
	if errno := f.Release(ctx, nil); errno != 0 {
		t.Fatal(errno)
	}
	if errno := pvc.Unlink(ctx, "f"); errno != 0 {
		t.Fatalf("an unlink in a full project: %v", errno)
	}
	if _, _, _, errno := pvc.Create(ctx, "g", syscall.O_RDWR, 0o644, &fuse.EntryOut{}); errno != 0 {
		t.Errorf("a create once the unlink freed an inode: %v", errno)
	}
}

// Close to a limit, where the unflushed changes of every live node could pass it together, an allocation flushes this
// node's changes and reads the usage again first; far from it, nothing is flushed.
func TestQuotaFlushesBeforeItCouldPassTheLimit(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	fsys.liveCount = func() int64 { return 2 }
	bs := int64(fsys.dataBlockSize())
	pvc := limitedProject(t, fsys, "pvc", "bytes=6291456")
	f := createIn(t, pvc, "f")
	writeAt(t, f, pattern(1, int(bs)), 0)
	writeAt(t, f, pattern(2, int(bs)), bs)
	if got := tableUsage(t, fsys, 1); got != [2]uint64{} {
		t.Errorf("far from the limit the table holds %v; want nothing flushed yet", got)
	}
	writeAt(t, f, pattern(3, int(bs)), 2*bs)
	if got := tableUsage(t, fsys, 1); got != [2]uint64{3, 2} {
		t.Errorf("with 2 nodes, 3 unflushed blocks and 1 more against a limit of 6 the table holds %v; want them and the 2 inodes flushed first", got)
	}
}

// A directory that becomes a project's root counts against it with its own inode and block, and removing it gives
// them back.
func TestQuotaCountsTheProjectRoot(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	limitedProject(t, fsys, "pvc", "inodes=10")
	if got := fsys.journal.QuotaPending()[1]; got.Blocks != 1 || got.Inodes != 1 {
		t.Errorf("a new project's root counts %+v, want its 1 block and 1 inode", got)
	}
	if errno := fsys.Rmdir(context.Background(), "pvc"); errno != 0 {
		t.Fatalf("rmdir of the project root: %v", errno)
	}
	if got := fsys.journal.QuotaPending()[1]; got.Blocks != 0 || got.Inodes != 0 {
		t.Errorf("after removing the project root the project counts %+v, want nothing", got)
	}
}
