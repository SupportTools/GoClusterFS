package goclusterfs

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// A node can outlive its file when another caller unlinks it. Opening or
// stat-ing that node must say it is stale, so the kernel looks the name up
// again, not that it is a directory or an I/O error.
func TestStaleFileNodeOpensAsStale(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	ctx := context.Background()
	victim := createFile(t, fsys, "victim")
	if errno := fsys.Unlink(ctx, "victim"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	if _, _, errno := victim.Open(ctx, syscall.O_RDONLY); errno != syscall.ESTALE {
		t.Errorf("open of the unlinked file's node: %v, want ESTALE", errno)
	}
	if errno := victim.Getattr(ctx, nil, &fuse.AttrOut{}); errno != syscall.ESTALE {
		t.Errorf("getattr of the unlinked file's node: %v, want ESTALE", errno)
	}

	newDirIn(t, fsys, 2, "reused")
	if _, _, errno := victim.Open(ctx, syscall.O_RDONLY); errno != syscall.ESTALE {
		t.Errorf("open of the node after a directory took its slot: %v, want ESTALE", errno)
	}
}

// The kernel sends CREATE when it saw no such name; another node or thread may
// have made it since. Without O_EXCL that create must open the file it finds.
func TestCreateWithoutExclOpensTheFileItFinds(t *testing.T) {
	fsys := withBridge(openFormattedFS(t, newFormattedImage(t)))
	ctx := context.Background()
	create := func(flags uint32) (*File, syscall.Errno) {
		node, _, _, errno := fsys.Create(ctx, "a", flags, 0o644, &fuse.EntryOut{})
		if errno != 0 {
			return nil, errno
		}
		return node.Operations().(*File), 0
	}

	first, errno := create(syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL)
	if errno != 0 {
		t.Fatalf("create a: %v", errno)
	}
	writeAt(t, first, pattern(1, 4096), 0)

	again, errno := create(syscall.O_RDWR | syscall.O_CREAT)
	if errno != 0 {
		t.Fatalf("create a again without O_EXCL: %v", errno)
	}
	if again.inodeNum != first.inodeNum {
		t.Errorf("create without O_EXCL opened inode %d, want the existing %d", again.inodeNum, first.inodeNum)
	}
	checkAt(t, again, pattern(1, 4096), 0)

	if _, errno := create(syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL); errno != syscall.EEXIST {
		t.Errorf("create with O_EXCL of an existing name: %v, want EEXIST", errno)
	}

	truncated, errno := create(syscall.O_RDWR | syscall.O_CREAT | syscall.O_TRUNC)
	if errno != 0 {
		t.Fatalf("create with O_TRUNC of an existing name: %v", errno)
	}
	if ino, err := fsys.inodeTbl.Read(truncated.inodeNum); err != nil || ino.Size != 0 {
		t.Errorf("after create with O_TRUNC: size %d, err %v; want 0", ino.Size, err)
	}
}
