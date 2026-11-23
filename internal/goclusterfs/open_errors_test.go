package goclusterfs

import (
	"context"
	"errors"
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

// A node can also outlive its file when a new file takes the slot; nothing through the old node may reach the new file.
func TestStaleFileNodeNeverReachesTheNextFileInItsSlot(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	ctx := context.Background()
	old := createFile(t, fsys, "old")
	writeAt(t, old, pattern(1, 4096), 0)
	if errno := fsys.Unlink(ctx, "old"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	next := createFile(t, fsys, "next")
	if next.inodeNum != old.inodeNum {
		t.Fatalf("the new file got inode %d; the test needs it in the freed slot %d", next.inodeNum, old.inodeNum)
	}
	writeAt(t, next, pattern(2, 4096), 0)

	truncate := &fuse.SetAttrIn{}
	truncate.Valid, truncate.Size = fuse.FATTR_SIZE, 0
	// Open goes last: before the fix it reaches go-fuse, which panics without a mounted server.
	for _, c := range []struct {
		op   string
		call func() syscall.Errno
	}{
		{"getattr", func() syscall.Errno { return old.Getattr(ctx, nil, &fuse.AttrOut{}) }},
		{"read", func() syscall.Errno { _, errno := old.Read(ctx, nil, make([]byte, 4096), 0); return errno }},
		{"write", func() syscall.Errno { _, errno := old.Write(ctx, nil, pattern(3, 4096), 0); return errno }},
		{"truncate", func() syscall.Errno { return old.Setattr(ctx, nil, truncate, &fuse.AttrOut{}) }},
		{"allocate", func() syscall.Errno { return old.Allocate(ctx, nil, 0, BlockSize, 0) }},
		{"snapshot", func() syscall.Errno { return old.Setxattr(ctx, "user.snapshot", []byte("s"), 0) }},
		{"getxattr", func() syscall.Errno { _, errno := old.Getxattr(ctx, "user.snapshots", make([]byte, 64)); return errno }},
		{"open", func() syscall.Errno { _, _, errno := old.Open(ctx, syscall.O_RDONLY); return errno }},
	} {
		if errno := c.call(); errno != syscall.ESTALE {
			t.Errorf("%s through the old file's node: %v, want ESTALE", c.op, errno)
		}
	}
	checkAt(t, next, pattern(2, 4096), 0)
}

// A write's last check under the lock must catch a new file in the slot that maps the freed block at the same place.
func TestInPlaceWriteRefusesTheNextFileInItsSlot(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	old := createFile(t, fsys, "old")
	writeAt(t, old, pattern(1, 4096), 0)
	block := physicalBlock(t, fsys, old.inodeNum, 0)
	if errno := fsys.Unlink(context.Background(), "old"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	next := createFile(t, fsys, "next")
	writeAt(t, next, pattern(2, 4096), 0)
	if next.inodeNum != old.inodeNum || physicalBlock(t, fsys, next.inodeNum, 0) != block {
		t.Fatalf("the new file is inode %d at block %d; the test needs inode %d at block %d", next.inodeNum, physicalBlock(t, fsys, next.inodeNum, 0), old.inodeNum, block)
	}
	if err := old.stillWritableInPlace(0, block, 1); !errors.Is(err, syscall.ESTALE) {
		t.Errorf("in-place check for the old file's node: %v, want ESTALE", err)
	}
	checkAt(t, next, pattern(2, 4096), 0)
}
