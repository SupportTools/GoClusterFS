package goclusterfs

import (
	"bytes"
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// raceSetattr runs other between the next setattr's read of the inode and its
// write. It waits up to a second for other, so other finishes first unless the
// setattr holds the inode lock across its read and write.
func raceSetattr(t *testing.T, other func() syscall.Errno) <-chan syscall.Errno {
	t.Helper()
	done := make(chan syscall.Errno, 1)
	testHookAfterSetattrRead = func() {
		testHookAfterSetattrRead = nil
		go func() { done <- other() }()
		select {
		case errno := <-done:
			done <- errno
		case <-time.After(time.Second):
		}
	}
	t.Cleanup(func() { testHookAfterSetattrRead = nil })
	return done
}

// withBridge gives fsys a go-fuse bridge without mounting it, so creates can make nodes.
func withBridge(fsys *FS) *FS {
	fs.NewNodeFS(fsys, &fs.Options{})
	return fsys
}

func chmodIn(mode uint32) *fuse.SetAttrIn {
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_MODE
	in.Mode = mode
	return in
}

// Node A changes a directory's mode while node B makes a directory in it.
// Node B's link count and mtime must survive node A's write.
func TestDirSetattrKeepsOtherNodesMkdir(t *testing.T) {
	path := newFormattedImage(t)
	nodeA, nodeB := withBridge(openFormattedFS(t, path)), withBridge(openFormattedFS(t, path))
	ctx := context.Background()

	var out fuse.EntryOut
	if _, errno := nodeA.Mkdir(ctx, "d", 0o755, &out); errno != 0 {
		t.Fatalf("mkdir d: %v", errno)
	}
	num := uint32(out.Ino)
	before, err := nodeA.inodeTbl.Read(num)
	if err != nil {
		t.Fatal(err)
	}

	made := raceSetattr(t, func() syscall.Errno {
		_, errno := nodeB.makeDir(ctx, &nodeB.Inode, num, "sub", 0o755, &fuse.EntryOut{})
		return errno
	})
	if errno := (&Dir{fs: nodeA, inodeNum: num}).Setattr(ctx, nil, chmodIn(0o700), &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("chmod d on node A: %v", errno)
	}
	if errno := <-made; errno != 0 {
		t.Fatalf("mkdir d/sub on node B: %v", errno)
	}

	after, err := nodeA.inodeTbl.Read(num)
	if err != nil {
		t.Fatal(err)
	}
	if after.Mode&0o777 != 0o700 {
		t.Errorf("mode %o after chmod 700", after.Mode&0o777)
	}
	if after.LinkCount != before.LinkCount+1 {
		t.Errorf("link count %d after node B's mkdir, want %d", after.LinkCount, before.LinkCount+1)
	}
	if after.Mtime <= before.Mtime {
		t.Errorf("mtime %d after node B's mkdir, not later than %d", after.Mtime, before.Mtime)
	}
}

// Node A changes a file's mode while node B appends inside the file's only
// block, which changes the size but not the extents. Node A must keep that size.
func TestFileSetattrKeepsOtherNodesSize(t *testing.T) {
	path := newFormattedImage(t)
	nodeA, nodeB := openFormattedFS(t, path), openFormattedFS(t, path)
	ctx := context.Background()

	fA := newRegularFile(t, nodeA)
	first := pattern(1, 100)
	writeAt(t, fA, first, 0)
	fB := &File{fs: nodeB, inodeNum: fA.inodeNum}
	appended := pattern(2, 100)

	wrote := raceSetattr(t, func() syscall.Errno {
		n, errno := fB.Write(ctx, nil, appended, int64(len(first)))
		if errno == 0 && int(n) != len(appended) {
			return syscall.EIO
		}
		return errno
	})
	if errno := fA.Setattr(ctx, nil, chmodIn(0o600), &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("chmod on node A: %v", errno)
	}
	if errno := <-wrote; errno != 0 {
		t.Fatalf("append on node B: %v", errno)
	}

	ino, err := nodeA.inodeTbl.Read(fA.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if ino.Mode&0o777 != 0o600 {
		t.Errorf("mode %o after chmod 600", ino.Mode&0o777)
	}
	if want := uint64(len(first) + len(appended)); ino.Size != want {
		t.Fatalf("size %d after node B's append, want %d", ino.Size, want)
	}
	res, errno := fA.Read(ctx, nil, make([]byte, ino.Size), 0)
	if errno != 0 {
		t.Fatalf("read: %v", errno)
	}
	got, _ := res.Bytes(make([]byte, ino.Size))
	if !bytes.Equal(got, append(first, appended...)) {
		t.Error("file data differs from both nodes' writes")
	}
}
