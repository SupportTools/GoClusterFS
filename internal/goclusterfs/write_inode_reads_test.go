package goclusterfs

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/lock"
)

func holdFileIO(t *testing.T, f *File) func() {
	t.Helper()
	unlock, err := f.fs.lockFileIO(context.Background(), f.inodeNum, lock.LockModeExclusive)
	if err != nil {
		t.Fatal(err)
	}
	return unlock
}

func writeIOLockedAt(t *testing.T, f *File, data []byte, off int64) {
	t.Helper()
	if _, errno := f.writeIOLocked(context.Background(), data, off); errno != 0 {
		t.Fatalf("write at %d: %v", off, errno)
	}
}

// A 4K overwrite of an allocated, unshared block, as the kernel sends it after
// another write under the same file I/O lock hold, reads the inode twice: once
// to check the block in place and once to set mtime. The security.capability
// lookup before it and the plan read nothing.
func TestOverwriteUnderOneHoldReadsItsInodeTwice(t *testing.T) {
	fsys := openLockedTreeFS(t, newTreeFSImage(t))
	f := newRegularFile(t, fsys)
	unlock := holdFileIO(t, f)
	writeIOLockedAt(t, f, pattern(1, 2*BlockSize), 0)

	sector := inodeSector(f.inodeNum)
	var reads, inodeReads atomic.Int32
	fsys.dev.SetReadFault(func(off int64) error {
		reads.Add(1)
		if off == sector {
			inodeReads.Add(1)
		}
		return nil
	})
	if _, errno := f.Getxattr(context.Background(), "security.capability", make([]byte, 64)); errno != syscall.ENODATA {
		t.Fatalf("security.capability: errno %v, want ENODATA", errno)
	}
	if n := reads.Load(); n != 0 {
		t.Fatalf("security.capability took %d device reads, want 0", n)
	}
	overwrite := pattern(2, 4096)
	writeIOLockedAt(t, f, overwrite, BlockSize+8192)
	fsys.dev.SetReadFault(func(int64) error { return nil })
	unlock()

	if n := inodeReads.Load(); n != 2 {
		t.Fatalf("the overwrite read the inode %d times, want 2", n)
	}
	checkAt(t, f, overwrite, BlockSize+8192)
}

// While this node holds the file I/O lock, a peer's chmod writes the inode. A
// write into a hole then finds its cached plan out of date under the inode
// lock, plans again from disk, and keeps the peer's mode.
func TestWriteAfterAPeersChmodPlansAgainFromDisk(t *testing.T) {
	path, lockPath := newTreeFSImage(t), newLockImage(t)
	fsys := openLockedTreeFSNode(t, path, lockPath, 1)
	peer := openLockedTreeFSNode(t, path, lockPath, 2)
	f := newRegularFile(t, fsys)
	unlock := holdFileIO(t, f)
	writeIOLockedAt(t, f, pattern(1, 4096), 0)
	writeIOLockedAt(t, f, pattern(1, 4096), 2*BlockSize)
	fsys.lockMgr.ReleaseCached()

	in := &fuse.SetAttrIn{}
	in.Valid, in.Mode = fuse.FATTR_MODE, 0o600
	if errno := fileNode(peer, f.inodeNum).Setattr(context.Background(), nil, in, &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("peer chmod: %v", errno)
	}
	peer.lockMgr.ReleaseCached()

	data := pattern(3, 4096)
	writeIOLockedAt(t, f, data, BlockSize)
	unlock()
	checkAt(t, f, data, BlockSize)
	ino, err := fsys.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if mode := ino.Mode & 0o7777; mode != 0o600 {
		t.Fatalf("mode after the write = %o, want the peer's 600", mode)
	}
}
