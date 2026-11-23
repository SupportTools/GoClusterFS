package goclusterfs

import (
	"bytes"
	"context"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/cache"
)

type readResult struct {
	data  []byte
	errno syscall.Errno
}

// readInBackground starts a read of n bytes at off on f and waits up to wait
// for it, so a read that does not wait for a lock finishes inside a test hook.
func readInBackground(f *File, n int, off int64, wait time.Duration) <-chan readResult {
	done := make(chan readResult, 1)
	go func() {
		res, errno := f.Read(context.Background(), nil, make([]byte, n), off)
		var data []byte
		if errno == 0 {
			data, _ = res.Bytes(make([]byte, n))
		}
		done <- readResult{data, errno}
	}()
	select {
	case r := <-done:
		done <- r
	case <-time.After(wait):
	}
	return done
}

// describeMix says which parts of got match before and which match after, by 4 KB sector.
func describeMix(got, before, after []byte) string {
	var b bytes.Buffer
	for off := 0; off < len(got); off += 4096 {
		end := min(off+4096, len(got))
		switch {
		case end <= len(after) && bytes.Equal(got[off:end], after[off:end]):
			b.WriteByte('n')
		case end <= len(before) && bytes.Equal(got[off:end], before[off:end]):
			b.WriteByte('o')
		default:
			b.WriteByte('?')
		}
	}
	return fmt.Sprintf("%d bytes, by sector (n new, o old, ? neither): %s", len(got), b.String())
}

func TestReadNeverSeesPartOfAnotherNodesWrite(t *testing.T) {
	path := newTreeFSImage(t)
	lockPath := newLockImage(t)
	fsA := openLockedTreeFSNode(t, path, lockPath, 1)
	fsB := openLockedTreeFSNode(t, path, lockPath, 2)
	fileA := newRegularFile(t, fsA)
	before, after := pattern(1, 2*BlockSize), pattern(2, 2*BlockSize)
	// Written in reverse, the blocks are not contiguous, so the overwrite writes them one at a time.
	writeAt(t, fileA, before[BlockSize:], BlockSize)
	writeAt(t, fileA, before[:BlockSize], 0)
	fileB := fileNode(fsB, fileA.inodeNum)

	var read <-chan readResult
	blocks := 0
	t.Cleanup(func() { duringInPlaceWrite = nil })
	duringInPlaceWrite = func() {
		if blocks++; blocks == 2 {
			read = readInBackground(fileB, 2*BlockSize, 0, 300*time.Millisecond)
		}
	}
	writeAt(t, fileA, after, 0)
	if read == nil {
		t.Fatal("node A's write never reached its second block in place")
	}
	got := <-read
	if got.errno != 0 {
		t.Fatalf("node B read: %v", got.errno)
	}
	if !bytes.Equal(got.data, before) && !bytes.Equal(got.data, after) {
		t.Fatalf("node B's read during node A's write returned part of it: %s", describeMix(got.data, before, after))
	}
}

func TestReadNeverSeesPartOfAnotherNodesTruncate(t *testing.T) {
	path := newTreeFSImage(t)
	lockPath := newLockImage(t)
	fsA := openLockedTreeFSNode(t, path, lockPath, 1)
	fsB := openLockedTreeFSNode(t, path, lockPath, 2)
	fileA := newRegularFile(t, fsA)
	before := pattern(1, 2*BlockSize)
	writeAt(t, fileA, before, 0)
	fileB := fileNode(fsB, fileA.inodeNum)

	var read <-chan readResult
	t.Cleanup(func() { testHookAfterSetattrRead = nil })
	testHookAfterSetattrRead = func() {
		if read == nil {
			read = readInBackground(fileB, 2*BlockSize, 0, 300*time.Millisecond)
		}
	}
	in := &fuse.SetAttrIn{}
	in.Valid, in.Size = fuse.FATTR_SIZE, BlockSize/2
	if errno := fileA.Setattr(context.Background(), nil, in, &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("node A truncate: %v", errno)
	}
	if read == nil {
		t.Fatal("node A's truncate never reached its size change")
	}
	got := <-read
	if got.errno != 0 {
		t.Fatalf("node B read: %v", got.errno)
	}
	if !bytes.Equal(got.data, before) && !bytes.Equal(got.data, before[:BlockSize/2]) {
		t.Fatalf("node B's read during node A's truncate to %d bytes returned part of it: %s", BlockSize/2, describeMix(got.data, before, before[:BlockSize/2]))
	}
}

func TestReadSeesAnotherNodesWriteThatGrewACachedInode(t *testing.T) {
	path := newTreeFSImage(t)
	lockPath := newLockImage(t)
	fsA := openLockedTreeFSNode(t, path, lockPath, 1)
	fsB := openLockedTreeFSNode(t, path, lockPath, 2)
	cacheMgr, err := cache.NewManager(cache.DefaultConfig(), fsB.inodeTbl, nil)
	if err != nil {
		t.Fatal(err)
	}
	fsB.cacheMgr = cacheMgr
	fileA := newRegularFile(t, fsA)
	data := pattern(3, 2*BlockSize)
	writeAt(t, fileA, data[:BlockSize], 0)
	fileB := fileNode(fsB, fileA.inodeNum)
	checkAt(t, fileB, data[:BlockSize], 0)

	writeAt(t, fileA, data[BlockSize:], BlockSize)
	res, errno := fileB.Read(context.Background(), nil, make([]byte, 2*BlockSize), 0)
	if errno != 0 {
		t.Fatalf("node B read: %v", errno)
	}
	if got, _ := res.Bytes(make([]byte, 2*BlockSize)); !bytes.Equal(got, data) {
		t.Fatalf("node B's read after node A grew the file returned %s", describeMix(got, data[:BlockSize], data))
	}
}
