package goclusterfs

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/lock"
)

func withBlockSize(fsys *FS, blockSize uint64) *FS {
	fsys.blockSize = blockSize
	fsys.bitmapMgr = bitmap.NewRegion(fsys.dev, treeFSBitmapOffset, treeFSDataBlocks*BlockSize/blockSize, fsys.lockMgr, fsys.nodeUUID)
	fsys.extentMgr = extent.NewManager(fsys.dev, fsys.bitmapMgr, fsys.lockMgr, treeFSDataOffset, blockSize)
	fsys.inodeTbl.SetTreeFlusher(fsys.extentMgr)
	fsys.dirMgr = dir.NewManager(fsys.dev, fsys.inodeTbl, fsys.extentMgr, fsys.lockMgr, treeFSDataOffset)
	return fsys
}

// At 8 KB blocks one extent lock covers each 1 MB of the data region, so a
// peer that holds it for one block holds off an in-place write to another
// block of that megabyte.
func TestExtentLockCoversAMegabyte(t *testing.T) {
	const blockSize = 8 << 10
	path, lockPath := newTreeFSImage(t), newLockImage(t)
	fsys := withBlockSize(openLockedTreeFSNode(t, path, lockPath, 1), blockSize)
	peer := withBlockSize(openLockedTreeFSNode(t, path, lockPath, 2), blockSize)
	f := newRegularFile(t, fsys)
	writeAt(t, f, pattern(1, 202*blockSize), 0)
	first, second := physicalBlock(t, fsys, f.inodeNum, 200), physicalBlock(t, fsys, f.inodeNum, 201)
	key := fsys.extentLockKey(second)
	if first == second || first == key || fsys.extentLockKey(first) != key {
		t.Fatalf("blocks %d and %d have extent lock keys %d and %d, want one key that is neither block", first, second, fsys.extentLockKey(first), key)
	}
	fsys.lockMgr.ReleaseCached()

	ctx := context.Background()
	if err := peer.lockMgr.AcquireExtent(ctx, key, lock.LockModeExclusive); err != nil {
		t.Fatal(err)
	}
	written := make(chan syscall.Errno, 1)
	go func() {
		_, errno := f.Write(ctx, nil, pattern(2, 4096), 200*blockSize)
		written <- errno
	}()
	select {
	case errno := <-written:
		t.Fatalf("the write to block %d ended (%v) while the peer held the extent lock of block %d", first, errno, second)
	case <-time.After(300 * time.Millisecond):
	}
	if err := peer.lockMgr.ReleaseExtent(key); err != nil {
		t.Fatal(err)
	}
	peer.lockMgr.ReleaseCached()
	if errno := <-written; errno != 0 {
		t.Fatalf("write after the peer let go: %v", errno)
	}
	checkAt(t, f, pattern(2, 4096), 200*blockSize)
}

// An overwrite of written blocks that one extent maps goes to the device in one
// write, and one that crosses a megabyte takes the extent locks of both.
func TestOverwriteOfARunIsOneDeviceWrite(t *testing.T) {
	const blockSize = 8 << 10
	path, lockPath := newTreeFSImage(t), newLockImage(t)
	fsys := withBlockSize(openLockedTreeFSNode(t, path, lockPath, 1), blockSize)
	f := newRegularFile(t, fsys)
	writeAt(t, f, pattern(1, 256*blockSize), 0)
	start, end := uint64(124), uint64(132)
	if fsys.extentLockKey(physicalBlock(t, fsys, f.inodeNum, start)) == fsys.extentLockKey(physicalBlock(t, fsys, f.inodeNum, end-1)) {
		t.Fatal("blocks 124 to 131 do not cross a megabyte of the data region")
	}

	var dataWrites atomic.Int64
	fsys.dev.SetWriteObserver(func(offset int64, n int) {
		if offset >= treeFSDataOffset {
			dataWrites.Add(1)
		}
	})
	data := pattern(2, int((end-start)*blockSize-4096))
	writeAt(t, f, data, int64(start*blockSize+512))
	fsys.dev.SetWriteObserver(nil)
	if n := dataWrites.Load(); n != 1 {
		t.Errorf("overwriting 8 blocks made %d data writes, want 1", n)
	}
	checkAt(t, f, data, int64(start*blockSize+512))
	checkAt(t, f, pattern(1, 256*blockSize)[:start*blockSize+512], 0)
}
