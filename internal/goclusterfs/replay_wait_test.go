package goclusterfs

import (
	"bytes"
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// A write into a block whose lock a failed node held lets go of its inode lock
// and waits holding nothing, so the failed node's replay can take the inode
// lock and finish; the write then completes without EIO.
func TestInPlaceWriteStepsAsideForReplay(t *testing.T) {
	path, lockPath := newTreeFSImage(t), newLockImage(t)
	fsys := openLockedTreeFSNode(t, path, lockPath, 1)
	f := newRegularFile(t, fsys)
	writeAt(t, f, bytes.Repeat([]byte{'a'}, storage.SectorSize), 0)
	ino, err := fsys.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	ext, off, err := fsys.extentMgr.Lookup(ino, 0)
	if err != nil {
		t.Fatal(err)
	}
	block := ext.PhysicalBlock + off
	fsys.lockMgr.ReleaseCached()

	lockDev, err := storage.Open(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockDev.Close() }()
	var replayed atomic.Bool
	view := func(slot int, fresh bool) ([16]byte, lock.OwnerState) {
		switch {
		case slot == 3 && !replayed.Load():
			return [16]byte{3}, lock.OwnerFailing
		case slot == 1 || slot == 4:
			return [16]byte{byte(slot)}, lock.OwnerAlive
		}
		return [16]byte{}, lock.OwnerReleased
	}
	fsys.lockMgr.SetOwnerView(view)
	dead := lock.NewManager(lockDev, 0, testLockRegion, [16]byte{3}, 3)
	replayer := lock.NewManager(lockDev, 0, testLockRegion, [16]byte{4}, 4)
	replayer.SetOwnerView(view)
	ctx := context.Background()
	if err := dead.AcquireExtent(ctx, block, lock.LockModeExclusive); err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte{'b'}, storage.SectorSize)
	written := make(chan syscall.Errno, 1)
	go func() {
		_, errno := f.Write(ctx, nil, data, 0)
		written <- errno
	}()
	deadline := time.Now().Add(5 * time.Second)
	for st := fsys.lockMgr.Stats(); st.Held != 0 || st.Waiting != 1; st = fsys.lockMgr.Stats() {
		if time.Now().After(deadline) {
			t.Fatalf("the write still holds %d locks while it waits on the failed node's block", st.Held)
		}
		time.Sleep(10 * time.Millisecond)
	}

	replayCtx, cancel := context.WithTimeout(lock.ForReplay(ctx), 5*time.Second)
	defer cancel()
	if err := replayer.Acquire(replayCtx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
		t.Fatalf("the replay could not take the file's inode lock: %v", err)
	}
	if err := replayer.Release(lock.LockTypeInode, uint64(f.inodeNum)); err != nil {
		t.Errorf("the replay could not release the file's inode lock: %v", err)
	}
	replayer.ReleaseCached()
	if err := dead.ReleaseExtent(block); err != nil {
		t.Errorf("the failed node could not release block %d: %v", block, err)
	}
	dead.ReleaseCached()
	replayed.Store(true)

	select {
	case errno := <-written:
		if errno != 0 {
			t.Fatalf("write after the replay: %v", errno)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write still waiting 5s after the replay ended")
	}
	checkAt(t, f, data, 0)
}
