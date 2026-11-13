package goclusterfs

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
)

// TestWriteFinishesAfterInterruptDuringLockWait cancels a write's request
// context, as a FUSE interrupt does, while the write waits for an inode lock
// another node holds. The write must finish once the lock is free.
func TestWriteFinishesAfterInterruptDuringLockWait(t *testing.T) {
	path := newFormattedImage(t)
	nodeA, nodeB := openFormattedFS(t, path), openFormattedFS(t, path)
	fileA := newRegularFile(t, nodeA)
	fileB := &File{fs: nodeB, inodeNum: fileA.inodeNum}

	held, released := make(chan error, 1), make(chan struct{})
	go func() {
		res := uint64(fileA.inodeNum)
		err := nodeA.lockMgr.Acquire(context.Background(), lock.LockTypeInode, res, lock.LockModeExclusive)
		held <- err
		if err == nil {
			time.Sleep(500 * time.Millisecond)
			nodeA.lockMgr.Release(lock.LockTypeInode, res)
		}
		close(released)
	}()
	if err := <-held; err != nil {
		t.Fatalf("node A lock: %v", err)
	}

	ctx, interrupt := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, interrupt)
	data := []byte("written after the interrupt")
	n, errno := fileB.Write(ctx, nil, data, 0)
	<-released
	if errno != 0 || int(n) != len(data) {
		t.Fatalf("Write = (%d, %v), want (%d, 0)", n, errno, len(data))
	}
}

// TestWriteFailsWhenLockIsNotFreed keeps an inode lock held on another node past
// lock.DefaultTimeout. The interrupted write must give up with EIO, not hang.
func TestWriteFailsWhenLockIsNotFreed(t *testing.T) {
	if testing.Short() {
		t.Skip("waits lock.DefaultTimeout")
	}
	path := newFormattedImage(t)
	nodeA, nodeB := openFormattedFS(t, path), openFormattedFS(t, path)
	fileA := newRegularFile(t, nodeA)
	fileB := &File{fs: nodeB, inodeNum: fileA.inodeNum}

	release := holdInodeLock(t, nodeA, uint64(fileA.inodeNum))
	ctx, interrupt := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, interrupt)
	start := time.Now()
	_, errno := fileB.Write(ctx, nil, []byte("never written"), 0)
	elapsed := time.Since(start)
	release()
	if errno != syscall.EIO {
		t.Fatalf("Write with the lock held elsewhere = %v, want EIO", errno)
	}
	if elapsed < lock.DefaultTimeout {
		t.Errorf("Write gave up after %v, before lock.DefaultTimeout (%v)", elapsed, lock.DefaultTimeout)
	}
}

// holdInodeLock takes an inode lock on fsys and keeps it until release is
// called. Like a busy holder, it rejoins the lock every second, which keeps the
// record from going stale.
func holdInodeLock(t *testing.T, fsys *FS, res uint64) (release func()) {
	t.Helper()
	held, stop, stopped := make(chan error, 1), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		err := fsys.lockMgr.Acquire(context.Background(), lock.LockTypeInode, res, lock.LockModeExclusive)
		held <- err
		if err != nil {
			return
		}
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				fsys.lockMgr.Release(lock.LockTypeInode, res)
				return
			case <-tick.C:
				if fsys.lockMgr.Acquire(context.Background(), lock.LockTypeInode, res, lock.LockModeExclusive) == nil {
					fsys.lockMgr.Release(lock.LockTypeInode, res)
				}
			}
		}
	}()
	if err := <-held; err != nil {
		t.Fatalf("hold inode lock %d: %v", res, err)
	}
	return func() {
		close(stop)
		<-stopped
	}
}

// TestCreateReportsStuckInodeLockAsEIO keeps the lock on the inode slot a
// create will claim held on another node past lock.DefaultTimeout. The create
// must fail with EIO, as for any lock wait that gives up, not ENOSPC: the
// inode table is not full.
func TestCreateReportsStuckInodeLockAsEIO(t *testing.T) {
	if testing.Short() {
		t.Skip("waits lock.DefaultTimeout")
	}
	path := newFormattedImage(t)
	nodeA, nodeB := openFormattedFS(t, path), openFormattedFS(t, path)
	slot, err := nodeB.inodeTbl.FindFreeSlot()
	if err != nil {
		t.Fatal(err)
	}
	release := holdInodeLock(t, nodeA, uint64(slot))
	_, errno := nodeB.createFileOnce(context.Background(), &nodeB.Inode, inode.InodeRoot, "stuck", 0o644, &fuse.EntryOut{})
	release()
	if errno != syscall.EIO {
		t.Fatalf("create while inode %d stays locked = %v, want EIO", slot, errno)
	}
	root, err := nodeB.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := nodeB.dirMgr.Lookup(context.Background(), root, "stuck"); !errors.Is(err, dir.ErrNotFound) {
		t.Errorf("lookup after the failed create: %v, want the entry rolled back", err)
	}
}

func TestClaimErrno(t *testing.T) {
	for err, want := range map[error]syscall.Errno{
		inode.ErrInodeFull:  syscall.ENOSPC,
		inode.ErrInodeInUse: syscall.EAGAIN,
		fmt.Errorf("failed to acquire inode lock: %w", lock.ErrLockTimeout): syscall.EIO,
		fmt.Errorf("failed to read inode slot: %w", syscall.EIO):            syscall.EIO,
	} {
		if got := claimErrno(err); got != want {
			t.Errorf("claimErrno(%v) = %v, want %v", err, got, want)
		}
	}
}
