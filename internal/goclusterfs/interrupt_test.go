package goclusterfs

import (
	"context"
	"testing"
	"time"

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
