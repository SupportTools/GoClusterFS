package inode

import (
	"context"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/lock"
)

// A directory listing holds the directory's shared lock and then reads the inode
// table, while Write holds the table mutex and asks for the exclusive lock.
func TestWriteDoesNotHangBehindSameNodeReader(t *testing.T) {
	dev, cleanup := createTestDevice(t, 16<<20)
	defer cleanup()
	var uuid [16]byte
	copy(uuid[:], "node-a")
	lm := lock.NewManager(dev, 8<<20, 1<<20, uuid, 1)
	table := NewTable(dev, 0, 4<<20, lm, uuid)
	ctx := context.Background()
	dir, err := table.Alloc(ctx)
	if err != nil {
		t.Fatalf("Alloc: %v", err)
	}
	key := uint64(dir.InodeNum)

	if err := lm.Acquire(ctx, lock.LockTypeInode, key, lock.LockModeShared); err != nil {
		t.Fatalf("Acquire shared: %v", err)
	}
	writeDone := make(chan error, 1)
	go func() {
		wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		writeDone <- table.Write(wctx, dir)
	}()
	time.Sleep(50 * time.Millisecond)

	readDone := make(chan error, 1)
	go func() {
		if err := lm.Acquire(ctx, lock.LockTypeInode, key, lock.LockModeShared); err != nil {
			readDone <- err
			return
		}
		_, err := table.Read(dir.InodeNum)
		lm.Release(lock.LockTypeInode, key)
		readDone <- err
	}()
	lm.Release(lock.LockTypeInode, key)

	for name, done := range map[string]chan error{"Write": writeDone, "listing": readDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not finish: same-node reader and writer deadlocked", name)
		}
	}
}
