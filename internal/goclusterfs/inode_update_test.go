package goclusterfs

import (
	"context"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/inode"
)

// TestUpdateInodeLockedWaitsForOtherNode runs another node's link count update
// between this node's read and write of the root directory inode. Neither
// update may be lost.
func TestUpdateInodeLockedWaitsForOtherNode(t *testing.T) {
	path := newFormattedImage(t)
	nodeA, nodeB := openFormattedFS(t, path), openFormattedFS(t, path)
	ctx := context.Background()
	increment := func(ino *inode.Inode) error { ino.LinkCount++; return nil }

	before, err := nodeA.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}

	otherDone := make(chan error, 1)
	_, err = nodeA.updateInodeLocked(ctx, inode.InodeRoot, func(ino *inode.Inode) error {
		go func() {
			_, err := nodeB.updateInodeLocked(ctx, inode.InodeRoot, increment)
			otherDone <- err
		}()
		// Without the lock, node B reads and writes the inode in this window.
		time.Sleep(200 * time.Millisecond)
		return increment(ino)
	})
	if err != nil {
		t.Fatalf("node A update: %v", err)
	}
	if err := <-otherDone; err != nil {
		t.Fatalf("node B update: %v", err)
	}

	after, err := nodeA.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if after.LinkCount != before.LinkCount+2 {
		t.Errorf("root link count %d after two increments from %d, want %d",
			after.LinkCount, before.LinkCount, before.LinkCount+2)
	}
}
