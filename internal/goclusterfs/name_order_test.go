package goclusterfs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
)

func readRoot(t *testing.T, fsys *FS) *inode.Inode {
	t.Helper()
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// expectOneNamePerInode fails if two root entries name one inode, or an entry names a free slot.
func expectOneNamePerInode(t *testing.T, fsys *FS) map[string]uint64 {
	t.Helper()
	entries, err := fsys.dirMgr.List(context.Background(), readRoot(t, fsys))
	if err != nil {
		t.Fatal(err)
	}
	names, named := map[string]uint64{}, map[uint64]string{}
	for _, e := range entries {
		if e.Name == "." || e.Name == ".." {
			continue
		}
		if other, ok := named[e.InodeNum]; ok {
			t.Errorf("%q and %q both name inode %d", other, e.Name, e.InodeNum)
		}
		names[e.Name], named[e.InodeNum] = e.InodeNum, e.Name
		if ino, err := fsys.inodeTbl.Read(uint32(e.InodeNum)); err != nil || ino.Magic != inode.InodeMagic {
			t.Errorf("%q names inode %d, which is not in use (%v)", e.Name, e.InodeNum, err)
		}
	}
	return names
}

// A create or mkdir crashes between claiming and naming its inode; a peer's file created before replay keeps its own inode.
func TestCrashBetweenClaimAndNameLeavesNoSharedName(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		crashPoint string
		create     func(fsys *FS)
	}{
		{"create: inode claimed", func(fsys *FS) { fsys.Create(ctx, "crashed", 0, 0o644, &fuse.EntryOut{}) }},
		{"mkdir: inode claimed", func(fsys *FS) { fsys.Mkdir(ctx, "crashed", 0o755, &fuse.EntryOut{}) }},
	} {
		t.Run(tc.crashPoint, func(t *testing.T) {
			fsys, path, offset, size := journaledFS(t)
			crashed := crashAfterCommit(t, fsys, tc.crashPoint)
			go tc.create(fsys)
			<-crashed
			afterCommit = nil

			createFile(t, openFormattedFS(t, path), "peer.txt")

			remounted := remountAndReplay(t, path, offset, size)
			if _, ok := expectOneNamePerInode(t, remounted)["peer.txt"]; !ok {
				t.Error("the peer's file is gone after replay")
			}
			for _, p := range runFsck(t, remounted, fsck.DefaultOptions()).Problems {
				if p.Severity == fsck.Error {
					t.Errorf("fsck after replay: %s", p.String())
				}
			}
		})
	}
}

// A peer that changes an entry while RemoveIf checks it waits for the directory lock.
func TestRemoveIfHoldsDirectoryLockWhileChecking(t *testing.T) {
	ctx := context.Background()
	path := newFormattedImage(t)
	node, peer := openFormattedFS(t, path), openFormattedFS(t, path)
	victim := createFile(t, node, "victim")
	root := readRoot(t, node)

	peerDone := make(chan error, 1)
	removed, err := node.dirMgr.RemoveIf(ctx, root, "victim", func(child uint64) bool {
		go func() { peerDone <- peer.dirMgr.Remove(ctx, root, "victim") }()
		select {
		case err := <-peerDone:
			t.Fatalf("the peer removed the entry during the check: %v", err)
		case <-time.After(time.Second):
		}
		return child == uint64(victim.inodeNum)
	})
	if err != nil || !removed {
		t.Fatalf("RemoveIf = %v, %v; want the entry removed", removed, err)
	}
	if err := <-peerDone; !errors.Is(err, dir.ErrNotFound) {
		t.Fatalf("the peer's remove after RemoveIf: %v, want %v", err, dir.ErrNotFound)
	}
}

// Add and Remove with a stale copy of a directory another node removed leave the freed slot alone.
func TestDirChangesRefuseRemovedDirectory(t *testing.T) {
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	sub := newSubdir(t, fsys, "sub")
	stale, err := fsys.inodeTbl.Read(sub.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if errno := fsys.Rmdir(ctx, "sub"); errno != 0 {
		t.Fatalf("rmdir: %v", errno)
	}
	freed, err := fsys.inodeTbl.Read(sub.inodeNum)
	if err != nil {
		t.Fatal(err)
	}

	if err := fsys.dirMgr.Add(ctx, stale, "late", 99, dir.DT_REG); !errors.Is(err, dir.ErrRemoved) {
		t.Errorf("Add to the removed directory: %v, want %v", err, dir.ErrRemoved)
	}
	if err := fsys.dirMgr.Remove(ctx, stale, "late"); !errors.Is(err, dir.ErrRemoved) {
		t.Errorf("Remove from the removed directory: %v, want %v", err, dir.ErrRemoved)
	}

	after, err := fsys.inodeTbl.Read(sub.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if after.Magic != 0 || after.Size != freed.Size || after.AllocatedBlocks != freed.AllocatedBlocks || after.MetaGeneration != freed.MetaGeneration {
		t.Errorf("the freed slot changed: size %d, %d blocks, generation %d", after.Size, after.AllocatedBlocks, after.MetaGeneration)
	}
}
