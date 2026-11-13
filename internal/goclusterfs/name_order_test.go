package goclusterfs

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
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

// expectEmptyDir fails unless directory num has "." and ".." entries and the link count of an empty directory.
func expectEmptyDir(t *testing.T, fsys *FS, num uint64, parent uint32) {
	t.Helper()
	ctx := context.Background()
	ino, err := fsys.inodeTbl.Read(uint32(num))
	if err != nil {
		t.Fatal(err)
	}
	if !ino.IsDir() || ino.LinkCount != 2 {
		t.Errorf("inode %d: mode %#o, link count %d; want an empty directory", num, ino.Mode, ino.LinkCount)
	}
	if self, _, err := fsys.dirMgr.Lookup(ctx, ino, "."); err != nil || self != num {
		t.Errorf("%q in inode %d: %d, %v", ".", num, self, err)
	}
	if up, _, err := fsys.dirMgr.Lookup(ctx, ino, ".."); err != nil || up != uint64(parent) {
		t.Errorf("%q in inode %d: %d, %v; want %d", "..", num, up, err, parent)
	}
}

// A create or mkdir crashes between claiming and naming its inode, and a peer creates a file before replay. Replay
// names the claimed inode, and the peer's file keeps an inode of its own.
func TestCrashBetweenClaimAndNameReplaysTheName(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		crashPoint string
		create     func(fsys *FS)
		isDir      bool
	}{
		{"create: inode claimed", func(fsys *FS) { fsys.Create(ctx, "crashed", 0, 0o644, &fuse.EntryOut{}) }, false},
		{"mkdir: inode claimed", func(fsys *FS) { fsys.Mkdir(ctx, "crashed", 0o755, &fuse.EntryOut{}) }, true},
	} {
		t.Run(tc.crashPoint, func(t *testing.T) {
			fsys, path, offset, size := journaledFS(t)
			links := readRoot(t, fsys).LinkCount
			crashed := crashAfterCommit(t, fsys, tc.crashPoint)
			go tc.create(fsys)
			<-crashed
			afterCommit = nil

			createFile(t, openFormattedFS(t, path), "peer.txt")

			remounted := remountAndReplay(t, path, offset, size)
			names := expectOneNamePerInode(t, remounted)
			if _, ok := names["peer.txt"]; !ok {
				t.Error("the peer's file is gone after replay")
			}
			num, ok := names["crashed"]
			if !ok {
				t.Fatal("replay did not name the claimed inode")
			}
			if tc.isDir {
				expectEmptyDir(t, remounted, num, inode.InodeRoot)
				if got := readRoot(t, remounted).LinkCount; got != links+1 {
					t.Errorf("root link count %d after replay, want %d", got, links+1)
				}
			}
			expectCleanFsck(t, remounted)
		})
	}
}

// A mkdir stops after its commit and the process dies: replay makes the directory with "." and "..", and counts its
// ".." in the parent's link count.
func TestMkdirReplaysAfterCrash(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	links := readRoot(t, fsys).LinkCount
	crashed := crashAfterCommit(t, fsys, "mkdir")
	go fsys.Mkdir(context.Background(), "made", 0o755, &fuse.EntryOut{})
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	num, ok := expectOneNamePerInode(t, remounted)["made"]
	if !ok {
		t.Fatal("replay did not add the directory")
	}
	expectEmptyDir(t, remounted, num, inode.InodeRoot)
	if got := readRoot(t, remounted).LinkCount; got != links+1 {
		t.Errorf("root link count %d after replay, want %d", got, links+1)
	}
	expectCleanFsck(t, remounted)
}

// Node A looks up a name to remove it, and node B removes the name and makes a new one before A removes it. A must
// not remove or free B's new inode by what its lookup saw.
func TestRemoveRacesPeerRecreate(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		isDir  bool
		newDir bool
		reuse  bool
		want   syscall.Errno
	}{
		{"unlink, new file in another slot", false, false, false, syscall.ENOENT},
		{"unlink, new file in the same slot", false, false, true, 0},
		{"unlink, new directory in the same slot", false, true, true, syscall.EISDIR},
		{"rmdir, new directory in another slot", true, true, false, syscall.ENOENT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, path, _, _ := journaledFS(t)
			b := openFormattedFS(t, path)
			remove := func(fsys *FS, isDir bool) syscall.Errno {
				if isDir {
					return fsys.RootDir().Rmdir(ctx, "x")
				}
				return fsys.RootDir().Unlink(ctx, "x")
			}
			recreate := func() {
				if !tc.reuse {
					createFile(t, b, "filler")
				}
				if tc.newDir {
					newSubdir(t, b, "x")
				} else {
					writeAt(t, createFile(t, b, "x"), pattern(2, 2*BlockSize), 0)
				}
			}
			if tc.isDir {
				newSubdir(t, a, "x")
			} else {
				writeAt(t, createFile(t, a, "x"), pattern(1, BlockSize), 0)
			}

			afterCommit = func(op string) {
				if op != "unlink" {
					return
				}
				afterCommit = nil
				if errno := remove(b, tc.isDir); errno != 0 {
					t.Fatalf("B's remove: %v", errno)
				}
				recreate()
			}
			t.Cleanup(func() { afterCommit = nil })

			if errno := remove(a, tc.isDir); errno != tc.want {
				t.Errorf("A's remove = %v, want %v", errno, tc.want)
			}
			if _, kept := expectOneNamePerInode(t, b)["x"]; kept != (tc.want != 0) {
				t.Errorf("x exists after A's remove: %v", kept)
			}
			expectCleanFsck(t, b)
		})
	}
}

// A create or mkdir whose name is taken, or whose directory another node removed, reports EEXIST or ENOENT and gives
// back the inode it claimed.
func TestCreateNameErrors(t *testing.T) {
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	createFile(t, fsys, "taken")
	gone := newSubdir(t, fsys, "gone")
	if errno := fsys.Rmdir(ctx, "gone"); errno != 0 {
		t.Fatalf("rmdir: %v", errno)
	}
	for _, tc := range []struct {
		parent uint32
		name   string
		want   syscall.Errno
	}{
		{inode.InodeRoot, "taken", syscall.EEXIST},
		{gone.inodeNum, "late", syscall.ENOENT},
	} {
		for _, mkdir := range []bool{false, true} {
			slot, err := fsys.inodeTbl.FindFreeSlot()
			if err != nil {
				t.Fatal(err)
			}
			var errno syscall.Errno
			if mkdir {
				_, errno = fsys.makeDirOnce(ctx, &fsys.Inode, tc.parent, tc.name, 0o755, &fuse.EntryOut{})
			} else {
				_, errno = fsys.createFileOnce(ctx, &fsys.Inode, tc.parent, tc.name, 0o644, &fuse.EntryOut{})
			}
			if errno != tc.want {
				t.Errorf("mkdir %v, %q in inode %d: %v, want %v", mkdir, tc.name, tc.parent, errno, tc.want)
			}
			if ino, err := fsys.inodeTbl.Read(slot); err != nil || ino.Magic != 0 {
				t.Errorf("mkdir %v, %q: slot %d still in use after the failure (%v)", mkdir, tc.name, slot, err)
			}
		}
	}
	expectCleanFsck(t, fsys)
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
