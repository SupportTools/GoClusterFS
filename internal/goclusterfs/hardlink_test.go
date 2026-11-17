package goclusterfs

import (
	"context"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
)

func linkCount(t *testing.T, fsys *FS, num uint32) uint32 {
	t.Helper()
	ino, err := fsys.inodeTbl.Read(num)
	if err != nil {
		t.Fatal(err)
	}
	return ino.LinkCount
}

func mustLink(t *testing.T, fsys *FS, parent, num uint32, name string) {
	t.Helper()
	if _, errno := fsys.addLink(context.Background(), parent, num, name); errno != 0 {
		t.Fatalf("link %q to inode %d: %v", name, num, errno)
	}
}

// Each link adds one to the count and each unlink takes one away; the data stays readable through every name left,
// and the file is freed with its last name.
func TestLinkCountsNames(t *testing.T) {
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	sub := newSubdir(t, fsys, "sub")
	a := createFile(t, fsys, "a")
	writeAt(t, a, pattern(1, BlockSize+100), 0)

	mustLink(t, fsys, inode.InodeRoot, a.inodeNum, "b")
	mustLink(t, fsys, sub.inodeNum, a.inodeNum, "c")
	if got := linkCount(t, fsys, a.inodeNum); got != 3 {
		t.Fatalf("link count after two links: %d, want 3", got)
	}
	if b := lookupFile(t, fsys, "b"); b.inodeNum != a.inodeNum {
		t.Fatalf("b names inode %d, want %d", b.inodeNum, a.inodeNum)
	}

	if errno := fsys.RootDir().Unlink(ctx, "a"); errno != 0 {
		t.Fatalf("unlink a: %v", errno)
	}
	if got := linkCount(t, fsys, a.inodeNum); got != 2 {
		t.Fatalf("link count after one unlink: %d, want 2", got)
	}
	checkFile(t, lookupFile(t, fsys, "b"), pattern(1, BlockSize+100))
	expectCleanFsck(t, fsys)

	if errno := fsys.RootDir().Unlink(ctx, "b"); errno != 0 {
		t.Fatalf("unlink b: %v", errno)
	}
	if errno := sub.Unlink(ctx, "c"); errno != 0 {
		t.Fatalf("unlink c: %v", errno)
	}
	if ino, err := fsys.inodeTbl.Read(a.inodeNum); err != nil || ino.Magic != 0 {
		t.Errorf("inode %d after its last name went: magic %#x, %v; want it freed", a.inodeNum, ino.Magic, err)
	}
	expectCleanFsck(t, fsys)
}

func TestLinkRefuses(t *testing.T) {
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	sub := newSubdir(t, fsys, "sub")
	a := createFile(t, fsys, "a")
	createFile(t, fsys, "taken")

	if _, errno := fsys.link(ctx, &fsys.Inode, inode.InodeRoot, sub, "dirlink", &fuse.EntryOut{}); errno != syscall.EPERM {
		t.Errorf("link to a directory: %v, want EPERM", errno)
	}
	if _, errno := fsys.addLink(ctx, inode.InodeRoot, a.inodeNum, "taken"); errno != syscall.EEXIST {
		t.Errorf("link over a taken name: %v, want EEXIST", errno)
	}
	setLinks := func(n uint32) {
		t.Helper()
		if _, err := fsys.updateInode(ctx, a.inodeNum, func(ino *inode.Inode) error { ino.LinkCount = n; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	setLinks(maxLinks)
	if _, errno := fsys.addLink(ctx, inode.InodeRoot, a.inodeNum, "many"); errno != syscall.EMLINK {
		t.Errorf("link at %d links: %v, want EMLINK", maxLinks, errno)
	}
	setLinks(0)
	if _, errno := fsys.addLink(ctx, inode.InodeRoot, a.inodeNum, "orphan"); errno != syscall.ENOENT {
		t.Errorf("link to a file with no link left: %v, want ENOENT", errno)
	}
	if got := linkCount(t, fsys, a.inodeNum); got != 0 {
		t.Errorf("a refused link changed the count to %d", got)
	}
}

// A link crashes at each step and its journal is replayed: the file ends with both names and two links.
func TestLinkReplaysAfterCrash(t *testing.T) {
	for _, crashPoint := range []string{"link", "link: counted"} {
		t.Run(strings.ReplaceAll(crashPoint, ": ", "_"), func(t *testing.T) {
			fsys, path, offset, size := journaledFS(t)
			a := createFile(t, fsys, "a")
			writeAt(t, a, pattern(2, 300), 0)
			crashed := crashAfterCommit(t, fsys, crashPoint)
			go func() { _, _ = fsys.addLink(context.Background(), inode.InodeRoot, a.inodeNum, "b") }()
			<-crashed

			after := remountAndReplay(t, path, offset, size)
			checkFile(t, lookupFile(t, after, "b"), pattern(2, 300))
			if got := linkCount(t, after, a.inodeNum); got != 2 {
				t.Errorf("link count after replay: %d, want 2", got)
			}
			expectCleanFsck(t, after)
		})
	}
}

// A link crashes after its increment, and another write changes the file before replay. Replay can't tell the
// increment was the link's, so it leaves an extra link and no new name, which fsck reports and repair takes back.
func TestLinkReplaySkipsChangedFile(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	a := createFile(t, fsys, "a")
	crashed := crashAfterCommit(t, fsys, "link: counted")
	go func() { _, _ = fsys.addLink(context.Background(), inode.InodeRoot, a.inodeNum, "b") }()
	<-crashed
	afterCommit = nil
	writeAt(t, a, pattern(3, 100), 0)

	after := remountAndReplay(t, path, offset, size)
	root, err := after.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := after.dirMgr.Lookup(context.Background(), root, "b"); err != dir.ErrNotFound {
		t.Errorf("lookup b after replay: %v, want it not added", err)
	}
	r := runFsck(t, after, fsck.DefaultOptions())
	if !strings.Contains(problemList(r), "has 2 links but 1 names") {
		t.Errorf("fsck should report the extra link:%s", problemList(r))
	}
	expectRepairedClean(t, after)
	if got := linkCount(t, after, a.inodeNum); got != 1 {
		t.Errorf("link count after repair: %d, want 1", got)
	}
}

// A name without its link is the dangerous mismatch: unlinking the other name would free the file under it.
func TestFsckRepairsTooFewLinks(t *testing.T) {
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	a := createFile(t, fsys, "a")
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.dirMgr.Add(ctx, root, "uncounted", uint64(a.inodeNum), dir.DT_REG); err != nil {
		t.Fatal(err)
	}

	r := runFsck(t, fsys, fsck.DefaultOptions())
	if !strings.Contains(problemList(r), "has 1 links but 2 names: unlinking can free it while a name remains") {
		t.Errorf("fsck should report the missing link:%s", problemList(r))
	}
	expectRepairedClean(t, fsys)
	if got := linkCount(t, fsys, a.inodeNum); got != 2 {
		t.Errorf("link count after repair: %d, want 2", got)
	}
}

// Power fails during a link: the file keeps its data under its first name, the new name is either gone or reads the
// same file, and fsck finds at most an extra link.
func TestPowerCutDuringLink(t *testing.T) {
	data := pattern(4, BlockSize+10)
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			f = createFile(t, fsys, "f")
			writeAt(t, f, data, 0)
		},
		op: func(fsys *FS, _ *power) {
			_, _ = fsys.addLink(context.Background(), inode.InodeRoot, f.inodeNum, "g")
		},
		check: func(t *testing.T, fsys *FS) {
			checkFile(t, lookupFile(t, fsys, "f"), data)
			root, err := fsys.inodeTbl.Read(inode.InodeRoot)
			if err != nil {
				t.Fatal(err)
			}
			if num, _, err := fsys.dirMgr.Lookup(context.Background(), root, "g"); err == nil {
				if uint32(num) != f.inodeNum {
					t.Fatalf("g names inode %d, want %d", num, f.inodeNum)
				}
				checkFile(t, lookupFile(t, fsys, "g"), data)
			}
		},
	})
}

// Power fails while one name of a linked file is removed: the other name keeps the file and its data.
func TestPowerCutDuringUnlinkOfLinkedFile(t *testing.T) {
	data := pattern(5, 2*BlockSize)
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			f = createFile(t, fsys, "f")
			writeAt(t, f, data, 0)
			mustLink(t, fsys, inode.InodeRoot, f.inodeNum, "g")
		},
		op: func(fsys *FS, _ *power) {
			_ = fsys.RootDir().Unlink(context.Background(), "f")
		},
		check: func(t *testing.T, fsys *FS) {
			checkFile(t, lookupFile(t, fsys, "g"), data)
		},
	})
}
