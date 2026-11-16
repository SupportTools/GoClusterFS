package goclusterfs

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/inode"
)

func fileIn(t *testing.T, fsys *FS, parent uint32, name string) *File {
	t.Helper()
	ctx := context.Background()
	f := newRegularFile(t, fsys)
	if _, err := fsys.updateInode(ctx, f.inodeNum, func(ino *inode.Inode) error { ino.LinkCount = 1; return nil }); err != nil {
		t.Fatal(err)
	}
	parentIno, err := fsys.inodeTbl.Read(parent)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.dirMgr.Add(ctx, parentIno, name, uint64(f.inodeNum), dir.DT_REG); err != nil {
		t.Fatal(err)
	}
	return fileNode(fsys, f.inodeNum)
}

// named returns what name in directory parent names, or 0.
func named(t *testing.T, fsys *FS, parent uint32, name string) uint32 {
	t.Helper()
	parentIno, err := fsys.inodeTbl.Read(parent)
	if err != nil {
		t.Fatal(err)
	}
	child, _, err := fsys.dirMgr.Lookup(context.Background(), parentIno, name)
	if errors.Is(err, dir.ErrNotFound) {
		return 0
	}
	if err != nil {
		t.Fatalf("lookup %q in %d: %v", name, parent, err)
	}
	return uint32(child)
}

func links(t *testing.T, fsys *FS, num uint32) uint32 {
	t.Helper()
	return readInode(t, fsys, num).LinkCount
}

func TestRenameKeepsTheFileAndItsData(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	f := createFile(t, fsys, "a")
	writeAt(t, f, pattern(1, 3*4096), 0)
	if errno := fsys.Rename(context.Background(), "a", fsys, "b", 0); errno != 0 {
		t.Fatalf("rename: %v", errno)
	}
	if got := named(t, fsys, inode.InodeRoot, "a"); got != 0 {
		t.Errorf("a still names inode %d", got)
	}
	if got := named(t, fsys, inode.InodeRoot, "b"); got != f.inodeNum {
		t.Errorf("b names inode %d, want %d", got, f.inodeNum)
	}
	checkAt(t, fileNode(fsys, f.inodeNum), pattern(1, 3*4096), 0)
	expectNoProblems(t, fsys, "after the rename")
}

func TestRenameOverAFileFreesIt(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	a, b := createFile(t, fsys, "a"), createFile(t, fsys, "b")
	writeAt(t, a, pattern(1, 4096), 0)
	writeAt(t, b, pattern(2, 2*BlockSize), 0)
	blocks := []uint64{physicalBlock(t, fsys, b.inodeNum, 0), physicalBlock(t, fsys, b.inodeNum, 1)}

	if errno := fsys.Rename(context.Background(), "a", fsys, "b", 0); errno != 0 {
		t.Fatalf("rename: %v", errno)
	}
	if got := named(t, fsys, inode.InodeRoot, "b"); got != a.inodeNum {
		t.Errorf("b names inode %d, want %d", got, a.inodeNum)
	}
	if ino := readInode(t, fsys, b.inodeNum); ino.Magic != 0 {
		t.Errorf("the replaced file's inode is still in use: magic %#x", ino.Magic)
	}
	expectAllocated(t, fsys, blocks, false)
	expectNoProblems(t, fsys, "after the rename over a file")
}

// A rename over a file a peer has open leaves it to that peer until its last close, as unlink does.
func TestRenameOverAFileAPeerHasOpen(t *testing.T) {
	path := newFormattedImage(t)
	cluster := newFakeCluster(0, 1)
	nodeA := cluster.join(t, withBridge(openFormattedFS(t, path)), 0)
	nodeB := cluster.join(t, openFormattedFS(t, path), 1)
	ctx := context.Background()
	held := createOpen(t, nodeA, "b")
	writeAt(t, held, pattern(1, 4096), 0)
	a := createFile(t, nodeB, "a")

	if errno := nodeB.Rename(ctx, "a", nodeB, "b", 0); errno != 0 {
		t.Fatalf("node B renames a over b: %v", errno)
	}
	if got := named(t, nodeB, inode.InodeRoot, "b"); got != a.inodeNum {
		t.Errorf("b names inode %d, want %d", got, a.inodeNum)
	}
	if ino := readInode(t, nodeA, held.inodeNum); ino.Magic != inode.InodeMagic || ino.LinkCount != 0 || ino.Holders != 1 {
		t.Fatalf("replaced file: magic %#x, links %d, holders %#b; want an orphan node A holds", ino.Magic, ino.LinkCount, ino.Holders)
	}
	writeAt(t, held, pattern(2, 4096), 0)
	checkAt(t, held, pattern(2, 4096), 0)
	if errno := held.Release(ctx, nil); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
	if ino := readInode(t, nodeA, held.inodeNum); ino.Magic != 0 {
		t.Errorf("after node A's last close: magic %#x; want the slot free", ino.Magic)
	}
}

func TestRenameMovesADirectory(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	if err := fsys.InitRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	p1, p2 := newSubdir(t, fsys, "p1"), newSubdir(t, fsys, "p2")
	d := newDirIn(t, fsys, p1.inodeNum, "d")
	before1, before2 := links(t, fsys, p1.inodeNum), links(t, fsys, p2.inodeNum)

	if errno := p1.Rename(context.Background(), "d", p2, "e", 0); errno != 0 {
		t.Fatalf("rename: %v", errno)
	}
	if named(t, fsys, p1.inodeNum, "d") != 0 || named(t, fsys, p2.inodeNum, "e") != d.inodeNum {
		t.Errorf("after the move: d in p1 names %d, e in p2 names %d; want 0 and %d", named(t, fsys, p1.inodeNum, "d"), named(t, fsys, p2.inodeNum, "e"), d.inodeNum)
	}
	if got := named(t, fsys, d.inodeNum, ".."); got != p2.inodeNum {
		t.Errorf("\"..\" of the moved directory names %d, want %d", got, p2.inodeNum)
	}
	if got1, got2 := links(t, fsys, p1.inodeNum), links(t, fsys, p2.inodeNum); got1 != before1-1 || got2 != before2+1 {
		t.Errorf("link counts %d and %d, want %d and %d", got1, got2, before1-1, before2+1)
	}
	expectNoProblems(t, fsys, "after the directory moved")
}

func TestRenameErrors(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatal(err)
	}
	createFile(t, fsys, "file")
	createFile(t, fsys, "other")
	p := newSubdir(t, fsys, "p")
	newDirIn(t, fsys, p.inodeNum, "child")
	newSubdir(t, fsys, "empty")
	full := newSubdir(t, fsys, "full")
	fileIn(t, fsys, full.inodeNum, "x")

	for _, c := range []struct {
		from     string
		to       string
		into     *Dir
		flags    uint32
		want     syscall.Errno
		describe string
	}{
		{"file", "other", nil, 2, syscall.EINVAL, "RENAME_EXCHANGE"},
		{"file", "other", nil, renameNoReplace, syscall.EEXIST, "NOREPLACE over a file"},
		{"missing", "x", nil, 0, syscall.ENOENT, "a missing name"},
		{"file", "empty", nil, 0, syscall.EISDIR, "a file over a directory"},
		{"p", "file", nil, 0, syscall.ENOTDIR, "a directory over a file"},
		{"empty", "full", nil, 0, syscall.ENOTEMPTY, "a directory over a non-empty one"},
		{"p", "inside", &Dir{fs: fsys, inodeNum: named(t, fsys, p.inodeNum, "child")}, 0, syscall.EINVAL, "a directory into itself"},
	} {
		var into fs.InodeEmbedder = fsys
		if c.into != nil {
			into = c.into
		}
		if errno := fsys.Rename(ctx, c.from, into, c.to, c.flags); errno != c.want {
			t.Errorf("rename of %s: %v, want %v", c.describe, errno, c.want)
		}
	}
	expectNoProblems(t, fsys, "after the refused renames")
}

func TestRenameReplayFinishesARenameOverAFile(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	ctx := context.Background()
	a, b := createFile(t, fsys, "a"), createFile(t, fsys, "b")
	writeAt(t, b, pattern(2, BlockSize), 0)
	block := physicalBlock(t, fsys, b.inodeNum, 0)

	crashed := crashAfterCommit(t, fsys, "rename")
	go func() { _ = fsys.Rename(ctx, "a", fsys, "b", 0) }()
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	if named(t, remounted, inode.InodeRoot, "a") != 0 || named(t, remounted, inode.InodeRoot, "b") != a.inodeNum {
		t.Errorf("after replay: a names %d, b names %d; want 0 and %d", named(t, remounted, inode.InodeRoot, "a"), named(t, remounted, inode.InodeRoot, "b"), a.inodeNum)
	}
	if ino := readInode(t, remounted, b.inodeNum); ino.Magic != 0 {
		t.Errorf("the replaced file's inode after replay: magic %#x; want free", ino.Magic)
	}
	expectAllocated(t, remounted, []uint64{block}, false)
	expectCleanFsck(t, remounted)
}

func TestRenameReplayMovesADirectory(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	ctx := context.Background()
	p1, p2 := newSubdir(t, fsys, "p1"), newSubdir(t, fsys, "p2")
	d := newDirIn(t, fsys, p1.inodeNum, "d")
	before1, before2 := links(t, fsys, p1.inodeNum), links(t, fsys, p2.inodeNum)

	crashed := crashAfterCommit(t, fsys, "rename")
	go func() { _ = p1.Rename(ctx, "d", p2, "e", 0) }()
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	if named(t, remounted, p1.inodeNum, "d") != 0 || named(t, remounted, p2.inodeNum, "e") != d.inodeNum {
		t.Errorf("after replay: d in p1 names %d, e in p2 names %d", named(t, remounted, p1.inodeNum, "d"), named(t, remounted, p2.inodeNum, "e"))
	}
	if got := named(t, remounted, d.inodeNum, ".."); got != p2.inodeNum {
		t.Errorf("\"..\" after replay names %d, want %d", got, p2.inodeNum)
	}
	if got1, got2 := links(t, remounted, p1.inodeNum), links(t, remounted, p2.inodeNum); got1 != before1-1 || got2 != before2+1 {
		t.Errorf("link counts after replay %d and %d, want %d and %d", got1, got2, before1-1, before2+1)
	}
	expectCleanFsck(t, remounted)
}

// A crash after the names changed but before the target lost its link: replay drops the link once.
func TestRenameReplayDropsTheTargetAfterTheNamesChanged(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	ctx := context.Background()
	a, b := createFile(t, fsys, "a"), createFile(t, fsys, "b")

	crashed := crashAfterCommit(t, fsys, "rename: names changed")
	go func() { _ = fsys.Rename(ctx, "a", fsys, "b", 0) }()
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	if named(t, remounted, inode.InodeRoot, "b") != a.inodeNum || named(t, remounted, inode.InodeRoot, "a") != 0 {
		t.Errorf("after replay: a names %d, b names %d", named(t, remounted, inode.InodeRoot, "a"), named(t, remounted, inode.InodeRoot, "b"))
	}
	if ino := readInode(t, remounted, b.inodeNum); ino.Magic != 0 {
		t.Errorf("the replaced file's inode after replay: magic %#x, links %d; want free", ino.Magic, ino.LinkCount)
	}
	expectCleanFsck(t, remounted)
}

// crashAfterOvertaken runs overtake once the rename has changed its names and released its locks, then crashes with
// the rename's intent still open, as a crash before its completion record reaches the device would.
func crashAfterOvertaken(t *testing.T, fsys *FS, overtake func()) <-chan struct{} {
	t.Helper()
	crashed := make(chan struct{})
	fired := false
	afterCommit = func(got string) {
		if got != "rename: names changed" || fired {
			return
		}
		fired = true
		overtake()
		if _, err := fsys.journal.Checkpoint(); err != nil {
			t.Errorf("checkpoint: %v", err)
		}
		close(crashed)
		select {}
	}
	t.Cleanup(func() { afterCommit = nil })
	return crashed
}

// Replay must not redo a rename that finished and that later operations changed.
func TestRenameReplayLeavesAnOvertakenRenameAlone(t *testing.T) {
	for _, c := range []struct {
		describe string
		overtake func(ctx context.Context, fsys *FS) syscall.Errno
		want     map[string]bool // which root names name the file after replay
	}{
		{"renamed again", func(ctx context.Context, fsys *FS) syscall.Errno { return fsys.Rename(ctx, "b", fsys, "c", 0) }, map[string]bool{"c": true}},
		{"renamed back", func(ctx context.Context, fsys *FS) syscall.Errno { return fsys.Rename(ctx, "b", fsys, "a", 0) }, map[string]bool{"a": true}},
		{"unlinked", func(ctx context.Context, fsys *FS) syscall.Errno { return fsys.Unlink(ctx, "b") }, map[string]bool{}},
	} {
		t.Run(c.describe, func(t *testing.T) {
			fsys, path, offset, size := journaledFS(t)
			ctx := context.Background()
			a := createFile(t, fsys, "a")
			crashed := crashAfterOvertaken(t, fsys, func() {
				if errno := c.overtake(ctx, fsys); errno != 0 {
					t.Errorf("the overtaking operation: %v", errno)
				}
			})
			go func() { _ = fsys.Rename(ctx, "a", fsys, "b", 0) }()
			<-crashed

			remounted := remountAndReplay(t, path, offset, size)
			for _, name := range []string{"a", "b", "c"} {
				if got := named(t, remounted, inode.InodeRoot, name) == a.inodeNum; got != c.want[name] {
					t.Errorf("after replay, %q names the file: %v, want %v", name, got, c.want[name])
				}
			}
			expectCleanFsck(t, remounted)
		})
	}
}

func TestRenameReplayLeavesADirectoryMovedBackAlone(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	ctx := context.Background()
	p1, p2 := newSubdir(t, fsys, "p1"), newSubdir(t, fsys, "p2")
	d := newDirIn(t, fsys, p1.inodeNum, "d")
	before1, before2 := links(t, fsys, p1.inodeNum), links(t, fsys, p2.inodeNum)
	crashed := crashAfterOvertaken(t, fsys, func() {
		if errno := p2.Rename(ctx, "e", p1, "d", 0); errno != 0 {
			t.Errorf("move back: %v", errno)
		}
	})
	go func() { _ = p1.Rename(ctx, "d", p2, "e", 0) }()
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	if named(t, remounted, p1.inodeNum, "d") != d.inodeNum || named(t, remounted, p2.inodeNum, "e") != 0 {
		t.Errorf("after replay: d in p1 names %d, e in p2 names %d; want %d and 0", named(t, remounted, p1.inodeNum, "d"), named(t, remounted, p2.inodeNum, "e"), d.inodeNum)
	}
	if got := named(t, remounted, d.inodeNum, ".."); got != p1.inodeNum {
		t.Errorf("\"..\" after replay names %d, want %d", got, p1.inodeNum)
	}
	if got1, got2 := links(t, remounted, p1.inodeNum), links(t, remounted, p2.inodeNum); got1 != before1 || got2 != before2 {
		t.Errorf("link counts after replay %d and %d, want %d and %d", got1, got2, before1, before2)
	}
	expectCleanFsck(t, remounted)
}
