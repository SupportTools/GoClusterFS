package goclusterfs

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
)

// fsck reports each way the directory tree can break; a tree mkdir and rename built has none of them.
func TestFsckReportsDirectoryTreeProblems(t *testing.T) {
	for _, c := range []struct {
		describe string
		corrupt  func(t *testing.T, fsys *FS, p1, p2, d *Dir)
		want     string
	}{
		{"a second name", func(t *testing.T, fsys *FS, _, p2, d *Dir) {
			if err := fsys.dirMgr.Add(context.Background(), readInode(t, fsys, p2.inodeNum), "again", uint64(d.inodeNum), dir.DT_DIR); err != nil {
				t.Fatal(err)
			}
		}, "directory has 2 names"},
		{"a \"..\" naming another parent", func(t *testing.T, fsys *FS, _, p2, d *Dir) {
			pointDots(t, fsys, d.inodeNum, p2.inodeNum)
		}, `".." names inode`},
		{"a wrong link count", func(t *testing.T, fsys *FS, p1, _, _ *Dir) {
			if _, err := fsys.updateInode(context.Background(), p1.inodeNum, func(ino *inode.Inode) error { ino.LinkCount = 7; return nil }); err != nil {
				t.Fatal(err)
			}
		}, "directory has 7 links, want 3"},
		{"a loop", func(t *testing.T, fsys *FS, p1, _, d *Dir) {
			if err := fsys.dirMgr.Remove(context.Background(), readInode(t, fsys, inode.InodeRoot), "p1"); err != nil {
				t.Fatal(err)
			}
			pointDots(t, fsys, p1.inodeNum, d.inodeNum)
		}, "never reaches the root"},
	} {
		t.Run(c.describe, func(t *testing.T) {
			fsys := openFormattedFS(t, newFormattedImage(t))
			if err := fsys.InitRoot(context.Background()); err != nil {
				t.Fatal(err)
			}
			p1, p2 := newSubdir(t, fsys, "p1"), newSubdir(t, fsys, "p2")
			d := newDirIn(t, fsys, p1.inodeNum, "d")
			expectNoProblems(t, fsys, "before the corruption")

			c.corrupt(t, fsys, p1, p2, d)
			r := runFsck(t, fsys, fsck.Options{})
			found := false
			for _, p := range r.Problems {
				found = found || strings.Contains(p.Message, c.want)
			}
			if !found {
				t.Errorf("fsck after %s found no %q:%s", c.describe, c.want, problemList(r))
			}
		})
	}
}

func pointDots(t *testing.T, fsys *FS, num, parent uint32) {
	t.Helper()
	if _, err := fsys.dirMgr.PointIf(context.Background(), readInode(t, fsys, num), "..", uint64(parent), dir.DT_DIR, func(uint64) bool { return true }); err != nil {
		t.Fatal(err)
	}
}

// A directory moved by rename keeps its old parent in "..", and its new parent's "." names another directory. fsck
// reports both with the inode numbers, a dry run changes nothing, and repair points them at the right directories.
func TestFsckRepairsDotEntriesAfterRename(t *testing.T) {
	path := newFormattedImage(t)
	fsys := openFormattedFS(t, path)
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatal(err)
	}
	p1, p2 := newSubdir(t, fsys, "p1"), newSubdir(t, fsys, "p2")
	d := newDirIn(t, fsys, p1.inodeNum, "d")
	if errno := p1.Rename(ctx, "d", p2, "e", 0); errno != 0 {
		t.Fatalf("rename: %v", errno)
	}
	expectNoProblems(t, fsys, "after the rename")

	pointDots(t, fsys, d.inodeNum, p1.inodeNum)
	if _, err := fsys.dirMgr.PointIf(ctx, readInode(t, fsys, p2.inodeNum), ".", uint64(p1.inodeNum), dir.DT_DIR, func(uint64) bool { return true }); err != nil {
		t.Fatal(err)
	}
	dotdot := fmt.Sprintf("inode %d: \"..\" names inode %d, but directory %d names it", d.inodeNum, p1.inodeNum, p2.inodeNum)
	dot := fmt.Sprintf("inode %d: \".\" names inode %d, not directory %d itself", p2.inodeNum, p1.inodeNum, p2.inodeNum)

	dry := repairOpts()
	dry.DryRun = true
	r := runFsck(t, fsys, dry)
	expectProblem(t, r, dotdot+" [WOULD FIX]")
	expectProblem(t, r, dot+" [WOULD FIX]")
	if got := named(t, openFormattedFS(t, path), d.inodeNum, ".."); got != p1.inodeNum {
		t.Fatalf("dry run changed \"..\" to %d", got)
	}

	r = runFsck(t, fsys, repairOpts())
	expectProblem(t, r, dotdot+" [FIXED]")
	expectProblem(t, r, dot+" [FIXED]")
	reopened := openFormattedFS(t, path)
	if got := named(t, reopened, d.inodeNum, ".."); got != p2.inodeNum {
		t.Errorf("\"..\" of the moved directory names %d after repair, want %d", got, p2.inodeNum)
	}
	if got := named(t, reopened, p2.inodeNum, "."); got != p2.inodeNum {
		t.Errorf("\".\" of the new parent names %d after repair, want %d", got, p2.inodeNum)
	}
	expectNoProblems(t, reopened, "after repair")
}
