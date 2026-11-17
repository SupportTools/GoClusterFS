package goclusterfs

import (
	"context"
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
