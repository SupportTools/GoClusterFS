package goclusterfs

import (
	"context"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/inode"
)

// expectOneName fails unless exactly one of the two names names child: a rename either happened or did not.
func expectOneName(t *testing.T, fsys *FS, oldParent uint32, oldName string, newParent uint32, newName string, child uint32) (moved bool) {
	t.Helper()
	atOld, atNew := named(t, fsys, oldParent, oldName) == child, named(t, fsys, newParent, newName) == child
	if atOld == atNew {
		t.Fatalf("inode %d: named by the old name %v, by the new name %v; want exactly one", child, atOld, atNew)
	}
	return atNew
}

// expectDirLinks checks that a directory counts 2 links plus one per subdirectory.
func expectDirLinks(t *testing.T, fsys *FS, num uint32) {
	t.Helper()
	entries, err := fsys.dirMgr.List(context.Background(), readInode(t, fsys, num))
	if err != nil {
		t.Fatal(err)
	}
	want := uint32(2)
	for _, e := range entries {
		if e.FileType == dir.DT_DIR && e.Name != "." && e.Name != ".." {
			want++
		}
	}
	if got := links(t, fsys, num); got != want {
		t.Errorf("directory %d counts %d links, want %d", num, got, want)
	}
}

func TestPowerCutDuringRenameInOneDirectory(t *testing.T) {
	data := pattern(4, 4096)
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			f = createFile(t, fsys, "a")
			writeAt(t, f, data, 0)
		},
		op: func(fsys *FS, _ *power) { _ = fsys.Rename(context.Background(), "a", fsys, "b", 0) },
		check: func(t *testing.T, fsys *FS) {
			expectOneName(t, fsys, inode.InodeRoot, "a", inode.InodeRoot, "b", f.inodeNum)
			checkAt(t, fileNode(fsys, f.inodeNum), data, 0)
		},
	})
}

func TestPowerCutDuringRenameBetweenDirectories(t *testing.T) {
	var p1, p2 *Dir
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			p1, p2 = newSubdir(t, fsys, "p1"), newSubdir(t, fsys, "p2")
			f = fileIn(t, fsys, p1.inodeNum, "a")
		},
		op: func(fsys *FS, _ *power) { _ = p1.Rename(context.Background(), "a", p2, "b", 0) },
		check: func(t *testing.T, fsys *FS) {
			expectOneName(t, fsys, p1.inodeNum, "a", p2.inodeNum, "b", f.inodeNum)
		},
	})
}

func TestPowerCutDuringRenameOverAFile(t *testing.T) {
	var a, b *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			a, b = createFile(t, fsys, "a"), createFile(t, fsys, "b")
			writeAt(t, b, pattern(2, BlockSize), 0)
		},
		op: func(fsys *FS, _ *power) { _ = fsys.Rename(context.Background(), "a", fsys, "b", 0) },
		check: func(t *testing.T, fsys *FS) {
			if !expectOneName(t, fsys, inode.InodeRoot, "a", inode.InodeRoot, "b", a.inodeNum) {
				if got := named(t, fsys, inode.InodeRoot, "b"); got != b.inodeNum {
					t.Errorf("the rename did not happen, but b names inode %d, not %d", got, b.inodeNum)
				}
			}
		},
	})
}

func TestPowerCutDuringDirectoryMove(t *testing.T) {
	var p1, p2, d *Dir
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			p1, p2 = newSubdir(t, fsys, "p1"), newSubdir(t, fsys, "p2")
			d = newDirIn(t, fsys, p1.inodeNum, "d")
		},
		op: func(fsys *FS, _ *power) { _ = p1.Rename(context.Background(), "d", p2, "e", 0) },
		check: func(t *testing.T, fsys *FS) {
			parent := p1.inodeNum
			if expectOneName(t, fsys, p1.inodeNum, "d", p2.inodeNum, "e", d.inodeNum) {
				parent = p2.inodeNum
			}
			if got := named(t, fsys, d.inodeNum, ".."); got != parent {
				t.Errorf("\"..\" of the moved directory names %d, want %d", got, parent)
			}
			expectDirLinks(t, fsys, p1.inodeNum)
			expectDirLinks(t, fsys, p2.inodeNum)
		},
	})
}

func TestPowerCutDuringRenameOverAnEmptyDirectory(t *testing.T) {
	var p1, p2, d, e *Dir
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			p1, p2 = newSubdir(t, fsys, "p1"), newSubdir(t, fsys, "p2")
			d, e = newDirIn(t, fsys, p1.inodeNum, "d"), newDirIn(t, fsys, p2.inodeNum, "e")
		},
		op: func(fsys *FS, _ *power) { _ = p1.Rename(context.Background(), "d", p2, "e", 0) },
		check: func(t *testing.T, fsys *FS) {
			if !expectOneName(t, fsys, p1.inodeNum, "d", p2.inodeNum, "e", d.inodeNum) {
				if got := named(t, fsys, p2.inodeNum, "e"); got != e.inodeNum {
					t.Errorf("the rename did not happen, but e names inode %d, not %d", got, e.inodeNum)
				}
			}
			expectDirLinks(t, fsys, p1.inodeNum)
			expectDirLinks(t, fsys, p2.inodeNum)
		},
	})
}
