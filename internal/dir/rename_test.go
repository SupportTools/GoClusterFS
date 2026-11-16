package dir

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/inode"
)

func newNode(t *testing.T, m *Manager, mode uint16) *inode.Inode {
	t.Helper()
	ctx := context.Background()
	num, err := m.inodeTbl.FindFreeSlot()
	if err != nil {
		t.Fatal(err)
	}
	ino, err := m.inodeTbl.ClaimSlot(ctx, num)
	if err != nil {
		t.Fatal(err)
	}
	ino.Mode = mode
	ino.LinkCount = 1
	if err := m.inodeTbl.Write(ctx, ino); err != nil {
		t.Fatal(err)
	}
	return ino
}

func newFileIn(t *testing.T, m *Manager, parent *inode.Inode, name string) *inode.Inode {
	t.Helper()
	ino := newNode(t, m, inode.S_IFREG|0o644)
	if err := m.Add(context.Background(), parent, name, uint64(ino.InodeNum), DT_REG); err != nil {
		t.Fatalf("add %s: %v", name, err)
	}
	return ino
}

// newDirIn makes a directory whose ".." names parent, and names it in parent.
func newDirIn(t *testing.T, m *Manager, parent *inode.Inode, name string) *inode.Inode {
	t.Helper()
	ctx := context.Background()
	num, err := m.inodeTbl.FindFreeSlot()
	if err != nil {
		t.Fatal(err)
	}
	ino, err := m.inodeTbl.ClaimSlot(ctx, num)
	if err != nil {
		t.Fatal(err)
	}
	ino.Mode, ino.LinkCount = inode.S_IFDIR|0o755, 2
	if err := m.InitDirectory(ctx, ino, uint64(parent.InodeNum)); err != nil {
		t.Fatal(err)
	}
	if err := m.Add(ctx, parent, name, uint64(ino.InodeNum), DT_DIR); err != nil {
		t.Fatalf("add %s: %v", name, err)
	}
	return ino
}

// renameOp builds the op a caller would after looking both names up.
func renameOp(t *testing.T, m *Manager, oldParent *inode.Inode, oldName string, newParent *inode.Inode, newName string) *RenameOp {
	t.Helper()
	ctx := context.Background()
	child, ctype, err := m.Lookup(ctx, oldParent, oldName)
	if err != nil {
		t.Fatalf("lookup %s: %v", oldName, err)
	}
	op := &RenameOp{OldParent: oldParent.InodeNum, NewParent: newParent.InodeNum, OldName: oldName, NewName: newName,
		Child: child, ChildIsDir: ctype == DT_DIR, Commit: func(*Renamed) error { return nil }}
	if target, ttype, err := m.Lookup(ctx, newParent, newName); err == nil {
		op.Target, op.TargetIsDir = target, ttype == DT_DIR
	}
	return op
}

func reread(t *testing.T, m *Manager, ino *inode.Inode) *inode.Inode {
	t.Helper()
	fresh, err := m.inodeTbl.Read(ino.InodeNum)
	if err != nil {
		t.Fatal(err)
	}
	return fresh
}

func expectName(t *testing.T, m *Manager, parent *inode.Inode, name string, want uint64) {
	t.Helper()
	got, err := lookupChild(m, reread(t, m, parent), name)
	switch {
	case want == 0 && !errors.Is(err, ErrNotFound):
		t.Errorf("%q in directory %d: %d, %v; want no such name", name, parent.InodeNum, got, err)
	case want != 0 && (err != nil || got != want):
		t.Errorf("%q in directory %d: %d, %v; want inode %d", name, parent.InodeNum, got, err, want)
	}
}

func TestRenameInOneDirectory(t *testing.T) {
	nodes, dirIno := newNodes(t, 1)
	m := nodes[0]
	f := newFileIn(t, m, dirIno, "a")
	before := diskGen(t, m, dirIno.InodeNum)

	var committed []*Renamed
	op := renameOp(t, m, dirIno, "a", dirIno, "b")
	op.Commit = func(r *Renamed) error { committed = append(committed, r); return nil }
	r, err := m.Rename(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	if len(committed) != 1 || r.Target != nil || r.MovesDir || r.OldParent.MetaGeneration <= before {
		t.Errorf("committed %d times, target %v, moves dir %v, parent generation %d (was %d)", len(committed), r.Target, r.MovesDir, r.OldParent.MetaGeneration, before)
	}
	expectName(t, m, dirIno, "a", 0)
	expectName(t, m, dirIno, "b", uint64(f.InodeNum))
}

func TestRenameReplacesATarget(t *testing.T) {
	nodes, dirIno := newNodes(t, 1)
	m := nodes[0]
	a, b := newFileIn(t, m, dirIno, "a"), newFileIn(t, m, dirIno, "b")

	r, err := m.Rename(context.Background(), renameOp(t, m, dirIno, "a", dirIno, "b"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Target == nil || r.Target.InodeNum != b.InodeNum || r.TargetType != DT_REG {
		t.Errorf("replaced %+v, want inode %d", r.Target, b.InodeNum)
	}
	expectName(t, m, dirIno, "a", 0)
	expectName(t, m, dirIno, "b", uint64(a.InodeNum))

	newFileIn(t, m, dirIno, "c")
	noReplace := renameOp(t, m, dirIno, "b", dirIno, "c")
	noReplace.NoReplace = true
	noReplace.Commit = func(*Renamed) error { t.Error("committed a rename NOREPLACE refused"); return nil }
	if _, err := m.Rename(context.Background(), noReplace); !errors.Is(err, ErrExists) {
		t.Errorf("rename over c with NOREPLACE: %v, want ErrExists", err)
	}
	expectName(t, m, dirIno, "b", uint64(a.InodeNum))
}

func TestRenameMovesADirectoryBetweenParents(t *testing.T) {
	nodes, root := newNodes(t, 1)
	m := nodes[0]
	p1, p2 := newDirIn(t, m, root, "p1"), newDirIn(t, m, root, "p2")
	d := newDirIn(t, m, p1, "d")
	links1, links2 := reread(t, m, p1).LinkCount, reread(t, m, p2).LinkCount

	r, err := m.Rename(context.Background(), renameOp(t, m, reread(t, m, p1), "d", reread(t, m, p2), "e"))
	if err != nil {
		t.Fatal(err)
	}
	if !r.MovesDir {
		t.Error("a move between parents did not count as moving a directory")
	}
	expectName(t, m, p1, "d", 0)
	expectName(t, m, p2, "e", uint64(d.InodeNum))
	if parent, err := m.parentOf(d.InodeNum); err != nil || parent != p2.InodeNum {
		t.Errorf("\"..\" of the moved directory: %d, %v; want %d", parent, err, p2.InodeNum)
	}
	expectName(t, m, d, "..", uint64(p2.InodeNum))
	if got1, got2 := reread(t, m, p1).LinkCount, reread(t, m, p2).LinkCount; got1 != links1-1 || got2 != links2+1 {
		t.Errorf("link counts %d and %d, want %d and %d", got1, got2, links1-1, links2+1)
	}
}

func TestRenameRefusesAMoveIntoItself(t *testing.T) {
	nodes, root := newNodes(t, 1)
	m := nodes[0]
	p := newDirIn(t, m, root, "p")
	child := newDirIn(t, m, p, "child")
	grandchild := newDirIn(t, m, child, "grandchild")

	for _, into := range []*inode.Inode{child, grandchild} {
		op := renameOp(t, m, reread(t, m, root), "p", reread(t, m, into), "moved")
		op.Commit = func(*Renamed) error { t.Error("committed a move into the directory's own subtree"); return nil }
		if _, err := m.Rename(context.Background(), op); !errors.Is(err, ErrIntoSelf) {
			t.Errorf("move p into directory %d: %v, want ErrIntoSelf", into.InodeNum, err)
		}
	}
	expectName(t, m, root, "p", uint64(p.InodeNum))
}

func TestRenameChecksTargetTypes(t *testing.T) {
	nodes, root := newNodes(t, 1)
	m := nodes[0]
	file := newFileIn(t, m, root, "file")
	newDirIn(t, m, root, "dir")
	full := newDirIn(t, m, root, "full")
	newFileIn(t, m, full, "x")
	empty := newDirIn(t, m, root, "empty")

	for _, c := range []struct {
		from, to string
		want     error
	}{
		{"dir", "file", ErrNotDirectory},
		{"file", "dir", ErrIsDir},
		{"dir", "full", ErrNotEmpty},
	} {
		op := renameOp(t, m, reread(t, m, root), c.from, reread(t, m, root), c.to)
		if _, err := m.Rename(context.Background(), op); !errors.Is(err, c.want) {
			t.Errorf("rename %s over %s: %v, want %v", c.from, c.to, err, c.want)
		}
	}
	expectName(t, m, root, "file", uint64(file.InodeNum))

	links := reread(t, m, root).LinkCount
	r, err := m.Rename(context.Background(), renameOp(t, m, reread(t, m, root), "dir", reread(t, m, root), "empty"))
	if err != nil {
		t.Fatalf("rename dir over an empty directory: %v", err)
	}
	if r.Target.InodeNum != empty.InodeNum || reread(t, m, root).LinkCount != links-1 {
		t.Errorf("replaced inode %d, parent links %d; want inode %d, %d links", r.Target.InodeNum, reread(t, m, root).LinkCount, empty.InodeNum, links-1)
	}
}

func TestRenameAfterAStaleLookupChangesNothing(t *testing.T) {
	nodes, dirIno := newNodes(t, 1)
	m := nodes[0]
	f := newFileIn(t, m, dirIno, "a")
	op := renameOp(t, m, dirIno, "a", dirIno, "b")
	newFileIn(t, m, dirIno, "b")
	if _, err := m.Rename(context.Background(), op); !errors.Is(err, ErrRaced) {
		t.Errorf("rename after b appeared: %v, want ErrRaced", err)
	}

	op = renameOp(t, m, dirIno, "a", dirIno, "c")
	op.Commit = func(*Renamed) error { return errors.New("journal full") }
	if _, err := m.Rename(context.Background(), op); err == nil || !strings.Contains(err.Error(), "journal full") {
		t.Errorf("rename with a failing commit: %v", err)
	}
	expectName(t, m, dirIno, "a", uint64(f.InodeNum))
	expectName(t, m, dirIno, "c", 0)
}

// The other node's index follows the rename on its next lookup, since both generations change first.
func TestRenameIsSeenByAnotherNode(t *testing.T) {
	nodes, dirIno := newNodes(t, 2)
	a, b := nodes[0], nodes[1]
	f := newFileIn(t, a, dirIno, "old")
	expectName(t, b, dirIno, "old", uint64(f.InodeNum))
	expectName(t, b, dirIno, "new", 0)

	if _, err := a.Rename(context.Background(), renameOp(t, a, reread(t, a, dirIno), "old", reread(t, a, dirIno), "new")); err != nil {
		t.Fatal(err)
	}
	expectName(t, b, dirIno, "old", 0)
	expectName(t, b, dirIno, "new", uint64(f.InodeNum))
}

func TestRenameGrowsAFullNewParent(t *testing.T) {
	nodes, root := newNodes(t, 1)
	m := nodes[0]
	from, to := newDirIn(t, m, root, "from"), newDirIn(t, m, root, "to")
	f := newFileIn(t, m, from, "moving")
	long := strings.Repeat("n", MaxNameLen-4)
	for i := 0; ; i++ {
		x, err := m.indexOf(reread(t, m, to))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := x.chunkWithRoom(entrySize(&DirEntry{NameLen: MaxNameLen})); !ok {
			break
		}
		mustAdd(t, m, reread(t, m, to), fmt.Sprintf("%s%04d", long, i), uint64(f.InodeNum))
	}
	blocks := reread(t, m, to).Size / BlockSize

	name := strings.Repeat("m", MaxNameLen)
	if _, err := m.Rename(context.Background(), renameOp(t, m, reread(t, m, from), "moving", reread(t, m, to), name)); err != nil {
		t.Fatal(err)
	}
	if got := reread(t, m, to).Size / BlockSize; got != blocks+1 {
		t.Errorf("the full parent has %d blocks after the rename, want %d", got, blocks+1)
	}
	expectName(t, m, to, name, uint64(f.InodeNum))
	expectName(t, m, from, "moving", 0)
}
