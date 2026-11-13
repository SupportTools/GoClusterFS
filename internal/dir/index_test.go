package dir

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	testInodeOffset = 3 << 20
	testDataOffset  = 10 << 20
)

// newNodes returns managers for n nodes sharing one device and lock region, and a directory node 0 made.
func newNodes(t *testing.T, n int) ([]*Manager, *inode.Inode) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nodes.img")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, 200<<20); err != nil {
		t.Fatal(err)
	}
	var managers []*Manager
	for slot := range n {
		dev, err := storage.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { dev.Close() })
		var nodeUUID [16]byte
		copy(nodeUUID[:], fmt.Sprintf("index-node-%04d", slot))
		lockMgr := lock.NewManager(dev, 0, 1<<20, nodeUUID, slot)
		t.Cleanup(lockMgr.ReleaseCached)
		bm := bitmap.NewRegion(dev, 2<<20, 100, lockMgr, nodeUUID)
		inodeTbl := inode.NewTable(dev, testInodeOffset, 100*inode.InodeSize, lockMgr, nodeUUID)
		extentMgr := extent.NewManager(dev, bm, lockMgr, testDataOffset)
		inodeTbl.SetTreeFlusher(extentMgr)
		managers = append(managers, NewManager(dev, inodeTbl, extentMgr, lockMgr, testDataOffset))
	}

	ctx := context.Background()
	tbl := managers[0].inodeTbl
	num, err := tbl.FindFreeSlot()
	if err != nil {
		t.Fatal(err)
	}
	dirIno, err := tbl.ClaimSlot(ctx, num)
	if err != nil {
		t.Fatal(err)
	}
	dirIno.Mode = inode.S_IFDIR | 0o755
	if err := managers[0].InitDirectory(ctx, dirIno, uint64(inode.InodeRoot)); err != nil {
		t.Fatalf("InitDirectory: %v", err)
	}
	return managers, dirIno
}

func mustAdd(t *testing.T, m *Manager, dirIno *inode.Inode, name string, child uint64) {
	t.Helper()
	if err := m.Add(context.Background(), dirIno, name, child, DT_REG); err != nil {
		t.Fatalf("add %s: %v", name, err)
	}
}

func lookupChild(m *Manager, dirIno *inode.Inode, name string) (uint64, error) {
	child, _, err := m.Lookup(context.Background(), dirIno, name)
	return child, err
}

func diskGen(t *testing.T, m *Manager, num uint32) uint64 {
	t.Helper()
	ino, err := m.inodeTbl.Read(num)
	if err != nil {
		t.Fatal(err)
	}
	return ino.MetaGeneration
}

// Node A's index must answer for node B's adds and removes on the next lookup, with no message between them.
func TestLookupSeesOtherNodesChanges(t *testing.T) {
	nodes, dirIno := newNodes(t, 2)
	a, b := nodes[0], nodes[1]

	if _, err := lookupChild(a, dirIno, "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("lookup x before it exists: %v", err)
	}
	mustAdd(t, b, dirIno, "x", 7)
	if child, err := lookupChild(a, dirIno, "x"); err != nil || child != 7 {
		t.Fatalf("node A lookup of node B's x: inode %d, %v", child, err)
	}
	if child, err := lookupChild(a, dirIno, "x"); err != nil || child != 7 {
		t.Fatalf("second lookup of x: inode %d, %v", child, err)
	}
	if err := b.Remove(context.Background(), dirIno, "x"); err != nil {
		t.Fatal(err)
	}
	if child, err := lookupChild(a, dirIno, "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("node A lookup of x after node B removed it: inode %d, %v", child, err)
	}

	stats := a.IndexStats()
	if stats.Hits == 0 || stats.Rebuilds < 3 || stats.Dirs != 1 {
		t.Errorf("node A index stats %+v: want hits, a rebuild per change seen, one directory", stats)
	}
}

// Between RemoveIf returning and the caller freeing the child, no node's index may still name the child.
func TestRemoveIfWritesDirectoryInodeBeforeReturning(t *testing.T) {
	nodes, dirIno := newNodes(t, 2)
	a, b := nodes[0], nodes[1]
	mustAdd(t, a, dirIno, "victim", 9)
	if child, err := lookupChild(a, dirIno, "victim"); err != nil || child != 9 {
		t.Fatalf("lookup victim: inode %d, %v", child, err)
	}

	before := diskGen(t, a, dirIno.InodeNum)
	removed, err := b.RemoveIf(context.Background(), dirIno, "victim", func(child uint64) bool { return child == 9 })
	if err != nil || !removed {
		t.Fatalf("RemoveIf: removed=%v err=%v", removed, err)
	}
	if after := diskGen(t, a, dirIno.InodeNum); after <= before {
		t.Fatalf("directory generation %d after RemoveIf, want above %d", after, before)
	}
	if _, found, ok := a.cachedLookup(dirIno.InodeNum, "victim"); ok {
		t.Fatalf("node A's index answered (found=%v) after node B removed the entry", found)
	}
	if child, err := lookupChild(a, dirIno, "victim"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("lookup victim after removal: inode %d, %v", child, err)
	}
}

// Adds re-check a cached index against the disk under the directory lock: a node B entry in the same chunk must survive.
func TestAddValidatesIndexAgainstDisk(t *testing.T) {
	nodes, dirIno := newNodes(t, 2)
	a, b := nodes[0], nodes[1]
	mustAdd(t, a, dirIno, "x", 7)
	mustAdd(t, b, dirIno, "y", 8)

	if err := a.Add(context.Background(), dirIno, "y", 99, DT_REG); !errors.Is(err, ErrExists) {
		t.Fatalf("node A add of node B's y: %v, want ErrExists", err)
	}
	mustAdd(t, a, dirIno, "z", 10)

	entries, err := b.List(context.Background(), dirIno)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, fmt.Sprintf("%s=%d", e.Name, e.InodeNum))
	}
	slices.Sort(names)
	want := []string{"..=2", fmt.Sprintf(".=%d", dirIno.InodeNum), "x=7", "y=8", "z=10"}
	if !slices.Equal(names, want) {
		t.Fatalf("entries %v, want %v", names, want)
	}
}

func TestIndexLimitEvictsLeastRecentlyUsed(t *testing.T) {
	nodes, first := newNodes(t, 1)
	m := nodes[0]
	ctx := context.Background()
	m.SetIndexLimit(10)

	dirs := []*inode.Inode{first}
	for range 2 {
		num, err := m.inodeTbl.FindFreeSlot()
		if err != nil {
			t.Fatal(err)
		}
		d, err := m.inodeTbl.ClaimSlot(ctx, num)
		if err != nil {
			t.Fatal(err)
		}
		d.Mode = inode.S_IFDIR | 0o755
		if err := m.InitDirectory(ctx, d, uint64(inode.InodeRoot)); err != nil {
			t.Fatal(err)
		}
		dirs = append(dirs, d)
	}
	for i, d := range dirs {
		for j := range 3 {
			mustAdd(t, m, d, fmt.Sprintf("f%d", j), uint64(20+10*i+j))
		}
	}
	for _, d := range dirs {
		if _, err := lookupChild(m, d, "f0"); err != nil {
			t.Fatal(err)
		}
	}
	if s := m.IndexStats(); s.Entries > 10 || s.Dirs != 2 {
		t.Fatalf("index stats %+v: want at most 10 names, in the 2 most recent directories", s)
	}
	if child, err := lookupChild(m, dirs[0], "f2"); err != nil || child != 22 {
		t.Fatalf("lookup in the evicted directory: inode %d, %v", child, err)
	}

	m.SetIndexLimit(0)
	if child, err := lookupChild(m, dirs[1], "f1"); err != nil || child != 31 {
		t.Fatalf("lookup with indexes off: inode %d, %v", child, err)
	}
	if s := m.IndexStats(); s.Dirs != 0 || s.Entries != 0 {
		t.Fatalf("index stats %+v with indexes off", s)
	}
}

// A directory inode that fails its checksum, as a torn read would, must not be trusted to validate an index.
func TestCachedLookupRejectsUnreadableInode(t *testing.T) {
	nodes, dirIno := newNodes(t, 1)
	m := nodes[0]
	mustAdd(t, m, dirIno, "x", 7)
	if _, err := lookupChild(m, dirIno, "x"); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := m.cachedLookup(dirIno.InodeNum, "x"); !ok {
		t.Fatal("index did not answer for an intact directory inode")
	}

	sector := int64(testInodeOffset) + int64(dirIno.InodeNum)/inode.InodesPerSector*inode.SectorSize
	buf := storage.GetBuffer(inode.SectorSize)
	defer storage.PutBuffer(buf)
	if _, err := m.dev.ReadAt(buf.Bytes(), sector); err != nil {
		t.Fatal(err)
	}
	buf.Bytes()[int(dirIno.InodeNum)%inode.InodesPerSector*inode.InodeSize+40] ^= 0xff
	if _, err := m.dev.WriteAt(buf.Bytes(), sector); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := m.cachedLookup(dirIno.InodeNum, "x"); ok {
		t.Fatal("index answered although the directory inode failed its checksum")
	}
}

// A directory inode write that changes no entries, such as an mtime update, must not cost the next lookup a rebuild.
func TestKeepIndexFollowsInodeOnlyWrites(t *testing.T) {
	nodes, dirIno := newNodes(t, 1)
	m := nodes[0]
	mustAdd(t, m, dirIno, "x", 7)
	if _, err := lookupChild(m, dirIno, "x"); err != nil {
		t.Fatal(err)
	}

	touch := func(keep bool) {
		t.Helper()
		ino, err := m.inodeTbl.Read(dirIno.InodeNum)
		if err != nil {
			t.Fatal(err)
		}
		from := ino.MetaGeneration
		ino.Mtime++
		if err := m.inodeTbl.Write(context.Background(), ino); err != nil {
			t.Fatal(err)
		}
		if keep {
			m.KeepIndex(dirIno.InodeNum, from, ino.MetaGeneration)
		}
	}

	touch(true)
	if _, _, ok := m.cachedLookup(dirIno.InodeNum, "x"); !ok {
		t.Fatal("index did not answer after an inode-only write it was told about")
	}
	touch(false)
	if _, _, ok := m.cachedLookup(dirIno.InodeNum, "x"); ok {
		t.Fatal("index answered after a directory write it was not told about")
	}
	m.KeepIndex(dirIno.InodeNum, 0, 1<<62)
	if _, _, ok := m.cachedLookup(dirIno.InodeNum, "x"); ok {
		t.Fatal("KeepIndex moved an index whose generation was not the one given")
	}
}
