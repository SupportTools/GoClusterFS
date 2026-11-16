package goclusterfs

import (
	"context"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/node"
)

// fakeCluster stands in for the node registry: which slots are live, and each slot's orphan list.
type fakeCluster struct {
	mu      sync.Mutex
	live    uint64
	lists   map[int][]node.Orphan
	max     int
	failing bool
}

type fakeSlot struct {
	c    *fakeCluster
	slot int
}

func newFakeCluster(slots ...int) *fakeCluster {
	c := &fakeCluster{lists: map[int][]node.Orphan{}, max: node.MaxOrphans}
	for _, s := range slots {
		c.live |= 1 << s
	}
	return c
}

func (c *fakeCluster) join(t *testing.T, fsys *FS, slot int) *FS {
	t.Helper()
	fsys.SetOrphanRegistry(&fakeSlot{c, slot})
	return fsys
}

func (c *fakeCluster) list(slot int) []node.Orphan {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.lists[slot])
}

func (s *fakeSlot) LocalSlot() int { return s.slot }

func (s *fakeSlot) LiveSlots() (uint64, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	return s.c.live, nil
}

func (s *fakeSlot) AddOrphan(o node.Orphan) error {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	switch l := s.c.lists[s.slot]; {
	case slices.Contains(l, o):
	case len(l) >= s.c.max:
		return node.ErrOrphansFull
	default:
		s.c.lists[s.slot] = append(l, o)
	}
	return nil
}

func (s *fakeSlot) DropOrphans(done func(node.Orphan) bool) error {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	s.c.lists[s.slot] = slices.DeleteFunc(s.c.lists[s.slot], done)
	return nil
}

func (s *fakeSlot) PeersFailing() bool {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	return s.c.failing
}

func (s *fakeSlot) AllOrphans() ([]node.Orphan, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	var all []node.Orphan
	for _, l := range s.c.lists {
		all = append(all, l...)
	}
	return all, nil
}

// createOpen creates name in the root as an O_CREAT open does, leaving it open on fsys.
func createOpen(t *testing.T, fsys *FS, name string) *File {
	t.Helper()
	node, fh, _, errno := fsys.Create(context.Background(), name, syscall.O_RDWR|syscall.O_CREAT, 0o644, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("create %s: %v", name, errno)
	}
	if fh == nil {
		t.Fatalf("create %s returned no file handle, so go-fuse would never call Release", name)
	}
	return node.Operations().(*File)
}

func readInode(t *testing.T, fsys *FS, num uint32) *inode.Inode {
	t.Helper()
	ino, err := fsys.inodeTbl.Read(num)
	if err != nil {
		t.Fatal(err)
	}
	return ino
}

func expectNoProblems(t *testing.T, fsys *FS, when string) {
	t.Helper()
	if r := runFsck(t, fsys, fsck.Options{}); len(r.Problems) > 0 {
		t.Errorf("fsck %s:%s", when, problemList(r))
	}
}

// Node B unlinks a file node A has open: A keeps reading and writing it, no new
// file gets its slot, and A's last close frees it.
func TestFileUnlinkedByAPeerWhileOpenStaysUntilLastClose(t *testing.T) {
	path := newFormattedImage(t)
	cluster := newFakeCluster(0, 1)
	nodeA := cluster.join(t, withBridge(openFormattedFS(t, path)), 0)
	nodeB := cluster.join(t, openFormattedFS(t, path), 1)
	ctx := context.Background()

	f := createOpen(t, nodeA, "f")
	writeAt(t, f, pattern(1, 4096), 0)
	if errno := nodeB.Unlink(ctx, "f"); errno != 0 {
		t.Fatalf("node B unlink: %v", errno)
	}
	ino := readInode(t, nodeA, f.inodeNum)
	if ino.Magic != inode.InodeMagic || ino.LinkCount != 0 || ino.Holders != 1 {
		t.Fatalf("after node B's unlink: magic %#x, links %d, holders %#b; want an orphan held by node A only", ino.Magic, ino.LinkCount, ino.Holders)
	}
	if got := cluster.list(1); !slices.Contains(got, orphanOf(ino)) {
		t.Errorf("node B's orphan list %v misses inode %d", got, f.inodeNum)
	}

	writeAt(t, f, pattern(2, 4096), 0)
	g := createFile(t, nodeB, "g")
	if g.inodeNum == f.inodeNum {
		t.Fatalf("node B's new file took inode %d, which node A still has open", g.inodeNum)
	}
	writeAt(t, g, pattern(3, 4096), 0)
	checkAt(t, f, pattern(2, 4096), 0)
	checkAt(t, g, pattern(3, 4096), 0)

	nodeA.LearnOrphans(1, cluster.list(1))
	if readInode(t, nodeA, f.inodeNum).Magic != inode.InodeMagic {
		t.Fatal("node A let go of a file it still has open")
	}
	if errno := f.Release(ctx, nil); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
	if ino := readInode(t, nodeA, f.inodeNum); ino.Magic != 0 {
		t.Fatalf("after node A's last close: magic %#x, holders %#b; want the slot free", ino.Magic, ino.Holders)
	}
	nodeB.LearnOrphans(1, cluster.list(1))
	if got := cluster.list(1); len(got) != 0 {
		t.Errorf("node B still lists %v after the orphan was freed", got)
	}
	expectNoProblems(t, nodeB, "after the orphan was freed")
}

// A node that dies holding an orphan must not keep it forever: the node that
// replays its journal clears its bit.
func TestDeadHolderLetsGoOfAnOrphan(t *testing.T) {
	path := newFormattedImage(t)
	cluster := newFakeCluster(0, 1)
	nodeA := cluster.join(t, withBridge(openFormattedFS(t, path)), 0)
	nodeB := cluster.join(t, openFormattedFS(t, path), 1)
	ctx := context.Background()

	f := createOpen(t, nodeA, "f")
	writeAt(t, f, pattern(1, 4096), 0)
	if errno := nodeB.Unlink(ctx, "f"); errno != 0 {
		t.Fatalf("node B unlink: %v", errno)
	}
	if err := nodeB.ForgetSlot(ctx, 0, nil); err != nil {
		t.Fatalf("forget node A's slot: %v", err)
	}
	if ino := readInode(t, nodeB, f.inodeNum); ino.Magic != 0 {
		t.Fatalf("after node A died: magic %#x, holders %#b; want the slot free", ino.Magic, ino.Holders)
	}
	expectNoProblems(t, nodeB, "after the dead holder was forgotten")
}

// With no node holding it open, the last unlink frees the file at once.
func TestLastUnlinkOfAClosedFileFreesIt(t *testing.T) {
	fsys := newFakeCluster(0, 1).join(t, openFormattedFS(t, newFormattedImage(t)), 0)
	ctx := context.Background()
	f := createFile(t, fsys, "f")
	writeAt(t, f, pattern(1, 4096), 0)
	if errno := fsys.Unlink(ctx, "f"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	if ino := readInode(t, fsys, f.inodeNum); ino.Magic != inode.InodeMagic || ino.Holders != 1<<1 {
		t.Fatalf("magic %#x, holders %#b; want an orphan held by the other live node", ino.Magic, ino.Holders)
	}

	alone := openFormattedFS(t, newFormattedImage(t))
	g := createFile(t, alone, "g")
	writeAt(t, g, pattern(2, 4096), 0)
	if errno := alone.Unlink(ctx, "g"); errno != 0 {
		t.Fatalf("unlink on a node alone: %v", errno)
	}
	if ino := readInode(t, alone, g.inodeNum); ino.Magic != 0 {
		t.Fatalf("on a node alone: magic %#x; want the slot free", ino.Magic)
	}
	expectNoProblems(t, alone, "after the last unlink")
}

// A full orphan list fails the unlink before it changes anything.
func TestFullOrphanListFailsUnlinkCleanly(t *testing.T) {
	cluster := newFakeCluster(0, 1)
	cluster.max = 0
	defer func(wait time.Duration) { orphanDrainWait = wait }(orphanDrainWait)
	orphanDrainWait = 100 * time.Millisecond
	fsys := cluster.join(t, withBridge(openFormattedFS(t, newFormattedImage(t))), 0)
	ctx := context.Background()
	createFile(t, fsys, "f")
	if errno := fsys.Unlink(ctx, "f"); errno != syscall.EBUSY {
		t.Fatalf("unlink with a full orphan list: %v, want EBUSY", errno)
	}
	if _, errno := fsys.Lookup(ctx, "f", &fuse.EntryOut{}); errno != 0 {
		t.Errorf("f is gone after a refused unlink: %v", errno)
	}
}

// A full list makes room by dropping orphans that peers have already freed.
func TestFullOrphanListMakesRoomForFreedOrphans(t *testing.T) {
	cluster := newFakeCluster(0, 1)
	cluster.max = 1
	cluster.lists[0] = []node.Orphan{{Inode: 99, Birth: 1}}
	fsys := cluster.join(t, openFormattedFS(t, newFormattedImage(t)), 0)
	f := createFile(t, fsys, "f")
	if errno := fsys.Unlink(context.Background(), "f"); errno != 0 {
		t.Fatalf("unlink with a list full of a freed orphan: %v", errno)
	}
	if got := cluster.list(0); len(got) != 1 || got[0].Inode != f.inodeNum {
		t.Errorf("orphan list %v, want only inode %d", got, f.inodeNum)
	}
}

// A node alone that unlinks a file it has open lists it, so the next mount of its slot frees it after a crash.
func TestNodeAloneListsAnOrphanItHoldsOpen(t *testing.T) {
	path := newFormattedImage(t)
	cluster := newFakeCluster(0)
	fsys := cluster.join(t, withBridge(openFormattedFS(t, path)), 0)
	ctx := context.Background()
	f := createOpen(t, fsys, "f")
	writeAt(t, f, pattern(1, 4096), 0)
	if errno := fsys.Unlink(ctx, "f"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	if ino := readInode(t, fsys, f.inodeNum); ino.Holders != 1 || !slices.Contains(cluster.list(0), orphanOf(ino)) {
		t.Fatalf("holders %#b, list %v; want inode %d held by this node and listed", ino.Holders, cluster.list(0), f.inodeNum)
	}

	remounted := cluster.join(t, openFormattedFS(t, path), 0)
	if err := remounted.ForgetSlot(ctx, 0, nil); err != nil {
		t.Fatalf("forget the crashed incarnation: %v", err)
	}
	if ino := readInode(t, remounted, f.inodeNum); ino.Magic != 0 {
		t.Fatalf("after the next mount: magic %#x, holders %#b; want the slot free", ino.Magic, ino.Holders)
	}
	expectNoProblems(t, remounted, "after the next mount let go")
}

// A full list keeps waiting while a peer is failing, since its replay will free what it held.
func TestFailingPeerKeepsAFullListWaiting(t *testing.T) {
	cluster := newFakeCluster(0, 1)
	cluster.max = 0
	cluster.failing = true
	defer func(wait time.Duration) { orphanDrainWait = wait }(orphanDrainWait)
	orphanDrainWait = 50 * time.Millisecond
	fsys := cluster.join(t, withBridge(openFormattedFS(t, newFormattedImage(t))), 0)
	createFile(t, fsys, "f")

	const failingFor = 300 * time.Millisecond
	time.AfterFunc(failingFor, func() {
		cluster.mu.Lock()
		defer cluster.mu.Unlock()
		cluster.failing = false
	})
	start := time.Now()
	if errno := fsys.Unlink(context.Background(), "f"); errno != syscall.EBUSY || time.Since(start) < failingFor {
		t.Fatalf("unlink: %v after %v; want EBUSY only after the peer stopped failing (%v)", errno, time.Since(start), failingFor)
	}
}

// The unlinking node lists a file before its last link goes; a peer that looks in between must still let go after.
func TestPeerReleasesAnOrphanItSawBeforeTheDrop(t *testing.T) {
	path := newFormattedImage(t)
	cluster := newFakeCluster(0, 1)
	nodeA := cluster.join(t, openFormattedFS(t, path), 0)
	nodeB := cluster.join(t, openFormattedFS(t, path), 1)
	ctx := context.Background()
	f := createFile(t, nodeB, "f")

	live, err := nodeB.reserveOrphan(readInode(t, nodeB, f.inodeNum))
	if err != nil {
		t.Fatal(err)
	}
	nodeA.LearnOrphans(1, cluster.list(1))
	if err := nodeB.dropLink(ctx, f.inodeNum, live); err != nil {
		t.Fatal(err)
	}
	if ino := readInode(t, nodeA, f.inodeNum); ino.LinkCount != 0 || ino.Holders != 1 {
		t.Fatalf("after the drop: links %d, holders %#b; want an orphan node A holds", ino.LinkCount, ino.Holders)
	}
	nodeA.LearnOrphans(1, cluster.list(1))
	if ino := readInode(t, nodeA, f.inodeNum); ino.Magic != 0 {
		t.Fatalf("node A never let go: magic %#x, holders %#b", ino.Magic, ino.Holders)
	}
}

// A node's own pass keeps an entry whose unlink sits between reserveOrphan and dropLink; ForgetSlot of its slot drops it.
func TestOwnPassKeepsAReservedOrphan(t *testing.T) {
	cluster := newFakeCluster(0, 1)
	fsys := cluster.join(t, openFormattedFS(t, newFormattedImage(t)), 0)
	f := createFile(t, fsys, "f")
	o := orphanOf(readInode(t, fsys, f.inodeNum))
	if _, err := fsys.reserveOrphan(readInode(t, fsys, f.inodeNum)); err != nil {
		t.Fatal(err)
	}
	fsys.LearnOrphans(0, cluster.list(0))
	if !slices.Contains(cluster.list(0), o) {
		t.Fatalf("this node's own pass dropped %v before the drop", o)
	}
	if err := fsys.ForgetSlot(context.Background(), 0, nil); err != nil {
		t.Fatal(err)
	}
	if got := cluster.list(0); len(got) != 0 {
		t.Errorf("ForgetSlot of this node's slot kept %v, a still-linked entry no unlink is working on", got)
	}
}
