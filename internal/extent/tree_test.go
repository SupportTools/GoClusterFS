package extent

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	treeTestInodeOffset = 1 << 20
	treeTestDataOffset  = 2 << 20
	treeTestBlocks      = 1000
	fakePhys            = 1 << 20 // data blocks the bitmap does not track
)

type treeEnv struct {
	t   *testing.T
	ctx context.Context
	dev *storage.Device
	bm  *bitmap.Region
	tbl *inode.Table
	mgr *Manager
}

// newTreeEnv sets up a device, bitmap, and inode table. A non-zero nodeCap
// shrinks both leaf and index capacity so tests reach splits quickly.
func newTreeEnv(t *testing.T, nodeCap int) *treeEnv {
	t.Helper()
	dev, cleanup := createTestDevice(t, 8<<20)
	t.Cleanup(cleanup)
	var uuid [16]byte
	env := &treeEnv{
		t:   t,
		ctx: context.Background(),
		dev: dev,
		bm:  bitmap.NewRegion(dev, 0, treeTestBlocks, nil, uuid),
		tbl: inode.NewTable(dev, treeTestInodeOffset, 64*inode.InodeSize, nil, uuid),
	}
	env.remount(nodeCap)
	return env
}

// remount replaces the manager, dropping the node cache as a new mount would.
func (env *treeEnv) remount(nodeCap int) {
	env.mgr = NewManager(env.dev, env.bm, nil, treeTestDataOffset)
	if nodeCap > 0 {
		env.mgr.leafCap, env.mgr.indexCap = nodeCap, nodeCap
	}
	env.tbl.SetTreeFlusher(env.mgr)
}

func (env *treeEnv) newInode() *inode.Inode {
	env.t.Helper()
	num, err := env.tbl.FindFreeSlot()
	if err != nil {
		env.t.Fatalf("find free inode: %v", err)
	}
	ino, err := env.tbl.ClaimSlot(env.ctx, num)
	if err != nil {
		env.t.Fatalf("claim inode %d: %v", num, err)
	}
	ino.Mode = inode.S_IFREG | 0o644
	return ino
}

func (env *treeEnv) write(ino *inode.Inode) {
	env.t.Helper()
	if err := env.tbl.Write(env.ctx, ino); err != nil {
		env.t.Fatalf("write inode %d: %v", ino.InodeNum, err)
	}
	if ino.PendingTree != nil {
		env.t.Fatal("PendingTree should be cleared after a successful write")
	}
}

func (env *treeEnv) reread(ino *inode.Inode) *inode.Inode {
	env.t.Helper()
	fresh, err := env.tbl.Read(ino.InodeNum)
	if err != nil {
		env.t.Fatalf("read inode %d: %v", ino.InodeNum, err)
	}
	return fresh
}

func (env *treeEnv) insert(ino *inode.Inode, x Extent) {
	env.t.Helper()
	if err := env.mgr.Insert(env.ctx, ino, &x); err != nil {
		env.t.Fatalf("Insert %+v: %v", x, err)
	}
}

// insertScattered inserts n single-block extents that can never merge.
func (env *treeEnv) insertScattered(ino *inode.Inode, n int) {
	env.t.Helper()
	for i := 0; i < n; i++ {
		env.insert(ino, Extent{uint64(i * 2), fakePhys + uint64(i*3), 1, FlagAllocated | FlagShared})
	}
}

func (env *treeEnv) checkScattered(ino *inode.Inode, n int) {
	env.t.Helper()
	env.checkScatteredWith(ino, n, n)
}

// checkScatteredWith checks the first n scattered extents and the total extent count.
func (env *treeEnv) checkScatteredWith(ino *inode.Inode, n, total int) {
	env.t.Helper()
	for i := 0; i < n; i++ {
		phys, err := env.mgr.LookupPhysical(ino, uint64(i*2))
		if err != nil || phys != fakePhys+uint64(i*3) {
			env.t.Fatalf("block %d: got %d, %v; want %d", i*2, phys, err, fakePhys+uint64(i*3))
		}
		if _, _, err := env.mgr.Lookup(ino, uint64(i*2+1)); err != ErrExtentNotFound {
			env.t.Fatalf("block %d should be a hole, got %v", i*2+1, err)
		}
	}
	if got := len(listExtents(env.t, env.mgr, ino)); got != total || ino.ExtentCount != uint64(total) {
		env.t.Fatalf("extent count: listed %d, inode says %d, want %d", got, ino.ExtentCount, total)
	}
}

func TestTreePersistsAcrossRemount(t *testing.T) {
	env := newTreeEnv(t, 0)
	ino := env.newInode()
	env.insertScattered(ino, 500)
	env.write(ino)

	env.remount(0)
	ino = env.reread(ino)
	if !ino.HasExtentTree() || ino.ExtentDepth != 1 {
		t.Fatalf("want a depth-1 tree, got flags=%#x depth=%d", ino.Flags, ino.ExtentDepth)
	}
	env.checkScattered(ino, 500)
}

func TestTreeGrowsToDepthTwoWithDefaultCapacity(t *testing.T) {
	env := newTreeEnv(t, 0)
	ino := env.newInode()
	env.insertScattered(ino, 3*defaultLeafCap)
	env.write(ino)
	if ino.ExtentDepth != 2 {
		t.Fatalf("ExtentDepth = %d, want 2", ino.ExtentDepth)
	}

	env.remount(0)
	env.checkScattered(env.reread(ino), 3*defaultLeafCap)
}

func TestTreeGrowsToDepthTwo(t *testing.T) {
	env := newTreeEnv(t, 4)
	ino := env.newInode()
	env.insertScattered(ino, 15)
	if ino.ExtentDepth != 2 {
		t.Fatalf("ExtentDepth = %d, want 2", ino.ExtentDepth)
	}
	env.write(ino)

	env.remount(4)
	ino = env.reread(ino)
	env.checkScattered(ino, 15)

	// Further edits after a remount touch only some leaves; the rest must survive.
	env.insert(ino, Extent{1000, fakePhys + 5000, 1, FlagAllocated | FlagShared})
	env.write(ino)
	env.remount(4)
	ino = env.reread(ino)
	env.checkScatteredWith(ino, 15, 16)
	if phys, err := env.mgr.LookupPhysical(ino, 1000); err != nil || phys != fakePhys+5000 {
		t.Fatalf("block 1000: got %d, %v", phys, err)
	}
}

func TestTreeFull(t *testing.T) {
	env := newTreeEnv(t, 2)
	ino := env.newInode()
	env.insertScattered(ino, 4)

	err := env.mgr.Insert(env.ctx, ino, &Extent{100, fakePhys + 100, 1, FlagAllocated})
	if !errors.Is(err, ErrExtentTreeFull) {
		t.Fatalf("Insert into a full tree: got %v, want ErrExtentTreeFull", err)
	}
	env.checkScattered(ino, 4)
}

func TestSequentialAllocationsMerge(t *testing.T) {
	env := newTreeEnv(t, 0)
	ino := env.newInode()
	for b := uint64(0); b < 10; b++ {
		if _, err := env.mgr.AllocateBlocks(env.ctx, ino, b, 1); err != nil {
			t.Fatalf("AllocateBlocks(%d): %v", b, err)
		}
	}
	extents := listExtents(t, env.mgr, ino)
	if len(extents) != 1 || extents[0].Length != 10 {
		t.Fatalf("contiguous appends should merge into one extent, got %+v", extents)
	}
	if ino.HasExtentTree() {
		t.Fatal("one extent should stay inline")
	}
}

func TestInterleavedAppendsUseTree(t *testing.T) {
	env := newTreeEnv(t, 0)
	a, b := env.newInode(), env.newInode()
	for blk := uint64(0); blk < 20; blk++ {
		for _, ino := range []*inode.Inode{a, b} {
			if _, err := env.mgr.AllocateBlocks(env.ctx, ino, blk, 1); err != nil {
				t.Fatalf("inode %d block %d: %v", ino.InodeNum, blk, err)
			}
		}
	}
	env.write(a)
	env.write(b)

	env.remount(0)
	for _, ino := range []*inode.Inode{a, b} {
		ino = env.reread(ino)
		seen := map[uint64]bool{}
		for blk := uint64(0); blk < 20; blk++ {
			phys, err := env.mgr.LookupPhysical(ino, blk)
			if err != nil {
				t.Fatalf("inode %d block %d: %v", ino.InodeNum, blk, err)
			}
			if seen[phys] {
				t.Fatalf("inode %d maps physical block %d twice", ino.InodeNum, phys)
			}
			seen[phys] = true
		}
		if !ino.HasExtentTree() || ino.AllocatedBlocks != 20 {
			t.Fatalf("inode %d: tree=%v allocated=%d", ino.InodeNum, ino.HasExtentTree(), ino.AllocatedBlocks)
		}
	}
}

func TestAllocateUsesFragmentedFreeSpace(t *testing.T) {
	env := newTreeEnv(t, 0)
	filler := env.newInode()
	for blk := uint64(0); blk < 8; blk++ {
		if _, err := env.mgr.AllocateBlocks(env.ctx, filler, blk*2, 1); err != nil {
			t.Fatal(err)
		}
	}
	// Free every other block so the next allocation cannot be contiguous.
	for blk := uint64(0); blk < 8; blk += 2 {
		if err := env.mgr.FreeExtent(env.ctx, filler, blk*2); err != nil {
			t.Fatal(err)
		}
	}
	env.write(filler)

	ino := env.newInode()
	if _, err := env.mgr.AllocateUnwritten(env.ctx, ino, 0, 6); err != nil {
		t.Fatalf("AllocateUnwritten over fragmented space: %v", err)
	}
	if ino.AllocatedBlocks != 6 {
		t.Fatalf("AllocatedBlocks = %d, want 6", ino.AllocatedBlocks)
	}
	for blk := uint64(0); blk < 6; blk++ {
		ext, _, err := env.mgr.Lookup(ino, blk)
		if err != nil || ext.Flags&FlagUnwritten == 0 {
			t.Fatalf("block %d: %+v, %v", blk, ext, err)
		}
	}
}

func TestTruncateFreesDataAndTreeBlocks(t *testing.T) {
	env := newTreeEnv(t, 4)
	free := env.bm.FreeCount()

	a, b := env.newInode(), env.newInode()
	for blk := uint64(0); blk < 12; blk++ {
		for _, ino := range []*inode.Inode{a, b} {
			if _, err := env.mgr.AllocateBlocks(env.ctx, ino, blk, 1); err != nil {
				t.Fatal(err)
			}
		}
	}
	env.write(a)
	env.write(b)
	if a.ExtentDepth != 2 {
		t.Fatalf("want depth 2 with 12 fragmented blocks, got %d", a.ExtentDepth)
	}

	if err := env.mgr.Truncate(env.ctx, a, 2*BlockSize); err != nil {
		t.Fatal(err)
	}
	env.write(a)
	if a.HasExtentTree() || a.ExtentCount != 2 {
		t.Fatalf("want 2 inline extents after truncating to 2 blocks, got depth=%d count=%d", a.ExtentDepth, a.ExtentCount)
	}
	for _, ino := range []*inode.Inode{a, b} {
		if err := env.mgr.Truncate(env.ctx, ino, 0); err != nil {
			t.Fatal(err)
		}
		env.write(ino)
	}
	if got := env.bm.FreeCount(); got != free {
		t.Fatalf("free blocks = %d after truncating everything, want %d", got, free)
	}
}

func TestTreeFoldsBackInline(t *testing.T) {
	env := newTreeEnv(t, 0)
	free := env.bm.FreeCount()
	ino := env.newInode()
	env.insertScattered(ino, 5)
	env.write(ino)
	if env.bm.FreeCount() != free-1 {
		t.Fatalf("a depth-1 tree should use one block")
	}

	if err := env.mgr.Truncate(env.ctx, ino, 3*BlockSize); err != nil {
		t.Fatal(err)
	}
	env.write(ino)
	if ino.HasExtentTree() || ino.ExtentCount != 2 || ino.ExtentTreeGen != 0 {
		t.Fatalf("want 2 inline extents, got flags=%#x count=%d gen=%d", ino.Flags, ino.ExtentCount, ino.ExtentTreeGen)
	}
	if env.bm.FreeCount() != free {
		t.Fatal("tree block should be freed after folding back inline")
	}

	env.remount(0)
	ino = env.reread(ino)
	env.checkScattered(ino, 2)
}

func TestFlushWithoutInodeWriteKeepsCommittedTree(t *testing.T) {
	env := newTreeEnv(t, 8)
	ino := env.newInode()
	env.insertScattered(ino, 10)
	env.write(ino)

	// Simulate a crash after the new node versions hit the disk but before the inode write.
	for i := 10; i < 30; i++ {
		env.insert(ino, Extent{uint64(i * 2), fakePhys + uint64(i*3), 1, FlagAllocated | FlagShared})
	}
	if err := env.mgr.Truncate(env.ctx, ino, 5*BlockSize); err != nil {
		t.Fatal(err)
	}
	onDisk := env.reread(ino)
	if _, err := env.mgr.FlushTree(ino, onDisk); err != nil {
		t.Fatalf("FlushTree: %v", err)
	}

	env.remount(8)
	env.checkScattered(env.reread(ino), 10)
}

func TestConcurrentTreeChangeIsDetected(t *testing.T) {
	env := newTreeEnv(t, 0)
	ino := env.newInode()
	env.insertScattered(ino, 3)
	env.write(ino)

	first, second := env.reread(ino), env.reread(ino)
	env.insert(first, Extent{100, fakePhys + 100, 1, FlagAllocated | FlagShared})
	env.insert(second, Extent{200, fakePhys + 200, 1, FlagAllocated | FlagShared})
	env.write(first)

	if err := env.tbl.Write(env.ctx, second); !errors.Is(err, ErrTreeConflict) {
		t.Fatalf("second writer: got %v, want ErrTreeConflict", err)
	}
	ino = env.reread(ino)
	if _, _, err := env.mgr.Lookup(ino, 100); err != nil {
		t.Fatalf("first writer's extent lost: %v", err)
	}
}

func TestStaleInodeIsDetected(t *testing.T) {
	env := newTreeEnv(t, 0)
	ino := env.newInode()
	env.insertScattered(ino, 3)
	env.write(ino)
	stale := env.reread(ino)

	// Two more commits reuse the half the stale copy points at.
	for i := 0; i < 2; i++ {
		env.insert(ino, Extent{uint64(100 + i), fakePhys + uint64(100+i*7), 1, FlagAllocated | FlagShared})
		env.write(ino)
	}

	env.remount(0)
	if _, _, err := env.mgr.Lookup(stale, 0); !errors.Is(err, ErrStaleTree) {
		t.Fatalf("stale inode lookup: got %v, want ErrStaleTree", err)
	}
}

func TestCorruptNodeIsDetected(t *testing.T) {
	env := newTreeEnv(t, 0)
	ino := env.newInode()
	env.insertScattered(ino, 3)
	env.write(ino)

	off := TreeNodeOffset(treeTestDataOffset, ino.ExtentTreeRoot, ino.ExtentTreeGen)
	buf := storage.NewAlignedBuffer(storage.SectorSize)
	if _, err := env.dev.ReadAt(buf.Bytes(), off); err != nil {
		t.Fatal(err)
	}
	buf.Bytes()[nodeHeaderSize+8] ^= 0xFF
	if _, err := env.dev.WriteAt(buf.Bytes(), off); err != nil {
		t.Fatal(err)
	}

	env.remount(0)
	if _, _, err := env.mgr.Lookup(env.reread(ino), 0); !errors.Is(err, ErrCorruptTree) {
		t.Fatalf("lookup in corrupted node: got %v, want ErrCorruptTree", err)
	}
}

func TestCloneGetsItsOwnTree(t *testing.T) {
	env := newTreeEnv(t, 4)
	src := env.newInode()
	env.insertScattered(src, 9)
	env.write(src)

	dst := env.newInode()
	if err := env.mgr.CopyExtentsForClone(env.ctx, src, dst); err != nil {
		t.Fatal(err)
	}
	env.write(dst)
	if dst.ExtentTreeRoot == src.ExtentTreeRoot {
		t.Fatal("clone must not share tree blocks with its source")
	}

	if err := env.mgr.Truncate(env.ctx, src, 0); err != nil {
		t.Fatal(err)
	}
	env.write(src)
	env.remount(4)
	env.checkScattered(env.reread(dst), 9)
}

func TestReserveTreeBlocks(t *testing.T) {
	env := newTreeEnv(t, 0)
	free := env.bm.FreeCount()
	ino := env.newInode()
	env.insertScattered(ino, 2)

	ctx, release, err := env.mgr.ReserveTreeBlocks(env.ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	if env.bm.FreeCount() != free-1 {
		t.Fatalf("spilling a full inline map should reserve one block")
	}
	if err := env.mgr.Insert(ctx, ino, &Extent{10, fakePhys + 10, 1, FlagAllocated | FlagShared}); err != nil {
		t.Fatal(err)
	}
	if env.bm.FreeCount() != free-1 {
		t.Fatal("the spill should use the reserved block instead of allocating")
	}
	release()
	if env.bm.FreeCount() != free-1 {
		t.Fatal("release must not free a block the tree uses")
	}

	_, release, err = env.mgr.ReserveTreeBlocks(env.ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if env.bm.FreeCount() != free-1 {
		t.Fatal("a depth-1 tree with room needs no reservation")
	}
}

func TestParseRejectsBadNodes(t *testing.T) {
	n := &TreeNode{Level: LevelLeaf, Inode: 7, Gen: 3, Extents: []Extent{{0, 10, 2, FlagAllocated}, {5, 20, 1, FlagAllocated}}}
	buf := make([]byte, treeNodeSize(2))
	encodeTreeNode(buf, n)
	got, err := parseTreeNode(buf)
	if err != nil || len(got.Extents) != 2 || got.Extents[1] != n.Extents[1] || got.Gen != 3 || got.Inode != 7 {
		t.Fatalf("round trip: %+v, %v", got, err)
	}

	overlap := &TreeNode{Level: LevelLeaf, Extents: []Extent{{0, 10, 5, 0}, {3, 20, 1, 0}}}
	encodeTreeNode(buf, overlap)
	if _, err := parseTreeNode(buf); !errors.Is(err, ErrCorruptTree) {
		t.Fatalf("overlapping extents: got %v", err)
	}

	index := &TreeNode{Level: LevelIndex, Children: []TreeChild{{First: 5, Block: 1, Gen: 1, Count: 1}}}
	encodeTreeNode(buf, index)
	if _, err := parseTreeNode(buf); !errors.Is(err, ErrCorruptTree) {
		t.Fatalf("index not starting at 0: got %v", err)
	}
}

type modelBlock struct {
	phys  uint64
	flags uint32
}

// TestTreeMatchesModel applies random inserts, remaps, and truncates and checks
// the extent map against a per-block model, flushing and remounting regularly.
func TestTreeMatchesModel(t *testing.T) {
	for _, nodeCap := range []int{3, 8, 0} {
		t.Run(fmt.Sprintf("nodeCap=%d", nodeCap), func(t *testing.T) {
			env := newTreeEnv(t, nodeCap)
			free := env.bm.FreeCount()
			rng := rand.New(rand.NewSource(int64(nodeCap)))
			ino := env.newInode()
			model := map[uint64]modelBlock{}
			const span = 300

			for op := 0; op < 3000; op++ {
				switch r := rng.Intn(100); {
				case r < 70:
					lb := uint64(rng.Intn(span))
					length := uint64(1 + rng.Intn(3))
					phys := fakePhys + uint64(rng.Intn(4*span))
					if prev, ok := model[lb-1]; ok && lb > 0 && rng.Intn(2) == 0 {
						phys = prev.phys + 1 // invite a merge
					}
					flags := uint32(FlagAllocated | FlagShared)
					if rng.Intn(4) == 0 {
						flags |= FlagUnwritten
					}
					overlaps := false
					for b := lb; b < lb+length; b++ {
						_, overlaps = model[b]
						if overlaps {
							break
						}
					}
					err := env.mgr.Insert(env.ctx, ino, &Extent{lb, phys, length, flags})
					switch {
					case overlaps:
						if !errors.Is(err, ErrExtentOverlap) {
							t.Fatalf("op %d: overlapping insert returned %v", op, err)
						}
					case errors.Is(err, ErrExtentTreeFull):
					case err != nil:
						t.Fatalf("op %d: insert: %v", op, err)
					default:
						for b := uint64(0); b < length; b++ {
							model[lb+b] = modelBlock{phys + b, flags}
						}
					}
				case r < 85:
					lb := uint64(rng.Intn(span))
					cur, ok := model[lb]
					if !ok {
						continue
					}
					private := Extent{lb, fakePhys + 10*span + uint64(op), 1, cur.flags &^ FlagShared}
					err := env.mgr.remap(env.ctx, ino, private)
					if errors.Is(err, ErrExtentTreeFull) {
						continue
					}
					if err != nil {
						t.Fatalf("op %d: remap: %v", op, err)
					}
					model[lb] = modelBlock{private.PhysicalBlock, private.Flags}
				default:
					size := uint64(rng.Intn(span+10)) * BlockSize / uint64(1+rng.Intn(2))
					if err := env.mgr.Truncate(env.ctx, ino, size); err != nil {
						t.Fatalf("op %d: truncate: %v", op, err)
					}
					for b := range model {
						if b >= BlocksForSize(size) {
							delete(model, b)
						}
					}
				}

				if op%97 == 0 {
					env.write(ino)
					env.remount(nodeCap)
					ino = env.reread(ino)
				}
				if op%13 == 0 {
					checkModel(t, env, ino, model, span+10)
				}
			}
			env.write(ino)
			env.remount(nodeCap)
			ino = env.reread(ino)
			checkModel(t, env, ino, model, span+10)

			if err := env.mgr.Truncate(env.ctx, ino, 0); err != nil {
				t.Fatal(err)
			}
			env.write(ino)
			if got := env.bm.FreeCount(); got != free {
				t.Fatalf("tree blocks leaked: %d free, want %d", got, free)
			}
		})
	}
}

func checkModel(t *testing.T, env *treeEnv, ino *inode.Inode, model map[uint64]modelBlock, span uint64) {
	t.Helper()
	for b := uint64(0); b < span; b++ {
		ext, off, err := env.mgr.Lookup(ino, b)
		want, mapped := model[b]
		switch {
		case !mapped && err != ErrExtentNotFound:
			t.Fatalf("block %d should be a hole, got %+v, %v", b, ext, err)
		case mapped && err != nil:
			t.Fatalf("block %d: %v", b, err)
		case mapped && (ext.PhysicalBlock+off != want.phys || ext.Flags != want.flags):
			t.Fatalf("block %d: got phys %d flags %#x, want %d %#x", b, ext.PhysicalBlock+off, ext.Flags, want.phys, want.flags)
		}
	}

	extents := listExtents(t, env.mgr, ino)
	total := uint64(0)
	for i, x := range extents {
		total += x.Length
		if i > 0 {
			prev := extents[i-1]
			if x.LogicalBlock < prev.LogicalBlock+prev.Length {
				t.Fatalf("extents %d and %d overlap: %+v %+v", i-1, i, *prev, *x)
			}
		}
	}
	if total != uint64(len(model)) || ino.ExtentCount != uint64(len(extents)) {
		t.Fatalf("listed %d blocks in %d extents (inode says %d), model has %d blocks",
			total, len(extents), ino.ExtentCount, len(model))
	}
	if inline := len(extents) <= MaxInlineExtents; inline == ino.HasExtentTree() {
		t.Fatalf("%d extents but HasExtentTree=%v", len(extents), ino.HasExtentTree())
	}
}

func TestInsertSortedMerges(t *testing.T) {
	const f = FlagAllocated
	got := insertSorted([]Extent{{0, 100, 2, f}, {4, 104, 2, f}}, Extent{2, 102, 2, f})
	if len(got) != 1 || got[0] != (Extent{0, 100, 6, f}) {
		t.Errorf("bridging insert: %+v", got)
	}
	got = insertSorted([]Extent{{10, 200, 2, f}}, Extent{8, 198, 2, f})
	if len(got) != 1 || got[0] != (Extent{8, 198, 4, f}) {
		t.Errorf("merge with next: %+v", got)
	}
	got = insertSorted([]Extent{{0, 100, 2, f}}, Extent{2, 102, 1, f | FlagUnwritten})
	if len(got) != 2 {
		t.Errorf("different flags must not merge: %+v", got)
	}
	got = insertSorted([]Extent{{0, 100, 1, f | FlagSFB}}, Extent{1, 101, 1, f | FlagSFB})
	if len(got) != 2 {
		t.Errorf("SFB extents must not merge: %+v", got)
	}
	got = insertSorted([]Extent{{0, 100, 2, f}}, Extent{2, 300, 1, f})
	if len(got) != 2 || got[1] != (Extent{2, 300, 1, f}) {
		t.Errorf("physically distant extents must not merge: %+v", got)
	}
}

func TestNodeCacheEvictsOldest(t *testing.T) {
	c := newNodeCache(10)
	for i := 0; i < 5; i++ {
		c.put(nodeKey{1, uint64(i), 1}, &TreeNode{Extents: make([]Extent, 3)})
	}
	if _, ok := c.get(nodeKey{1, 0, 1}); ok {
		t.Error("oldest node should be evicted")
	}
	if _, ok := c.get(nodeKey{1, 4, 1}); !ok {
		t.Error("newest node should be cached")
	}
	if c.size > c.max {
		t.Errorf("cache size %d exceeds max %d", c.size, c.max)
	}
}

func TestTruncateSFBDoesNotFreeBitmapBlocks(t *testing.T) {
	env := newTreeEnv(t, 0)
	if _, err := env.bm.Alloc(env.ctx, 8); err != nil {
		t.Fatal(err)
	}
	free := env.bm.FreeCount()

	// Container 0, slot 7 packs to physical value 7, which is an allocated bitmap block.
	ino := env.newInode()
	sfbExt := Extent{Length: 1}
	sfbExt.SetSFBLocation(0, 7)
	env.insert(ino, sfbExt)
	if err := env.mgr.Truncate(env.ctx, ino, 0); err != nil {
		t.Fatal(err)
	}
	if got := env.bm.FreeCount(); got != free {
		t.Fatalf("truncating an SFB file changed free blocks from %d to %d", free, got)
	}
}

// A truncate that loses a conflict must not free blocks the winning commit still maps.
func TestConflictingTruncateFreesNothing(t *testing.T) {
	env := newTreeEnv(t, 0)
	ino := env.newInode()
	for blk := uint64(0); blk < 6; blk++ {
		if _, err := env.mgr.AllocateBlocks(env.ctx, ino, blk*2, 1); err != nil {
			t.Fatal(err)
		}
	}
	env.write(ino)

	loser, winner := env.reread(ino), env.reread(ino)
	if err := env.mgr.Truncate(env.ctx, loser, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := env.mgr.AllocateBlocks(env.ctx, winner, 100, 1); err != nil {
		t.Fatal(err)
	}
	env.write(winner)
	free := env.bm.FreeCount()

	if err := env.tbl.Write(env.ctx, loser); !errors.Is(err, ErrTreeConflict) {
		t.Fatalf("losing truncate: got %v, want ErrTreeConflict", err)
	}
	env.mgr.DiscardPending(env.ctx, loser)
	if got := env.bm.FreeCount(); got != free {
		t.Fatalf("losing truncate changed free blocks from %d to %d", free, got)
	}
	env.remount(0)
	ino = env.reread(ino)
	for blk := uint64(0); blk < 6; blk++ {
		if _, _, err := env.mgr.Lookup(ino, blk*2); err != nil {
			t.Fatalf("block %d lost: %v", blk*2, err)
		}
	}
}

func TestDeleteFreesWithoutInodeWrite(t *testing.T) {
	env := newTreeEnv(t, 4)
	free := env.bm.FreeCount()
	ino := env.newInode()
	for blk := uint64(0); blk < 12; blk++ {
		if _, err := env.mgr.AllocateBlocks(env.ctx, ino, blk*2, 1); err != nil {
			t.Fatal(err)
		}
	}
	env.write(ino)

	if err := env.mgr.Delete(env.ctx, ino); err != nil {
		t.Fatal(err)
	}
	if ino.PendingTree != nil || ino.HasExtentTree() {
		t.Fatal("Delete should leave no tree and no pending change")
	}
	if got := env.bm.FreeCount(); got != free {
		t.Fatalf("free blocks after Delete = %d, want %d", got, free)
	}
}

func TestDiscardPendingFreesNewTreeBlocks(t *testing.T) {
	env := newTreeEnv(t, 0)
	free := env.bm.FreeCount()
	ino := env.newInode()
	env.insertScattered(ino, 3)
	if env.bm.FreeCount() != free-1 {
		t.Fatal("spilling to a tree should allocate one block")
	}
	env.mgr.DiscardPending(env.ctx, ino)
	if got := env.bm.FreeCount(); got != free {
		t.Fatalf("free blocks after discard = %d, want %d", got, free)
	}
}

// Writing a stale copy that has no extent edits must not roll back the tree.
func TestStaleInodeWriteIsRejected(t *testing.T) {
	env := newTreeEnv(t, 0)
	ino := env.newInode()
	env.insertScattered(ino, 3)
	env.write(ino)

	stale := env.reread(ino)
	for i := 0; i < 2; i++ {
		env.insert(ino, Extent{uint64(100 + i), fakePhys + uint64(100+i*7), 1, FlagAllocated | FlagShared})
		env.write(ino)
	}
	stale.Size = 12345
	if err := env.tbl.Write(env.ctx, stale); !errors.Is(err, ErrTreeConflict) {
		t.Fatalf("stale write: got %v, want ErrTreeConflict", err)
	}

	env.remount(0)
	got := env.reread(ino)
	if got.Size == 12345 {
		t.Fatal("the rejected stale write must not change the inode")
	}
	env.checkScatteredWith(got, 3, 5)
}

func TestDataBlocks(t *testing.T) {
	env := newTreeEnv(t, 0)
	ino := env.newInode()
	env.insert(ino, Extent{0, 10, 3, FlagAllocated})
	sfb := Extent{LogicalBlock: 3, Length: 1}
	sfb.SetSFBLocation(2, 5)
	env.insert(ino, sfb)
	env.insert(ino, Extent{8, 20, 2, FlagAllocated})

	got, err := env.mgr.DataBlocks(ino)
	if err != nil {
		t.Fatal(err)
	}
	if want := []uint64{10, 11, 12, 20, 21}; !slices.Equal(got, want) {
		t.Fatalf("DataBlocks = %v, want %v (SFB extents skipped)", got, want)
	}
}

// A spill that started from inline extents conflicts when another node changed
// them; the extents it carries over may map blocks that node freed.
func TestStaleInlineSpillIsDetected(t *testing.T) {
	env := newTreeEnv(t, 0)
	ino := env.newInode()
	for _, lb := range []uint64{0, 2} {
		if _, err := env.mgr.AllocateBlocks(env.ctx, ino, lb, 1); err != nil {
			t.Fatal(err)
		}
	}
	env.write(ino)

	loser, winner := env.reread(ino), env.reread(ino)
	if err := env.mgr.Truncate(env.ctx, winner, BlockSize); err != nil {
		t.Fatal(err)
	}
	env.write(winner)

	if _, err := env.mgr.AllocateBlocks(env.ctx, loser, 4, 1); err != nil {
		t.Fatal(err)
	}
	if !loser.HasExtentTree() {
		t.Fatal("third extent should spill into a tree")
	}
	if err := env.tbl.Write(env.ctx, loser); !errors.Is(err, ErrTreeConflict) {
		t.Fatalf("stale inline spill: got %v, want ErrTreeConflict", err)
	}
}

// Blocks released while a never-committed tree existed stay pending when the
// map folds back inline, and are freed once the inode is written.
func TestFoldOfUncommittedTreeKeepsReleases(t *testing.T) {
	env := newTreeEnv(t, 0)
	free := env.bm.FreeCount()
	ino := env.newInode()
	for _, lb := range []uint64{0, 2} {
		if _, err := env.mgr.AllocateBlocks(env.ctx, ino, lb, 1); err != nil {
			t.Fatal(err)
		}
	}
	env.write(ino)

	for _, lb := range []uint64{4, 6} {
		if _, err := env.mgr.AllocateBlocks(env.ctx, ino, lb, 1); err != nil {
			t.Fatal(err)
		}
	}
	// The first shrink keeps a tree, so block 6 waits for the commit.
	if err := env.mgr.Truncate(env.ctx, ino, 5*BlockSize); err != nil {
		t.Fatal(err)
	}
	if !ino.HasExtentTree() {
		t.Fatal("3 extents should still use a tree")
	}
	if err := env.mgr.Truncate(env.ctx, ino, BlockSize); err != nil {
		t.Fatal(err)
	}
	if ino.HasExtentTree() || ino.PendingTree == nil {
		t.Fatalf("want an inline map with releases still pending, tree=%v pending=%v", ino.HasExtentTree(), ino.PendingTree != nil)
	}
	env.write(ino)
	if got := env.bm.FreeCount(); got != free-1 {
		t.Fatalf("free blocks = %d after commit, want %d (only block 0 still mapped)", got, free-1)
	}
}
