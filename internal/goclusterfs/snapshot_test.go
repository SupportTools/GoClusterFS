package goclusterfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	snapFSBitmapOffset   = 1 << 20
	snapFSInodeOffset    = 2 << 20
	snapFSRefcountOffset = 3 << 20
	snapFSRegistryOffset = 3<<20 + 64<<10
	snapFSRegistrySize   = 64 << 10
	snapFSDataOffset     = 4 << 20
	snapFSDataBlocks     = 60
)

func newSnapshotFSImage(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fs.img")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, snapFSDataOffset+snapFSDataBlocks*BlockSize); err != nil {
		t.Fatal(err)
	}
	return path
}

// openSnapshotFS mounts an image with snapshots enabled, as a fresh node would.
func openSnapshotFS(t *testing.T, path string) *FS {
	t.Helper()
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { dev.Close() })

	var nodeUUID [16]byte
	copy(nodeUUID[:], "testnode12345678")
	bm := bitmap.NewRegion(dev, snapFSBitmapOffset, snapFSDataBlocks, nil, nodeUUID)
	inodeTbl := inode.NewTable(dev, snapFSInodeOffset, 4096*inode.InodeSize, nil, nodeUUID)
	extentMgr := extent.NewManager(dev, bm, nil, snapFSDataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)
	refs := refcount.NewRegion(dev, snapFSRefcountOffset, snapFSDataBlocks, nil)
	dirMgr := dir.NewManager(dev, inodeTbl, extentMgr, nil, snapFSDataOffset)
	fsys := NewWithComponents(dev, bm, inodeTbl, extentMgr, dirMgr, nil, nil, nil, nil, refs, nodeUUID, snapFSDataOffset)
	if err := fsys.InitSnapshots(context.Background(), snapFSRegistryOffset, snapFSRegistrySize, nil); err != nil {
		t.Fatalf("InitSnapshots: %v", err)
	}
	t.Cleanup(fsys.snapshotWkr.Stop)
	return fsys
}

func setxattr(t *testing.T, f *File, attr, value string) {
	t.Helper()
	if errno := f.Setxattr(context.Background(), attr, []byte(value), 0); errno != 0 {
		t.Fatalf("setxattr %s=%q on inode %d: %v", attr, value, f.inodeNum, errno)
	}
}

func lookupFile(t *testing.T, fsys *FS, name string) *File {
	t.Helper()
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	num, _, err := fsys.dirMgr.Lookup(context.Background(), root, name)
	if err != nil {
		t.Fatalf("lookup %s: %v", name, err)
	}
	return &File{fs: fsys, inodeNum: uint32(num)}
}

func truncateFile(t *testing.T, f *File, size uint64) {
	t.Helper()
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE
	in.Size = size
	if errno := f.Setattr(context.Background(), nil, in, &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("truncate inode %d to %d: %v", f.inodeNum, size, errno)
	}
}

// checkFile compares the whole file, including its size, with want.
func checkFile(t *testing.T, f *File, want []byte) {
	t.Helper()
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if ino.Size != uint64(len(want)) {
		t.Fatalf("inode %d: size %d, want %d", f.inodeNum, ino.Size, len(want))
	}
	checkAt(t, f, want, 0)
}

func overwritten(base []byte, off int, data []byte) []byte {
	out := bytes.Clone(base)
	copy(out[off:], data)
	return out
}

func TestSnapshotKeepsDataThroughWrites(t *testing.T) {
	path := newSnapshotFSImage(t)
	fsys := openSnapshotFS(t, path)
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatalf("InitRoot: %v", err)
	}
	original := pattern(1, 3*BlockSize)

	t.Run("SnapshotSurvivesWrites", func(t *testing.T) {
		f := newRegularFile(t, fsys)
		writeAt(t, f, original, 0)
		setxattr(t, f, "user.snapshot", "before")

		writeAt(t, f, pattern(2, 4096), BlockSize+100)
		writeAt(t, f, pattern(3, 2*BlockSize), 3*BlockSize)
		truncateFile(t, f, BlockSize+BlockSize/2)
		setxattr(t, f, "user.snapshot.clone", "before:/before-clone")

		live := overwritten(original[:BlockSize+BlockSize/2], BlockSize+100, pattern(2, 4096))
		for _, fsys := range []*FS{fsys, openSnapshotFS(t, path)} {
			checkFile(t, lookupFile(t, fsys, "before-clone"), original)
			checkFile(t, &File{fs: fsys, inodeNum: f.inodeNum}, live)
		}
	})

	t.Run("CloneWritesStayInClone", func(t *testing.T) {
		src := newRegularFile(t, fsys)
		writeAt(t, src, original[:2*BlockSize], 0)
		setxattr(t, src, "user.clone", "/clone")
		clone := lookupFile(t, fsys, "clone")

		writeAt(t, clone, pattern(4, 100), 10)
		writeAt(t, src, pattern(5, 100), BlockSize+10)

		for _, fsys := range []*FS{fsys, openSnapshotFS(t, path)} {
			checkFile(t, &File{fs: fsys, inodeNum: src.inodeNum}, overwritten(original[:2*BlockSize], BlockSize+10, pattern(5, 100)))
			checkFile(t, &File{fs: fsys, inodeNum: clone.inodeNum}, overwritten(original[:2*BlockSize], 10, pattern(4, 100)))
		}
	})

	// Two blocks keep the copied block's split extents inline, so no tree node is allocated.
	t.Run("DeleteAfterPartialOverwrite", func(t *testing.T) {
		f := newRegularFile(t, fsys)
		writeAt(t, f, original[:2*BlockSize], 0)
		free := fsys.bitmapMgr.FreeCount()
		setxattr(t, f, "user.snapshot", "partial")

		writeAt(t, f, pattern(7, 4096), BlockSize+10)
		if got := fsys.bitmapMgr.FreeCount(); got != free-1 {
			t.Fatalf("free blocks after a write into 1 shared block = %d, want %d", got, free-1)
		}
		if errno := f.Removexattr(ctx, "user.snapshot.partial"); errno != 0 {
			t.Fatalf("delete snapshot: %v", errno)
		}
		waitSnapshotGone(t, fsys, "partial")
		if got := fsys.bitmapMgr.FreeCount(); got != free {
			t.Fatalf("free blocks after deleting the snapshot = %d, want %d", got, free)
		}
		checkFile(t, f, overwritten(original[:2*BlockSize], BlockSize+10, pattern(7, 4096)))
	})

	t.Run("DeleteAfterOverwriteFreesSnapshotBlocks", func(t *testing.T) {
		f := newRegularFile(t, fsys)
		writeAt(t, f, original[:2*BlockSize], 0)
		free := fsys.bitmapMgr.FreeCount()
		setxattr(t, f, "user.snapshot", "temp")

		writeAt(t, f, pattern(6, 2*BlockSize), 0)
		if got := fsys.bitmapMgr.FreeCount(); got != free-2 {
			t.Fatalf("free blocks after overwriting 2 shared blocks = %d, want %d", got, free-2)
		}

		if errno := f.Removexattr(ctx, "user.snapshot.temp"); errno != 0 {
			t.Fatalf("delete snapshot: %v", errno)
		}
		waitSnapshotGone(t, fsys, "temp")
		if got := fsys.bitmapMgr.FreeCount(); got != free {
			t.Fatalf("free blocks after deleting the snapshot = %d, want %d", got, free)
		}
		checkFile(t, f, pattern(6, 2*BlockSize))
	})
}

func waitSnapshotGone(t *testing.T, fsys *FS, name string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := fsys.snapshotReg.GetByName(context.Background(), name)
		if errors.Is(err, snapshot.ErrSnapshotNotFound) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot %q still listed after 10s: %v", name, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func physicalBlock(t *testing.T, fsys *FS, num uint32, logical uint64) uint64 {
	t.Helper()
	ino, err := fsys.inodeTbl.Read(num)
	if err != nil {
		t.Fatal(err)
	}
	ext, off, err := fsys.extentMgr.Lookup(ino, logical)
	if err != nil {
		t.Fatal(err)
	}
	return ext.PhysicalBlock + off
}

func refcountOf(t *testing.T, fsys *FS, block uint64) uint32 {
	t.Helper()
	e, err := fsys.refcountReg.Get(context.Background(), block)
	if err != nil {
		t.Fatal(err)
	}
	return e.RefCount
}

// Older versions shared blocks without counting the owners, so this version
// refuses to share them again.
func TestSnapshotRefusesUncountedSharing(t *testing.T) {
	fsys := openSnapshotFS(t, newSnapshotFSImage(t))
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatal(err)
	}
	f := newRegularFile(t, fsys)
	writeAt(t, f, pattern(1, BlockSize), 0)
	ino, err := fsys.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	ino.InlineExtents[0].Flags |= extent.FlagShared
	if err := fsys.inodeTbl.Write(ctx, ino); err != nil {
		t.Fatal(err)
	}
	usedInodes := func() int {
		n := 0
		for num := uint32(inode.InodeFirstUsable); num < 64; num++ {
			if ino, err := fsys.inodeTbl.Read(num); err == nil && ino.Magic != 0 {
				n++
			}
		}
		return n
	}
	used := usedInodes()

	if errno := f.Setxattr(ctx, "user.snapshot", []byte("legacy"), 0); errno != syscall.ENOTSUP {
		t.Fatalf("snapshot of a file with uncounted sharing: errno %v, want ENOTSUP", errno)
	}
	if errno := f.Setxattr(ctx, "user.clone", []byte("/legacy-clone"), 0); errno != syscall.ENOTSUP {
		t.Fatalf("clone of a file with uncounted sharing: errno %v, want ENOTSUP", errno)
	}
	waitSnapshotGone(t, fsys, "legacy")
	// The delete job frees the data inode right after removing the descriptor.
	deadline := time.Now().Add(10 * time.Second)
	for usedInodes() != used {
		if time.Now().After(deadline) {
			t.Fatalf("refused snapshot and clone left %d inodes in use, want %d", usedInodes(), used)
		}
		time.Sleep(10 * time.Millisecond)
	}
	checkFile(t, f, pattern(1, BlockSize))
}

// A new mount cleans up a snapshot a crash left half created or half deleted,
// and the file keeps its blocks.
func TestSnapshotRecoveryAfterCrash(t *testing.T) {
	path := newSnapshotFSImage(t)
	fsys := openSnapshotFS(t, path)
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatal(err)
	}
	content := pattern(3, 2*BlockSize)
	f := newRegularFile(t, fsys)
	writeAt(t, f, content, 0)
	blocks := []uint64{physicalBlock(t, fsys, f.inodeNum, 0), physicalBlock(t, fsys, f.inodeNum, 1)}

	// Crash after the blocks were shared, before the snapshot became active.
	data, err := fsys.claimInode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data.Mode = inode.S_IFREG | 0o644
	data.Flags |= inode.FlagSnapshotData
	if err := fsys.inodeTbl.Write(ctx, data); err != nil {
		t.Fatal(err)
	}
	if _, err := fsys.snapshotReg.Create(ctx, "half-created", f.inodeNum, 0, data.InodeNum, 2, uint64(len(content)), snapshot.FlagReadOnly); err != nil {
		t.Fatal(err)
	}
	if err := fsys.shareInto(ctx, f.inodeNum, data, nil); err != nil {
		t.Fatal(err)
	}

	// Crash after the data inode gave back its blocks, before the descriptor went away.
	setxattr(t, f, "user.snapshot", "half-deleted")
	deleting, err := fsys.snapshotReg.GetByName(ctx, "half-deleted")
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.snapshotReg.Delete(ctx, deleting.SnapshotID); err != nil {
		t.Fatal(err)
	}
	if err := fsys.snapshotWkr.ReleaseData(ctx, deleting.DataInode); err != nil {
		t.Fatal(err)
	}

	remounted := openSnapshotFS(t, path)
	waitSnapshotGone(t, remounted, "half-created")
	waitSnapshotGone(t, remounted, "half-deleted")
	for _, num := range []uint32{data.InodeNum, deleting.DataInode} {
		if ino, err := remounted.inodeTbl.Read(num); err != nil || ino.Magic != 0 {
			t.Errorf("data inode %d still in use after recovery (err=%v)", num, err)
		}
	}
	for _, b := range blocks {
		if got := refcountOf(t, remounted, b); got > 1 {
			t.Errorf("block %d refcount %d after recovery, want at most 1", b, got)
		}
	}

	// With no other owner left, a write goes in place.
	g := &File{fs: remounted, inodeNum: f.inodeNum}
	writeAt(t, g, pattern(4, 100), 10)
	if got := physicalBlock(t, remounted, f.inodeNum, 0); got != blocks[0] {
		t.Errorf("write after recovery moved block 0 from %d to %d", blocks[0], got)
	}
	checkFile(t, g, overwritten(content, 10, pattern(4, 100)))
}

// Two nodes write into the same shared block at once. Node A commits its copy
// first; node B's commit then fails, B frees only its own copy and retries,
// and the old block loses exactly one owner.
func TestConcurrentCopyOnWriteAcrossNodes(t *testing.T) {
	path := newSnapshotFSImage(t)
	nodeA := openSnapshotFS(t, path)
	if err := nodeA.InitRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	original := pattern(1, BlockSize)
	f := newRegularFile(t, nodeA)
	writeAt(t, f, original, 0)
	setxattr(t, f, "user.snapshot", "race")
	old := physicalBlock(t, nodeA, f.inodeNum, 0)

	nodeB := openSnapshotFS(t, path)
	t.Cleanup(func() { beforeCoWCommit = nil })
	beforeCoWCommit = func() {
		beforeCoWCommit = nil
		writeAt(t, f, pattern(2, 100), 0)
	}
	writeAt(t, &File{fs: nodeB, inodeNum: f.inodeNum}, pattern(3, 100), 200)
	if beforeCoWCommit != nil {
		t.Fatal("node B's write never reached a copy-on-write commit")
	}

	after := openSnapshotFS(t, path)
	if got := refcountOf(t, after, old); got != 1 {
		t.Fatalf("old block refcount %d, want 1: the snapshot is its only owner", got)
	}
	current := physicalBlock(t, after, f.inodeNum, 0)
	allocated := 0
	for b := uint64(0); b < snapFSDataBlocks; b++ {
		if ok, err := after.bitmapMgr.IsAllocated(b); err != nil {
			t.Fatal(err)
		} else if ok {
			allocated++
		}
	}
	// The root directory, the snapshot's old block and the file's copy; node B's copy is gone.
	if allocated != 3 || current == old {
		t.Fatalf("%d blocks allocated with the file on block %d (old %d); want 3 and a copy", allocated, current, old)
	}
	want := overwritten(overwritten(original, 0, pattern(2, 100)), 200, pattern(3, 100))
	checkFile(t, &File{fs: after, inodeNum: f.inodeNum}, want)
	setxattr(t, &File{fs: after, inodeNum: f.inodeNum}, "user.snapshot.clone", "race:/race-clone")
	checkFile(t, lookupFile(t, after, "race-clone"), original)
}

// An image written by an older version, with an old snapshot and an old clone
// of one file. Writes, then deleting the clone and the source, never free a
// block another inode still maps; the blocks leak instead.
func TestOldSnapshotAndCloneNeverFreeSharedBlocks(t *testing.T) {
	fsys := openSnapshotFS(t, newSnapshotFSImage(t))
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatal(err)
	}
	src := newRegularFile(t, fsys)
	writeAt(t, src, pattern(5, 2*BlockSize), 0)

	// What older versions left: blocks marked shared without FlagCounted, a
	// refcount of 1 from the snapshot job, a clone of the same blocks whose
	// refcount job never ran, and a snapshot descriptor without data.
	srcIno, err := fsys.inodeTbl.Read(src.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	for i := range srcIno.InlineExtents {
		if srcIno.InlineExtents[i].Length > 0 {
			srcIno.InlineExtents[i].Flags |= extent.FlagShared
		}
	}
	if err := fsys.inodeTbl.Write(ctx, srcIno); err != nil {
		t.Fatal(err)
	}
	blocks, err := fsys.extentMgr.DataBlocks(srcIno)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fsys.refcountReg.IncrementBatch(ctx, blocks); err != nil {
		t.Fatal(err)
	}
	clone := newRegularFile(t, fsys)
	cloneIno, err := fsys.inodeTbl.Read(clone.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	cloneIno.InlineExtents = srcIno.InlineExtents
	cloneIno.ExtentCount = srcIno.ExtentCount
	cloneIno.Size, cloneIno.AllocatedBlocks, cloneIno.WrittenBlocks = srcIno.Size, srcIno.AllocatedBlocks, srcIno.WrittenBlocks
	if err := fsys.inodeTbl.Write(ctx, cloneIno); err != nil {
		t.Fatal(err)
	}
	old, err := fsys.snapshotReg.Create(ctx, "old-snap", src.inodeNum, 0, 0, 2, srcIno.Size, snapshot.FlagReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.snapshotReg.Activate(ctx, old.SnapshotID); err != nil {
		t.Fatal(err)
	}
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	for name, num := range map[string]uint32{"src.bin": src.inodeNum, "clone.bin": clone.inodeNum} {
		if err := fsys.dirMgr.Add(ctx, root, name, uint64(num), dir.DT_REG); err != nil {
			t.Fatal(err)
		}
	}
	stillAllocated := func(step string) {
		t.Helper()
		for _, b := range blocks {
			if ok, err := fsys.bitmapMgr.IsAllocated(b); err != nil || !ok {
				t.Fatalf("after %s, shared block %d is free (err=%v)", step, b, err)
			}
		}
	}

	writeAt(t, clone, pattern(6, 100), 10)
	stillAllocated("writing the old clone")
	if errno := src.Setxattr(ctx, "user.snapshot.clone", []byte("old-snap:/x"), 0); errno != syscall.ENOTSUP {
		t.Fatalf("clone of a snapshot without data: errno %v, want ENOTSUP", errno)
	}
	if errno := src.Removexattr(ctx, "user.snapshot.old-snap"); errno != 0 {
		t.Fatalf("delete old snapshot: %v", errno)
	}
	waitSnapshotGone(t, fsys, "old-snap")
	stillAllocated("deleting the old snapshot")
	if errno := fsys.Unlink(ctx, "clone.bin"); errno != 0 {
		t.Fatalf("unlink old clone: %v", errno)
	}
	stillAllocated("deleting the old clone")
	if errno := fsys.Unlink(ctx, "src.bin"); errno != 0 {
		t.Fatalf("unlink source: %v", errno)
	}
	stillAllocated("deleting the source")
}

// On an older version's image, file F had two snapshots (refcount 2) and then
// two clones whose owners were never counted. Copying on write must leave the
// old block to the other clone instead of releasing it.
func TestOldSharingCopiesWithoutReleasing(t *testing.T) {
	fsys := openSnapshotFS(t, newSnapshotFSImage(t))
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatal(err)
	}
	original := pattern(8, BlockSize)
	f := newRegularFile(t, fsys)
	writeAt(t, f, original, 0)
	fIno, err := fsys.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	fIno.InlineExtents[0].Flags |= extent.FlagShared
	if err := fsys.inodeTbl.Write(ctx, fIno); err != nil {
		t.Fatal(err)
	}
	old := physicalBlock(t, fsys, f.inodeNum, 0)
	if err := fsys.refcountReg.Set(ctx, old, 2, refcount.FlagNone); err != nil {
		t.Fatal(err)
	}
	var clones []*File
	for range 2 {
		c := newRegularFile(t, fsys)
		cIno, err := fsys.inodeTbl.Read(c.inodeNum)
		if err != nil {
			t.Fatal(err)
		}
		cIno.InlineExtents, cIno.ExtentCount = fIno.InlineExtents, fIno.ExtentCount
		cIno.Size, cIno.AllocatedBlocks, cIno.WrittenBlocks = fIno.Size, fIno.AllocatedBlocks, fIno.WrittenBlocks
		if err := fsys.inodeTbl.Write(ctx, cIno); err != nil {
			t.Fatal(err)
		}
		clones = append(clones, c)
	}

	writeAt(t, f, pattern(9, 100), 10)
	writeAt(t, clones[0], pattern(10, 100), 20)

	if ok, err := fsys.bitmapMgr.IsAllocated(old); err != nil || !ok {
		t.Fatalf("old block %d freed while the second clone maps it (err=%v)", old, err)
	}
	if got := refcountOf(t, fsys, old); got != 2 {
		t.Fatalf("old block refcount %d, want 2: copies of uncounted blocks must not release them", got)
	}
	checkFile(t, clones[1], original)
	checkFile(t, f, overwritten(original, 10, pattern(9, 100)))
	checkFile(t, clones[0], overwritten(original, 20, pattern(10, 100)))
}
