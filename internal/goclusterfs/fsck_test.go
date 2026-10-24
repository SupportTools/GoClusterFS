package goclusterfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// newFormattedImage formats an image the way goclusterfs-mount -format does.
func newFormattedImage(t *testing.T) string {
	t.Helper()
	return newFormattedImageOfSize(t, superblock.MinDeviceSize+64<<20)
}

func newFormattedImageOfSize(t *testing.T, size int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fs.img")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, size); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()
	if _, err := superblock.Format(dev, 0); err != nil {
		t.Fatalf("Format: %v", err)
	}
	fsys := openFormattedFS(t, path)
	if err := fsys.InitRoot(context.Background()); err != nil {
		t.Fatalf("InitRoot: %v", err)
	}
	fsys.snapshotWkr.Stop()
	return path
}

// openFormattedFS opens a formatted image with locks and snapshots on, as
// goclusterfs-mount does.
func openFormattedFS(t *testing.T, path string) *FS {
	t.Helper()
	ctx := context.Background()
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dev.Close() })
	sb, err := superblock.Read(dev)
	if err != nil {
		t.Fatal(err)
	}

	slot := int(snapFSNodes.Add(1)-1) % 64
	var nodeUUID [16]byte
	copy(nodeUUID[:], "fscknode12345678")
	nodeUUID[15] = byte(slot)
	locks := lock.NewManager(dev, sb.LockBitmapOffset, sb.LockBitmapSize, nodeUUID, slot)
	bm := bitmap.NewRegion(dev, sb.BlockBitmapOffset, sb.TotalBlocks, locks, nodeUUID)
	inodeTbl := inode.NewTable(dev, sb.InodeTableOffset, sb.InodeCount*inode.InodeSize, locks, nodeUUID)
	extentMgr := extent.NewManager(dev, bm, locks, sb.DataRegionOffset)
	inodeTbl.SetTreeFlusher(extentMgr)
	refs := refcount.NewRegion(dev, sb.RefCountTableOffset, sb.RefCountTableSize, sb.TotalBlocks, locks)
	dirMgr := dir.NewManager(dev, inodeTbl, extentMgr, locks, sb.DataRegionOffset)
	fsys := NewWithComponents(dev, bm, inodeTbl, extentMgr, dirMgr, nil, nil, nil, nil, refs, locks, nodeUUID, sb.DataRegionOffset)
	if err := fsys.InitSnapshots(ctx, sb.SnapshotRegOffset, sb.SnapshotRegSize, locks, slot); err != nil {
		t.Fatalf("InitSnapshots: %v", err)
	}
	t.Cleanup(fsys.snapshotWkr.Stop)
	return fsys
}

// createFile adds a regular file to the root directory as Create does.
func createFile(t *testing.T, fsys *FS, name string) *File {
	t.Helper()
	ctx := context.Background()
	f := newRegularFile(t, fsys)
	if _, err := fsys.updateInode(ctx, f.inodeNum, func(ino *inode.Inode) error {
		ino.LinkCount = 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.dirMgr.Add(ctx, root, name, uint64(f.inodeNum), dir.DT_REG); err != nil {
		t.Fatalf("add %s: %v", name, err)
	}
	return f
}

// runFsck stops fsys's background work and checks its image.
func runFsck(t *testing.T, fsys *FS, opts fsck.Options) *fsck.Result {
	t.Helper()
	fsys.snapshotWkr.Stop()
	opts.Output = &bytes.Buffer{}
	if opts.SkipPhases == nil {
		opts.SkipPhases = map[fsck.Phase]bool{}
	}
	return fsck.New(fsys.dev, opts).Check()
}

func problemList(r *fsck.Result) string {
	var b strings.Builder
	for _, p := range r.Problems {
		b.WriteString("\n  " + p.String())
	}
	return b.String()
}

// Everything the runtime writes, including snapshots, clones, copies on
// write and extent trees, must pass fsck without a single problem.
func TestFsckAcceptsRuntimeImage(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	ctx := context.Background()

	small := createFile(t, fsys, "small.txt")
	writeAt(t, small, pattern(1, 100), 0)
	big := createFile(t, fsys, "big.bin")
	writeAt(t, big, pattern(2, 3*BlockSize), 0)
	sparse := createFile(t, fsys, "sparse.bin")
	for i := int64(0); i < 5; i++ {
		writeAt(t, sparse, pattern(byte(10+i), 4096), 2*i*BlockSize)
	}

	setxattr(t, big, "user.snapshot", "s1")
	writeAt(t, big, pattern(3, 100), 10)
	setxattr(t, big, "user.clone", "/big-clone.bin")
	writeAt(t, lookupFile(t, fsys, "big-clone.bin"), pattern(4, 100), BlockSize+10)
	setxattr(t, big, "user.snapshot.clone", "s1:/from-s1.bin")
	setxattr(t, sparse, "user.snapshot", "s2")
	writeAt(t, sparse, pattern(5, 4096), 4*BlockSize)
	if errno := sparse.Removexattr(ctx, "user.snapshot.s2"); errno != 0 {
		t.Fatalf("delete snapshot: %v", errno)
	}
	waitSnapshotGone(t, fsys, "s2")
	if errno := fsys.Unlink(ctx, "small.txt"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	if ino, err := fsys.inodeTbl.Read(sparse.inodeNum); err != nil || !ino.HasExtentTree() {
		t.Fatalf("sparse.bin should use an extent tree (err=%v)", err)
	}

	r := runFsck(t, fsys, fsck.DefaultOptions())
	if len(r.Problems) > 0 || r.ExitCode() != fsck.ExitClean {
		t.Fatalf("fsck exit %d on a runtime image:%s", r.ExitCode(), problemList(r))
	}
}

// shareTreeLikeOlderVersions marks every extent of a depth-1 tree inode shared
// without FlagCounted, as versions before counted sharing did.
func shareTreeLikeOlderVersions(t *testing.T, fsys *FS, ino *inode.Inode) {
	t.Helper()
	if !ino.HasExtentTree() || ino.ExtentDepth != 1 {
		t.Fatalf("inode %d: want a depth-1 extent tree", ino.InodeNum)
	}
	off := extent.TreeNodeOffset(fsys.dataOffset, ino.ExtentTreeRoot, ino.ExtentTreeGen)
	buf := storage.NewAlignedBuffer(64 << 10).Bytes()
	if _, err := fsys.dev.ReadAt(buf, off); err != nil {
		t.Fatal(err)
	}
	const header, entry = 64, 32
	count := int(binary.LittleEndian.Uint32(buf[12:16]))
	for i := 0; i < count; i++ {
		flags := buf[header+i*entry+24:]
		binary.LittleEndian.PutUint32(flags, binary.LittleEndian.Uint32(flags)|extent.FlagShared)
	}
	crc := storage.CRC32CUpdate(storage.CRC32C(buf[:60]), buf[header:header+count*entry])
	binary.LittleEndian.PutUint32(buf[60:64], crc)
	if _, err := fsys.dev.WriteAt(buf, off); err != nil {
		t.Fatal(err)
	}
}

// Images from before counted sharing refuse snapshots and clones of shared
// files until fsck -repair rebuilds the refcounts and marks the sharing
// counted. Dry runs change nothing.
func TestFsckRepairsOlderSharing(t *testing.T) {
	path := newFormattedImage(t)
	fsys := openFormattedFS(t, path)
	ctx := context.Background()

	src := createFile(t, fsys, "src.bin")
	srcData := pattern(1, 2*BlockSize)
	writeAt(t, src, srcData, 0)
	tree := createFile(t, fsys, "tree.bin")
	for i := int64(0); i < 5; i++ {
		writeAt(t, tree, pattern(byte(20+i), 4096), 2*i*BlockSize)
	}

	// An old snapshot marked src shared and an old clone copied its extents;
	// neither counted the owners. Another old clone shares the tree's first block.
	srcIno, err := fsys.updateInode(ctx, src.inodeNum, func(ino *inode.Inode) error {
		for i := range ino.InlineExtents {
			if ino.InlineExtents[i].Length > 0 {
				ino.InlineExtents[i].Flags |= extent.FlagShared
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	clone := createFile(t, fsys, "old-clone.bin")
	if _, err := fsys.updateInode(ctx, clone.inodeNum, func(ino *inode.Inode) error {
		ino.InlineExtents, ino.ExtentCount = srcIno.InlineExtents, srcIno.ExtentCount
		ino.Size, ino.AllocatedBlocks, ino.WrittenBlocks = srcIno.Size, srcIno.AllocatedBlocks, srcIno.WrittenBlocks
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	treeIno, err := fsys.inodeTbl.Read(tree.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	treeBlock, err := fsys.extentMgr.LookupPhysical(treeIno, 0)
	if err != nil {
		t.Fatal(err)
	}
	treeClone := createFile(t, fsys, "old-tree-clone.bin")
	if _, err := fsys.updateInode(ctx, treeClone.inodeNum, func(ino *inode.Inode) error {
		ino.InlineExtents[0] = inode.InlineExtent{PhysicalBlock: treeBlock, Length: 1, Flags: extent.FlagAllocated | extent.FlagShared}
		ino.ExtentCount, ino.Size, ino.AllocatedBlocks, ino.WrittenBlocks = 1, 4096, 1, 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	fsys.snapshotWkr.Stop()
	shareTreeLikeOlderVersions(t, fsys, treeIno)

	fsys = openFormattedFS(t, path)
	for _, f := range []*File{{fs: fsys, inodeNum: src.inodeNum}, {fs: fsys, inodeNum: tree.inodeNum}} {
		if errno := f.Setxattr(ctx, "user.snapshot", []byte("refused"), 0); errno != syscall.ENOTSUP {
			t.Fatalf("snapshot of inode %d before repair: errno %v, want ENOTSUP", f.inodeNum, errno)
		}
		waitSnapshotGone(t, fsys, "refused")
	}

	check := runFsck(t, fsys, fsck.DefaultOptions())
	if check.ExitCode() != fsck.ExitUncorrected || !strings.Contains(problemList(check), "older versions") {
		t.Fatalf("check exit %d, want %d with older sharing reported:%s", check.ExitCode(), fsck.ExitUncorrected, problemList(check))
	}

	dry := fsck.DefaultOptions()
	dry.Mode, dry.DryRun = fsck.Repair, true
	if r := runFsck(t, fsys, dry); r.ExitCode() != fsck.ExitUncorrected || r.RepairedCount != 0 {
		t.Fatalf("dry run: exit %d, repaired %d, want exit %d and nothing repaired:%s",
			r.ExitCode(), r.RepairedCount, fsck.ExitUncorrected, problemList(r))
	}
	if again := runFsck(t, fsys, fsck.DefaultOptions()); problemList(again) != problemList(check) {
		t.Fatalf("dry run changed the image: before%s\nafter%s", problemList(check), problemList(again))
	}

	repair := fsck.DefaultOptions()
	repair.Mode = fsck.Repair
	if r := runFsck(t, fsys, repair); r.ExitCode() != fsck.ExitCorrected {
		t.Fatalf("repair exit %d, want %d:%s", r.ExitCode(), fsck.ExitCorrected, problemList(r))
	}
	if r := runFsck(t, fsys, fsck.DefaultOptions()); len(r.Problems) > 0 {
		t.Fatalf("problems left after repair:%s", problemList(r))
	}

	fsys = openFormattedFS(t, path)
	src, tree = &File{fs: fsys, inodeNum: src.inodeNum}, &File{fs: fsys, inodeNum: tree.inodeNum}
	setxattr(t, src, "user.snapshot", "after-repair")
	setxattr(t, tree, "user.clone", "/tree-clone.bin")
	srcBlocks, err := fsys.extentMgr.DataBlocks(srcIno)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range srcBlocks {
		if got := refcountOf(t, fsys, b); got != 3 {
			t.Fatalf("block %d: refcount %d with src, an old clone and a snapshot as owners, want 3", b, got)
		}
	}
	writeAt(t, &File{fs: fsys, inodeNum: clone.inodeNum}, pattern(7, 100), 10)
	writeAt(t, src, pattern(8, 100), BlockSize+10)
	checkFile(t, &File{fs: fsys, inodeNum: clone.inodeNum}, overwritten(srcData, 10, pattern(7, 100)))
	checkFile(t, src, overwritten(srcData, BlockSize+10, pattern(8, 100)))
	setxattr(t, src, "user.snapshot.clone", "after-repair:/snap-clone.bin")
	checkFile(t, lookupFile(t, fsys, "snap-clone.bin"), srcData)

	if r := runFsck(t, fsys, fsck.DefaultOptions()); len(r.Problems) > 0 {
		t.Fatalf("problems after using the repaired image:%s", problemList(r))
	}
}

// On a 200 GiB volume the 1 MB refcount table has entries for blocks 0-130815
// only. Several inodes counting on a share of a higher block lose data when
// one is deleted, and marking older sharing of such blocks counted would
// cause the same, so repair leaves that sharing alone.
func TestFsckReportsSharingPastRefcountTable(t *testing.T) {
	path := newFormattedImageOfSize(t, 200<<30)
	fsys := openFormattedFS(t, path)
	ctx := context.Background()
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	covered := sb.RefCountTableSize / 4096 * 511
	counted, older := covered+5, covered+9

	share := func(name string, block uint64, flags uint32) *File {
		f := createFile(t, fsys, name)
		if _, err := fsys.updateInode(ctx, f.inodeNum, func(ino *inode.Inode) error {
			ino.InlineExtents[0] = inode.InlineExtent{PhysicalBlock: block, Length: 1, Flags: extent.FlagAllocated | flags}
			ino.ExtentCount, ino.Size, ino.AllocatedBlocks, ino.WrittenBlocks = 1, 4096, 1, 1
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return f
	}
	for _, block := range []uint64{counted, older} {
		if err := fsys.bitmapMgr.MarkAllocated(ctx, block); err != nil {
			t.Fatal(err)
		}
	}
	share("counted-a", counted, extent.FlagShared|extent.FlagCounted)
	share("counted-b", counted, extent.FlagShared|extent.FlagCounted)
	oldA := share("older-a", older, extent.FlagShared)
	share("older-b", older, extent.FlagShared)

	check := runFsck(t, fsys, fsck.DefaultOptions())
	problems := problemList(check)
	for _, want := range []string{
		fmt.Sprintf("has entries for blocks 0-%d of %d", covered-1, sb.TotalBlocks),
		fmt.Sprintf("block %d: mapped by 2 inodes but past the refcount table", counted),
		fmt.Sprintf("inode %d: shares blocks past the refcount table the way older versions did", oldA.inodeNum),
	} {
		if !strings.Contains(problems, want) {
			t.Errorf("fsck did not report %q:%s", want, problems)
		}
	}
	if strings.Contains(problems, fmt.Sprintf("block %d: mapped by", older)) {
		t.Errorf("block %d is only shared without counts, so it is never freed, yet fsck reported it:%s", older, problems)
	}
	if check.ExitCode() != fsck.ExitUncorrected {
		t.Errorf("check exit %d, want %d", check.ExitCode(), fsck.ExitUncorrected)
	}
	for _, p := range check.Problems {
		if p.Location == fmt.Sprintf("inode %d", oldA.inodeNum) && p.Severity != fsck.Error {
			t.Errorf("inode %d reported as %v, want an error: writes to it change older-b too", oldA.inodeNum, p.Severity)
		}
	}

	repair := fsck.DefaultOptions()
	repair.Mode = fsck.Repair
	runFsck(t, fsys, repair)
	ino, err := fsys.inodeTbl.Read(oldA.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if ino.InlineExtents[0].Flags&extent.FlagCounted != 0 {
		t.Fatalf("repair marked older sharing of block %d counted, so its first release would free it", older)
	}
}

// The runtime cannot read a refcount sector with a bad checksum. Repair
// rebuilds every count in it from the extents that map the blocks.
func TestFsckRebuildsCorruptRefcountSector(t *testing.T) {
	path := newFormattedImage(t)
	fsys := openFormattedFS(t, path)
	src := createFile(t, fsys, "src.bin")
	writeAt(t, src, pattern(1, 2*BlockSize), 0)
	setxattr(t, src, "user.clone", "/clone.bin")
	srcIno, err := fsys.inodeTbl.Read(src.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := fsys.extentMgr.DataBlocks(srcIno)
	if err != nil {
		t.Fatal(err)
	}
	fsys.snapshotWkr.Stop()

	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	garbage := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	for i := range garbage {
		garbage[i] = 0x5a
	}
	if _, err := fsys.dev.WriteAt(garbage, int64(sb.RefCountTableOffset)); err != nil {
		t.Fatal(err)
	}

	if r := runFsck(t, fsys, fsck.DefaultOptions()); r.ExitCode() != fsck.ExitUncorrected || !strings.Contains(problemList(r), "checksum mismatch") {
		t.Fatalf("check exit %d, want %d with a checksum mismatch:%s", r.ExitCode(), fsck.ExitUncorrected, problemList(r))
	}
	repair := fsck.DefaultOptions()
	repair.Mode = fsck.Repair
	if r := runFsck(t, fsys, repair); r.ExitCode() != fsck.ExitCorrected {
		t.Fatalf("repair exit %d, want %d:%s", r.ExitCode(), fsck.ExitCorrected, problemList(r))
	}
	if r := runFsck(t, fsys, fsck.DefaultOptions()); len(r.Problems) > 0 {
		t.Fatalf("problems left after repair:%s", problemList(r))
	}
	fsys = openFormattedFS(t, path)
	for _, b := range blocks {
		if got := refcountOf(t, fsys, b); got != 2 {
			t.Fatalf("block %d shared by a file and its clone: refcount %d after repair, want 2", b, got)
		}
	}
}
