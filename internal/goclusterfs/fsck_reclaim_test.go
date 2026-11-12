package goclusterfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/sfb"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// leakBlock allocates a block that no inode maps, as a crash between an allocation and its inode write leaves.
func leakBlock(t *testing.T, fsys *FS) {
	t.Helper()
	if _, err := fsys.bitmapMgr.Alloc(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
}

// deleteWithTimedOutFree removes name as Unlink does, but its block free gives
// up on a refcount lock another node holds, as it does after the 30s lock-wait
// limit. Unlink detaches from its caller's context, so it would wait the full 30s.
func deleteWithTimedOutFree(t *testing.T, fsys *FS, name string) {
	t.Helper()
	ctx := context.Background()
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	num, _, err := fsys.dirMgr.Lookup(ctx, root, name)
	if err != nil {
		t.Fatalf("lookup %s: %v", name, err)
	}
	if err := fsys.dirMgr.Remove(ctx, root, name); err != nil {
		t.Fatalf("remove %s: %v", name, err)
	}
	ino, err := fsys.inodeTbl.Read(uint32(num))
	if err != nil {
		t.Fatal(err)
	}

	slot := int(snapFSNodes.Add(1)-1) % 64
	var otherUUID [16]byte
	copy(otherUUID[:], "othernode1234567")
	otherUUID[15] = byte(slot)
	other := lock.NewManager(fsys.dev, sb.LockBitmapOffset, sb.LockBitmapSize, otherUUID, slot)
	if err := other.Acquire(ctx, lock.LockTypeRefCount, 0, lock.LockModeExclusive); err != nil {
		t.Fatal(err)
	}
	expired, cancel := context.WithCancel(ctx)
	cancel()
	if err := fsys.extentMgr.Delete(expired, ino); err != nil {
		t.Fatalf("delete %s: %v", name, err)
	}
	if err := other.Release(lock.LockTypeRefCount, 0); err != nil {
		t.Fatal(err)
	}
	if err := fsys.inodeTbl.Free(ctx, ino.InodeNum); err != nil {
		t.Fatal(err)
	}
}

func freeBlocksOnDisk(t *testing.T, path string) uint64 {
	t.Helper()
	return openFormattedFS(t, path).bitmapMgr.FreeCount()
}

// A write that fails after allocating, and frees that give up on a lock, leave
// blocks allocated that no file maps. Repair gives those back and leaves alone
// the blocks that files, snapshots and clones still share.
func TestFsckRepairReclaimsLeakedBlocks(t *testing.T) {
	path := newFormattedImage(t)
	fsys := openFormattedFS(t, path)
	ctx := context.Background()

	data := pattern(1, 3*BlockSize)
	src := createFile(t, fsys, "src.bin")
	writeAt(t, src, data, 0)
	setxattr(t, src, "user.snapshot", "before")
	setxattr(t, src, "user.clone", "/clone.bin")
	grow := createFile(t, fsys, "grow.bin")
	writeAt(t, grow, pattern(9, BlockSize), 0)
	victim := createFile(t, fsys, "victim.bin")
	writeAt(t, victim, pattern(2, 2*BlockSize), 0)
	pair := createFile(t, fsys, "pair-a.bin")
	writeAt(t, pair, pattern(3, 100), 0)
	setxattr(t, pair, "user.clone", "/pair-b.bin")
	pairBlock := physicalBlock(t, fsys, pair.inodeNum, 0)
	free := fsys.bitmapMgr.FreeCount()

	leakBlock(t, fsys)
	if got := fsys.bitmapMgr.FreeCount(); got != free-1 {
		t.Fatalf("leaked %d blocks, want 1", free-got)
	}
	deleteWithTimedOutFree(t, fsys, "victim.bin")
	deleteWithTimedOutFree(t, fsys, "clone.bin")
	deleteWithTimedOutFree(t, fsys, "pair-a.bin")
	deleteWithTimedOutFree(t, fsys, "pair-b.bin")

	check := runFsck(t, fsys, fsck.DefaultOptions())
	if !strings.Contains(problemList(check), "leaked: allocated in the bitmap but no inode maps it") ||
		!strings.Contains(problemList(check), "leaked: no inode maps it but refcount is 2") {
		t.Fatalf("check did not report the leaks:%s", problemList(check))
	}

	dry := fsck.DefaultOptions()
	dry.Mode, dry.DryRun = fsck.Repair, true
	if r := runFsck(t, fsys, dry); r.RepairedCount != 0 {
		t.Fatalf("dry run repaired %d problems", r.RepairedCount)
	}
	if got := freeBlocksOnDisk(t, path); got != free-1 {
		t.Fatalf("dry run changed the bitmap: %d free blocks, want %d", got, free-1)
	}

	repair := fsck.DefaultOptions()
	repair.Mode = fsck.Repair
	if r := runFsck(t, fsys, repair); r.ExitCode() != fsck.ExitCorrected {
		t.Fatalf("repair exit %d, want %d:%s", r.ExitCode(), fsck.ExitCorrected, problemList(r))
	}
	if r := runFsck(t, fsys, fsck.DefaultOptions()); len(r.Problems) > 0 {
		t.Fatalf("problems left after repair:%s", problemList(r))
	}
	if r := runFsck(t, fsys, repair); r.RepairedCount != 0 || len(r.Problems) > 0 {
		t.Fatalf("second repair changed %d things:%s", r.RepairedCount, problemList(r))
	}

	fsys = openFormattedFS(t, path)
	// The failed write's block, victim's 2 blocks and the pair's block are free again.
	if got, want := fsys.bitmapMgr.FreeCount(), free+3; got != want {
		t.Fatalf("%d free blocks after repair, want %d", got, want)
	}
	if got := refcountOf(t, fsys, pairBlock); got != 0 {
		t.Fatalf("freed block %d keeps refcount %d", pairBlock, got)
	}
	checkFile(t, &File{fs: fsys, inodeNum: grow.inodeNum}, pattern(9, BlockSize))
	src = &File{fs: fsys, inodeNum: src.inodeNum}
	checkFile(t, src, data)
	setxattr(t, src, "user.snapshot.clone", "before:/from-snapshot.bin")
	fromSnapshot := lookupFile(t, fsys, "from-snapshot.bin")
	checkFile(t, fromSnapshot, data)
	for logical := uint64(0); logical < 3; logical++ {
		block := physicalBlock(t, fsys, src.inodeNum, logical)
		if got := refcountOf(t, fsys, block); got != 3 {
			t.Fatalf("block %d: refcount %d with src, the snapshot and its clone as owners, want 3", block, got)
		}
	}
	writeAt(t, src, pattern(6, 100), 10)
	checkFile(t, fromSnapshot, data)
	if errno := fsys.Unlink(ctx, "from-snapshot.bin"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	if r := runFsck(t, fsys, fsck.DefaultOptions()); len(r.Problems) > 0 {
		t.Fatalf("problems after using the repaired image:%s", problemList(r))
	}
}

// Repair frees a leaked block only when it can see every reference that may
// map it, and never while a node may be using the filesystem. When it keeps the
// leaks it writes neither the bitmap nor their refcounts.
func TestFsckRepairKeepsLeaksItCannotPlace(t *testing.T) {
	repair := fsck.DefaultOptions()
	repair.Mode = fsck.Repair
	skip := func(p fsck.Phase) fsck.Options {
		opts := repair
		opts.SkipPhases = map[fsck.Phase]bool{p: true}
		return opts
	}

	for _, tc := range []struct {
		name   string
		damage func(t *testing.T, fsys *FS, sb *superblock.Superblock) fsck.Options
		reason string
	}{
		{"UnreadableInode", func(t *testing.T, fsys *FS, sb *superblock.Superblock) fsck.Options {
			other := createFile(t, fsys, "other.bin")
			writeAt(t, other, pattern(7, 100), 0)
			flipByte(t, fsys, int64(sb.InodeTableOffset)+int64(other.inodeNum)*inode.InodeSize+100)
			return repair
		}, "leaked blocks are kept, because these may map them: inode"},
		{"DamagedExtentTree", func(t *testing.T, fsys *FS, sb *superblock.Superblock) fsck.Options {
			sparse := createFile(t, fsys, "sparse.bin")
			for i := int64(0); i < 5; i++ {
				writeAt(t, sparse, pattern(byte(10+i), 4096), 2*i*BlockSize)
			}
			ino, err := fsys.inodeTbl.Read(sparse.inodeNum)
			if err != nil || !ino.HasExtentTree() {
				t.Fatalf("sparse.bin should use an extent tree (err=%v)", err)
			}
			flipByte(t, fsys, extent.TreeNodeOffset(fsys.dataOffset, ino.ExtentTreeRoot, ino.ExtentTreeGen)+100)
			return repair
		}, "extent tree"},
		{"SkippedInodePhase", func(t *testing.T, fsys *FS, sb *superblock.Superblock) fsck.Options {
			return skip(fsck.PhaseInodes)
		}, "Phase 3 (Inodes) did not finish"},
		{"SkippedBitmapPhase", func(t *testing.T, fsys *FS, sb *superblock.Superblock) fsck.Options {
			return skip(fsck.PhaseBitmap)
		}, "Phase 2 (Bitmap) did not finish"},
		{"BadBitmapSector", func(t *testing.T, fsys *FS, sb *superblock.Superblock) fsck.Options {
			// Past the bits of the image's blocks, so only the checksum breaks.
			flipByte(t, fsys, int64(sb.BlockBitmapOffset)+4000)
			return repair
		}, "their bitmap sectors have a bad checksum"},
		{"MountedNode", func(t *testing.T, fsys *FS, sb *superblock.Superblock) fsck.Options {
			reg := node.NewRegistry(fsys.dev, sb.NodeRegistryOffset)
			if _, err := reg.Register(context.Background(), "mounted"); err != nil {
				t.Fatal(err)
			}
			if err := reg.StartHeartbeat(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(reg.Stop)
			return repair
		}, "mounted by node"},
		{"DirtyJournalWithForce", func(t *testing.T, fsys *FS, sb *superblock.Superblock) fsck.Options {
			reg := node.NewRegistry(fsys.dev, sb.NodeRegistryOffset)
			slot, err := reg.Register(context.Background(), "crashed")
			if err != nil {
				t.Fatal(err)
			}
			// An hour-old heartbeat keeps fsck's watch for running nodes short.
			slotAt := int64(sb.NodeRegistryOffset) + int64(slot)*node.SlotSize
			buf := storage.NewAlignedBuffer(node.SlotSize)
			if _, err := fsys.dev.ReadAt(buf.Bytes(), slotAt); err != nil {
				t.Fatal(err)
			}
			binary.LittleEndian.PutUint64(buf.Bytes()[32:40], uint64(time.Now().Add(-time.Hour).Unix()))
			binary.LittleEndian.PutUint32(buf.Bytes()[node.SlotSize-4:], storage.CRC32C(buf.Bytes()[:node.SlotSize-4]))
			if _, err := fsys.dev.WriteAt(buf.Bytes(), slotAt); err != nil {
				t.Fatal(err)
			}
			jsb := journal.NewJournalSuperblock([16]byte{1}, 1, 1<<20)
			jsb.State = journal.JournalStateDirty
			buf = storage.NewAlignedBuffer(storage.SectorSize)
			copy(buf.Bytes(), jsb.Serialize())
			if _, err := fsys.dev.WriteAt(buf.Bytes(), int64(sb.JournalRegionOffset+uint64(slot)*sb.JournalSlotSize())); err != nil {
				t.Fatal(err)
			}
			force := repair
			force.AllowUncleanNodes = true
			return force
		}, "stopped with a dirty journal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := newFormattedImage(t)
			fsys := openFormattedFS(t, path)
			sb, err := superblock.Read(fsys.dev)
			if err != nil {
				t.Fatal(err)
			}
			// One leaked block with no count, and one a cloned pair left with a count of 2.
			createFile(t, fsys, "f.bin")
			leakBlock(t, fsys)
			pair := createFile(t, fsys, "pair-a.bin")
			writeAt(t, pair, pattern(9, 100), 0)
			setxattr(t, pair, "user.clone", "/pair-b.bin")
			deleteWithTimedOutFree(t, fsys, "pair-a.bin")
			deleteWithTimedOutFree(t, fsys, "pair-b.bin")
			opts := tc.damage(t, fsys, sb)
			bitmap := readRegion(t, fsys, sb.BlockBitmapOffset, sb.BlockBitmapSize)
			refcounts := readRegion(t, fsys, sb.RefCountTableOffset, sb.RefCountTableSize)

			if r := runFsck(t, fsys, fsck.DefaultOptions()); !strings.Contains(problemList(r), "leaked") {
				t.Fatalf("check did not report the leaked blocks:%s", problemList(r))
			}
			r := runFsck(t, fsys, opts)
			if text := r.AbortReason + problemList(r); !strings.Contains(text, tc.reason) {
				t.Fatalf("repair did not say %q:%s", tc.reason, text)
			}
			if !bytes.Equal(readRegion(t, fsys, sb.BlockBitmapOffset, sb.BlockBitmapSize), bitmap) {
				t.Fatal("repair changed the bitmap although it had to keep the leaked blocks")
			}
			if !bytes.Equal(readRegion(t, fsys, sb.RefCountTableOffset, sb.RefCountTableSize), refcounts) {
				t.Fatal("repair changed refcounts although it had to keep the leaked blocks")
			}
		})
	}
}

// The runtime maps every extent with a length: one without the allocated flag,
// and the part of one that runs past the device end. Repair must keep them.
func TestFsckRepairKeepsBlocksOfOddExtents(t *testing.T) {
	repair := fsck.DefaultOptions()
	repair.Mode = fsck.Repair
	ctx := context.Background()

	t.Run("WithoutAllocatedFlag", func(t *testing.T) {
		path := newFormattedImage(t)
		fsys := openFormattedFS(t, path)
		f := createFile(t, fsys, "noflag.bin")
		writeAt(t, f, pattern(4, 4096), 0)
		block := physicalBlock(t, fsys, f.inodeNum, 0)
		if _, err := fsys.updateInode(ctx, f.inodeNum, func(ino *inode.Inode) error {
			ino.InlineExtents[0].Flags = 0
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if got := physicalBlock(t, fsys, f.inodeNum, 0); got != block {
			t.Fatalf("runtime maps logical 0 to %d, want %d", got, block)
		}
		runFsck(t, fsys, repair)
		if allocated, _ := openFormattedFS(t, path).bitmapMgr.IsAllocated(block); !allocated {
			t.Fatalf("repair freed block %d, which the runtime still reads for noflag.bin", block)
		}
	})

	t.Run("PastDeviceEnd", func(t *testing.T) {
		path := newFormattedImage(t)
		fsys := openFormattedFS(t, path)
		sb, err := superblock.Read(fsys.dev)
		if err != nil {
			t.Fatal(err)
		}
		last := sb.TotalBlocks - 1
		if err := fsys.bitmapMgr.MarkAllocated(ctx, last); err != nil {
			t.Fatal(err)
		}
		f := createFile(t, fsys, "edge.bin")
		if _, err := fsys.updateInode(ctx, f.inodeNum, func(ino *inode.Inode) error {
			ino.InlineExtents[0] = inode.InlineExtent{PhysicalBlock: last, Length: 2, Flags: extent.FlagAllocated}
			ino.ExtentCount, ino.Size, ino.AllocatedBlocks, ino.WrittenBlocks = 1, 4096, 2, 2
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		runFsck(t, fsys, repair)
		if allocated, _ := openFormattedFS(t, path).bitmapMgr.IsAllocated(last); !allocated {
			t.Fatalf("repair freed block %d, which edge.bin still maps at logical 0", last)
		}
	})
}

func readRegion(t *testing.T, fsys *FS, off, size uint64) []byte {
	t.Helper()
	buf := storage.NewAlignedBuffer(int(size))
	if _, err := fsys.dev.ReadAt(buf.Bytes(), int64(off)); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func flipByte(t *testing.T, fsys *FS, off int64) {
	t.Helper()
	sector := off &^ (storage.SectorSize - 1)
	buf := storage.NewAlignedBuffer(storage.SectorSize)
	if _, err := fsys.dev.ReadAt(buf.Bytes(), sector); err != nil {
		t.Fatal(err)
	}
	buf.Bytes()[off-sector] ^= 0xFF
	if _, err := fsys.dev.WriteAt(buf.Bytes(), sector); err != nil {
		t.Fatal(err)
	}
}

// A file that grows out of its SFB slot moves to a full block and leaves its
// container empty. SFB allocation is disabled, so the empty container is
// leaked. Repair frees it and keeps a container that another file still uses.
func TestFsckRepairFreesEmptySFBContainers(t *testing.T) {
	path := newFormattedImage(t)
	fsys := openFormattedFS(t, path)
	ctx := context.Background()
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	link := func(f *File, name string) {
		if err := fsys.dirMgr.Add(ctx, root, name, uint64(f.inodeNum), dir.DT_REG); err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
	}
	kept, emptied := newSFBContainer(t, fsys, sfb.Tier16KB), newSFBContainer(t, fsys, sfb.Tier16KB)
	small := newSFBFile(t, fsys, kept, sfb.Tier16KB, 1, pattern(1, 5000))
	link(small, "small.txt")
	moved := newSFBFile(t, fsys, emptied, sfb.Tier16KB, 1, pattern(2, 10000))
	link(moved, "moved.txt")
	writeAt(t, moved, pattern(3, 10000), 10000)
	if sfbSlotUsed(t, fsys, emptied, 1) {
		t.Fatal("growing moved.txt past its slot did not free the slot")
	}
	free := fsys.bitmapMgr.FreeCount()

	check := runFsck(t, fsys, fsck.DefaultOptions())
	if problems := problemList(check); !strings.Contains(problems, fmt.Sprintf("block %d: leaked", emptied)) ||
		strings.Contains(problems, fmt.Sprintf("block %d: leaked", kept)) {
		t.Fatalf("check should report container %d as leaked and keep %d:%s", emptied, kept, problems)
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
	if got := fsys.bitmapMgr.FreeCount(); got != free+1 {
		t.Fatalf("%d free blocks after repair, want %d", got, free+1)
	}
	checkFile(t, lookupFile(t, fsys, "small.txt"), pattern(1, 5000))
	checkFile(t, lookupFile(t, fsys, "moved.txt"), append(pattern(2, 10000), pattern(3, 10000)...))
}

// holdRefcountLock makes another node hold the refcount lock until the test
// ends, so frees on fsys wait for it.
func holdRefcountLock(t *testing.T, fsys *FS) {
	t.Helper()
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	slot := int(snapFSNodes.Add(1)-1) % 64
	var otherUUID [16]byte
	copy(otherUUID[:], "lockholder123456")
	otherUUID[15] = byte(slot)
	other := lock.NewManager(fsys.dev, sb.LockBitmapOffset, sb.LockBitmapSize, otherUUID, slot)
	if err := other.Acquire(context.Background(), lock.LockTypeRefCount, 0, lock.LockModeExclusive); err != nil {
		t.Fatal(err)
	}
}

func allocatedBlocks(t *testing.T, fsys *FS) map[uint64]bool {
	t.Helper()
	out := map[uint64]bool{}
	for b := uint64(0); b < fsys.bitmapMgr.TotalBlocks(); b++ {
		allocated, err := fsys.bitmapMgr.IsAllocated(b)
		if err != nil {
			t.Fatal(err)
		}
		if allocated {
			out[b] = true
		}
	}
	return out
}

// crashWhileFreeing runs op until its block free waits on a refcount lock another
// node holds and stuck reports its earlier changes on disk, then crashes the
// node there by closing its device.
func crashWhileFreeing(t *testing.T, fsys *FS, op func(), stuck func() bool) {
	t.Helper()
	holdRefcountLock(t, fsys)
	go op()
	deadline := time.Now().Add(10 * time.Second)
	for !stuck() {
		if time.Now().After(deadline) {
			t.Fatal("the operation did not reach its block free")
		}
		time.Sleep(10 * time.Millisecond)
	}
	fsys.dev.Close()
}

// replayAndRepair remounts after the crash, replays the journal, and checks that
// fsck -repair leaves exactly before minus released allocated. Replay may leave
// any of released allocated; check must report those as leaked and repair must
// free them and nothing else. It returns the image mounted again.
func replayAndRepair(t *testing.T, path string, offset, size uint64, before map[uint64]bool, released []uint64) *FS {
	t.Helper()
	want := maps.Clone(before)
	for _, b := range released {
		delete(want, b)
	}
	remounted := remountAndReplay(t, path, offset, size)
	var leaked []uint64
	for b := range allocatedBlocks(t, remounted) {
		if !want[b] {
			leaked = append(leaked, b)
		}
	}
	slices.Sort(leaked)
	for _, b := range leaked {
		if !slices.Contains(released, b) {
			t.Fatalf("block %d is allocated after replay but the operation did not release it", b)
		}
	}
	for b := range want {
		if allocated, _ := remounted.bitmapMgr.IsAllocated(b); !allocated {
			t.Fatalf("replay freed block %d, which should stay allocated", b)
		}
	}

	check := runFsck(t, remounted, fsck.DefaultOptions())
	for _, b := range leaked {
		if !strings.Contains(problemList(check), fmt.Sprintf("block %d: leaked", b)) {
			t.Fatalf("check did not report block %d, which replay left allocated:%s", b, problemList(check))
		}
	}
	if len(leaked) == 0 && strings.Contains(problemList(check), "leaked") {
		t.Fatalf("check reported leaks although replay freed every released block:%s", problemList(check))
	}
	repair := fsck.DefaultOptions()
	repair.Mode = fsck.Repair
	wantExit := fsck.ExitClean
	if len(leaked) > 0 {
		wantExit = fsck.ExitCorrected
	}
	if r := runFsck(t, remounted, repair); r.ExitCode() != wantExit {
		t.Fatalf("repair exit %d, want %d:%s", r.ExitCode(), wantExit, problemList(r))
	}
	expectCleanFsck(t, remounted)

	remounted = openFormattedFS(t, path)
	if got := allocatedBlocks(t, remounted); !maps.Equal(got, want) {
		t.Fatalf("allocated after repair %v, want %v", slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(want)))
	}
	return remounted
}

// A crash after a truncate wrote the shorter inode, but before it freed the
// blocks it cut: replay leaves those blocks allocated (or, once it frees them,
// frees them), and repair frees whatever replay left.
func TestFsckRepairReclaimsInterruptedTruncate(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	victim := createFile(t, fsys, "victim.bin")
	writeAt(t, victim, pattern(1, 3*BlockSize), 0)
	other := createFile(t, fsys, "other.bin")
	writeAt(t, other, pattern(2, 2*BlockSize), 0)
	cut := []uint64{physicalBlock(t, fsys, victim.inodeNum, 1), physicalBlock(t, fsys, victim.inodeNum, 2)}
	before := allocatedBlocks(t, fsys)

	crashWhileFreeing(t, fsys, func() { tryTruncate(victim, BlockSize) }, func() bool {
		ino, err := fsys.inodeTbl.Read(victim.inodeNum)
		return err == nil && ino.Size == BlockSize
	})

	remounted := replayAndRepair(t, path, offset, size, before, cut)
	checkFile(t, lookupFile(t, remounted, "victim.bin"), pattern(1, BlockSize))
	checkFile(t, lookupFile(t, remounted, "other.bin"), pattern(2, 2*BlockSize))
}

// A crash while unlinking a file in a subdirectory, after its entry is gone but
// before its blocks are freed: replay deletes the inode, and leaves the blocks
// allocated (or, once it frees them, frees them). Repair frees whatever is left.
func TestFsckRepairReclaimsInterruptedSubdirectoryUnlink(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	ctx := context.Background()
	sub := newRegularFile(t, fsys)
	subIno, err := fsys.updateInode(ctx, sub.inodeNum, func(ino *inode.Inode) error {
		ino.Mode = inode.S_IFDIR | 0o755
		ino.LinkCount = 2
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.dirMgr.InitDirectory(ctx, subIno, uint64(inode.InodeRoot)); err != nil {
		t.Fatal(err)
	}
	if err := fsys.inodeTbl.Write(ctx, subIno); err != nil {
		t.Fatal(err)
	}
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.dirMgr.Add(ctx, root, "sub", uint64(sub.inodeNum), dir.DT_DIR); err != nil {
		t.Fatal(err)
	}
	victim := newRegularFile(t, fsys)
	if _, err := fsys.updateInode(ctx, victim.inodeNum, func(ino *inode.Inode) error {
		ino.LinkCount = 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if subIno, err = fsys.inodeTbl.Read(sub.inodeNum); err != nil {
		t.Fatal(err)
	}
	if err := fsys.dirMgr.Add(ctx, subIno, "victim.bin", uint64(victim.inodeNum), dir.DT_REG); err != nil {
		t.Fatal(err)
	}
	writeAt(t, victim, pattern(1, 2*BlockSize), 0)
	other := createFile(t, fsys, "other.bin")
	writeAt(t, other, pattern(2, BlockSize), 0)
	released := []uint64{physicalBlock(t, fsys, victim.inodeNum, 0), physicalBlock(t, fsys, victim.inodeNum, 1)}
	before := allocatedBlocks(t, fsys)

	subDir := &Dir{fs: fsys, inodeNum: sub.inodeNum}
	crashWhileFreeing(t, fsys, func() { subDir.Unlink(ctx, "victim.bin") }, func() bool {
		ino, err := fsys.inodeTbl.Read(sub.inodeNum)
		if err != nil {
			return false
		}
		_, _, err = fsys.dirMgr.Lookup(ctx, ino, "victim.bin")
		return errors.Is(err, dir.ErrNotFound)
	})

	remounted := replayAndRepair(t, path, offset, size, before, released)
	subIno, err = remounted.inodeTbl.Read(sub.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := remounted.dirMgr.Lookup(ctx, subIno, "victim.bin"); !errors.Is(err, dir.ErrNotFound) {
		t.Fatalf("sub/victim.bin after replay: %v, want it gone", err)
	}
	checkFile(t, lookupFile(t, remounted, "other.bin"), pattern(2, BlockSize))
}
