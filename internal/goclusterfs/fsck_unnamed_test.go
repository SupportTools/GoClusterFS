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
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// unlinkSkippedByReplay leaves root file name as an unnamed inode the way a
// skipped replay record does. The unlink removes the name and waits for the
// inode lock, which another node holds because it has the file open, and the
// node crashes. The other node then writes the inode, so replay skips the
// unlink's ExtentFree and InodeDelete.
func unlinkSkippedByReplay(t *testing.T, fsys *FS, path string, offset, size uint64, name string, num uint32) *FS {
	t.Helper()
	ctx := context.Background()
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	otherDev, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { otherDev.Close() })
	slot := int(snapFSNodes.Add(1)-1) % 64
	var otherUUID [16]byte
	copy(otherUUID[:], "openfileholder12")
	otherUUID[15] = byte(slot)
	other := lock.NewManager(otherDev, sb.LockBitmapOffset, sb.LockBitmapSize, otherUUID, slot)
	if err := other.Acquire(ctx, lock.LockTypeInode, uint64(num), lock.LockModeExclusive); err != nil {
		t.Fatal(err)
	}

	go fsys.Unlink(ctx, name)
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		root, err := fsys.inodeTbl.Read(inode.InodeRoot)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := fsys.dirMgr.Lookup(ctx, root, name); errors.Is(err, dir.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the unlink never removed its entry")
		}
	}
	fsys.dev.Close()
	if err := other.Release(lock.LockTypeInode, uint64(num)); err != nil {
		t.Fatal(err)
	}

	if _, err := openFormattedFS(t, path).updateInodeLocked(ctx, num, func(ino *inode.Inode) error {
		ino.Mtime++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	remounted := remountAndReplay(t, path, offset, size)
	if !inodeInUse(t, remounted, num) {
		t.Fatalf("replay freed inode %d although another node wrote it after the unlink", num)
	}
	return remounted
}

func inodeInUse(t *testing.T, fsys *FS, num uint32) bool {
	t.Helper()
	ino, err := fsys.inodeTbl.Read(num)
	if err != nil {
		t.Fatal(err)
	}
	return ino.Magic == inode.InodeMagic
}

func repairOpts() fsck.Options {
	opts := fsck.DefaultOptions()
	opts.Mode = fsck.Repair
	return opts
}

func expectProblem(t *testing.T, r *fsck.Result, want string) {
	t.Helper()
	if !strings.Contains(problemList(r), want) {
		t.Fatalf("fsck did not report %q:%s", want, problemList(r))
	}
}

// Replay skips an unlink's InodeDelete and leaves a file with an extent tree
// that no directory names. A dry run changes nothing; repair frees the inode,
// its data and its tree nodes, and the check after it is clean.
func TestFsckRepairFreesInodeReplayLeftUnnamed(t *testing.T) {
	t.Parallel()
	fsys, path, offset, size := journaledFS(t)
	keep := createFile(t, fsys, "keep.bin")
	writeAt(t, keep, pattern(9, BlockSize), 0)
	want := allocatedBlocks(t, fsys)

	f := createFile(t, fsys, "doomed.bin")
	for i := range int64(3) {
		writeAt(t, f, pattern(byte(i+1), BlockSize/4), 2*i*BlockSize)
	}
	if ino, err := fsys.inodeTbl.Read(f.inodeNum); err != nil || ino.Flags&inode.FlagHasExtentTree == 0 {
		t.Fatalf("doomed.bin has no extent tree (flags %#x, err %v)", ino.Flags, err)
	}
	remounted := unlinkSkippedByReplay(t, fsys, path, offset, size, "doomed.bin", f.inodeNum)

	reported := fmt.Sprintf("inode %d: unnamed: no directory names it", f.inodeNum)
	expectProblem(t, runFsck(t, remounted, fsck.DefaultOptions()), reported+" [FIXABLE]")
	dry := repairOpts()
	dry.DryRun = true
	expectProblem(t, runFsck(t, remounted, dry), reported+" [WOULD FIX]")
	if !inodeInUse(t, remounted, f.inodeNum) {
		t.Fatal("dry run freed the inode")
	}

	r := runFsck(t, remounted, repairOpts())
	if r.ExitCode() != fsck.ExitCorrected {
		t.Fatalf("repair exit %d, want %d:%s", r.ExitCode(), fsck.ExitCorrected, problemList(r))
	}
	expectProblem(t, r, reported+" [FIXED]")
	if strings.Contains(problemList(r), "leaked") {
		t.Fatalf("repair reported the blocks it freed with the inode as leaked:%s", problemList(r))
	}
	expectCleanFsck(t, remounted)
	reopened := openFormattedFS(t, path)
	if inodeInUse(t, reopened, f.inodeNum) {
		t.Errorf("inode %d still in use after repair", f.inodeNum)
	}
	if got := allocatedBlocks(t, reopened); !maps.Equal(got, want) {
		t.Errorf("allocated after repair %v, want %v", slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(want)))
	}
	checkFile(t, lookupFile(t, reopened, "keep.bin"), pattern(9, BlockSize))
}

// A clone shares the blocks of the file replay leaves unnamed. Repair frees the
// inode and gives the blocks to the clone alone.
func TestFsckRepairFreesUnnamedInodeKeepsCloneBlocks(t *testing.T) {
	t.Parallel()
	fsys, path, offset, size := journaledFS(t)
	data := pattern(3, 2*BlockSize)
	f := createFile(t, fsys, "doomed.bin")
	writeAt(t, f, data, 0)
	blocks := cloneShares(t, fsys, f, 2)
	remounted := unlinkSkippedByReplay(t, fsys, path, offset, size, "doomed.bin", f.inodeNum)
	expectRefcounts(t, remounted, blocks, 2)

	if r := runFsck(t, remounted, repairOpts()); r.ExitCode() != fsck.ExitCorrected {
		t.Fatalf("repair exit %d, want %d:%s", r.ExitCode(), fsck.ExitCorrected, problemList(r))
	}
	expectCleanFsck(t, remounted)
	reopened := openFormattedFS(t, path)
	if inodeInUse(t, reopened, f.inodeNum) {
		t.Errorf("inode %d still in use after repair", f.inodeNum)
	}
	expectAllocated(t, reopened, blocks, true)
	expectRefcounts(t, reopened, blocks, 1)
	checkFile(t, lookupFile(t, reopened, "clone.bin"), data)
}

// A snapshot refers to the file replay leaves unnamed and shares its blocks.
// Repair keeps the inode and changes nothing.
func TestFsckRepairKeepsUnnamedSnapshotTarget(t *testing.T) {
	t.Parallel()
	fsys, path, offset, size := journaledFS(t)
	f := createFile(t, fsys, "doomed.bin")
	writeAt(t, f, pattern(4, 2*BlockSize), 0)
	setxattr(t, f, "user.snapshot", "keep")
	blocks := []uint64{physicalBlock(t, fsys, f.inodeNum, 0), physicalBlock(t, fsys, f.inodeNum, 1)}
	remounted := unlinkSkippedByReplay(t, fsys, path, offset, size, "doomed.bin", f.inodeNum)
	expectRefcounts(t, remounted, blocks, 2)
	sb, err := superblock.Read(remounted.dev)
	if err != nil {
		t.Fatal(err)
	}
	image := func() []byte {
		return slices.Concat(
			readRegion(t, remounted, sb.BlockBitmapOffset, sb.BlockBitmapSize),
			readRegion(t, remounted, sb.RefCountTableOffset, sb.RefCountTableSize),
			readRegion(t, remounted, sb.InodeTableOffset, sb.InodeTableSize))
	}
	before := image()

	r := runFsck(t, remounted, repairOpts())
	expectProblem(t, r, fmt.Sprintf("inode %d: unnamed: no directory names it; kept, because a snapshot refers to it", f.inodeNum))
	if r.RepairedCount != 0 {
		t.Fatalf("repair changed %d things:%s", r.RepairedCount, problemList(r))
	}
	if !bytes.Equal(image(), before) {
		t.Fatal("repair changed the bitmap, refcounts or inodes")
	}
	reopened := openFormattedFS(t, path)
	expectAllocated(t, reopened, blocks, true)
	expectRefcounts(t, reopened, blocks, 2)
}

// unname removes name from the root directory and leaves its inode as it is.
func unname(t *testing.T, fsys *FS, name string) {
	t.Helper()
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.dirMgr.Remove(context.Background(), root, name); err != nil {
		t.Fatal(err)
	}
}

// Repair frees an unnamed file and an unnamed empty directory, and keeps
// unnamed inodes it must not free, or all of them when it may not know every
// name or snapshot reference.
func TestFsckRepairKeepsUnnamedInodesItCannotPlace(t *testing.T) {
	t.Parallel()
	type setup struct {
		fsys   *FS
		sb     *superblock.Superblock
		opts   fsck.Options
		keep   []uint32
		free   []uint32
		report []string
		after  func(t *testing.T, fsys *FS)
	}
	cases := []struct {
		name  string
		setup func(t *testing.T, s *setup)
	}{
		{"EmptyDirectory", func(t *testing.T, s *setup) {
			s.free = append(s.free, newSubdir(t, s.fsys, "empty").inodeNum)
			unname(t, s.fsys, "empty")
		}},
		{"DirectoryThatIsNotEmpty", func(t *testing.T, s *setup) {
			d := newSubdir(t, s.fsys, "full")
			newDirIn(t, s.fsys, d.inodeNum, "inner")
			unname(t, s.fsys, "full")
			s.keep = append(s.keep, d.inodeNum)
			s.report = append(s.report, fmt.Sprintf("inode %d: unnamed: no directory names it; kept, because it is a directory that is not empty", d.inodeNum))
		}},
		{"ReservedInode", func(t *testing.T, s *setup) {
			if err := s.fsys.inodeTbl.Write(context.Background(), &inode.Inode{
				Magic: inode.InodeMagic, InodeNum: inode.InodeBadBlocks, Mode: inode.S_IFREG | 0o644, LinkCount: 1,
			}); err != nil {
				t.Fatal(err)
			}
			s.keep = append(s.keep, inode.InodeBadBlocks)
			s.report = append(s.report, "inode 1: unnamed: no directory names it; kept, because it is a reserved inode")
		}},
		{"UnreferencedSnapshotData", func(t *testing.T, s *setup) {
			other := createFile(t, s.fsys, "other.bin")
			writeAt(t, other, pattern(5, BlockSize), 0)
			setxattr(t, other, "user.snapshot", "snap")
			s.fsys.snapshotWkr.Stop()
			block := physicalBlock(t, s.fsys, other.inodeNum, 0)
			at, sector := snapshotDescriptor(t, s.fsys, s.sb)
			desc := sector.Bytes()[at%storage.SectorSize:][:fsck.SnapshotDescriptorSize]
			s.free = append(s.free, binary.LittleEndian.Uint32(desc[28:]))
			clear(desc)
			binary.LittleEndian.PutUint32(sector.Bytes()[storage.SectorSize-4:], storage.CRC32C(sector.Bytes()[:storage.SectorSize-4]))
			if _, err := s.fsys.dev.WriteAt(sector.Bytes(), int64(s.sb.SnapshotRegOffset)+int64(at/storage.SectorSize*storage.SectorSize)); err != nil {
				t.Fatal(err)
			}
			s.after = func(t *testing.T, fsys *FS) {
				expectRefcounts(t, fsys, []uint64{block}, 1)
				checkFile(t, lookupFile(t, fsys, "other.bin"), pattern(5, BlockSize))
			}
		}},
		{"SkippedSnapshotPhase", func(t *testing.T, s *setup) {
			s.opts.SkipPhases = map[fsck.Phase]bool{fsck.PhaseSnapshots: true}
			s.keep, s.free = append(s.keep, s.free...), nil
			s.report = append(s.report, "Phase 7 (Snapshots) did not finish")
		}},
		{"SkippedDirectoryPhase", func(t *testing.T, s *setup) {
			s.opts.SkipPhases = map[fsck.Phase]bool{fsck.PhaseDirectories: true}
			s.keep, s.free = append(s.keep, s.free...), nil
		}},
		{"DamagedDirectory", func(t *testing.T, s *setup) {
			sub := newSubdir(t, s.fsys, "sub")
			block := physicalBlock(t, s.fsys, sub.inodeNum, 0)
			flipByte(t, s.fsys, int64(s.sb.DataRegionOffset+block*BlockSize+8))
			s.keep, s.free = append(s.keep, s.free...), nil
			s.report = append(s.report, fmt.Sprintf("directory inode %d is not fully readable", sub.inodeNum))
		}},
		{"DamagedSnapshotDescriptor", func(t *testing.T, s *setup) {
			other := createFile(t, s.fsys, "other.bin")
			writeAt(t, other, pattern(5, BlockSize), 0)
			setxattr(t, other, "user.snapshot", "snap")
			s.fsys.snapshotWkr.Stop()
			at, _ := snapshotDescriptor(t, s.fsys, s.sb)
			flipByte(t, s.fsys, int64(s.sb.SnapshotRegOffset)+int64(at))
			s.keep, s.free = append(s.keep, s.free...), nil
			s.report = append(s.report, fmt.Sprintf("snapshot registry sector %d slot %d is damaged",
				at/storage.SectorSize, at%storage.SectorSize/fsck.SnapshotDescriptorSize))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := newFormattedImage(t)
			fsys := openFormattedFS(t, path)
			ctx := context.Background()
			if err := fsys.InitRoot(ctx); err != nil {
				t.Fatal(err)
			}
			sb, err := superblock.Read(fsys.dev)
			if err != nil {
				t.Fatal(err)
			}
			doomed := createFile(t, fsys, "doomed.bin")
			writeAt(t, doomed, pattern(1, BlockSize), 0)
			block := physicalBlock(t, fsys, doomed.inodeNum, 0)
			unname(t, fsys, "doomed.bin")
			s := &setup{fsys: fsys, sb: sb, opts: repairOpts(), free: []uint32{doomed.inodeNum}}
			tc.setup(t, s)

			r := runFsck(t, fsys, s.opts)
			for _, want := range s.report {
				expectProblem(t, r, want)
			}
			reopened := openFormattedFS(t, path)
			for _, num := range s.keep {
				if !inodeInUse(t, reopened, num) {
					t.Errorf("repair freed inode %d:%s", num, problemList(r))
				}
			}
			for _, num := range s.free {
				if inodeInUse(t, reopened, num) {
					t.Errorf("repair kept inode %d:%s", num, problemList(r))
				}
			}
			expectAllocated(t, reopened, []uint64{block}, slices.Contains(s.keep, doomed.inodeNum))
			if s.after != nil {
				s.after(t, reopened)
			}
		})
	}
}

// snapshotDescriptor returns the registry offset of the first snapshot
// descriptor and a copy of the sector that holds it.
func snapshotDescriptor(t *testing.T, fsys *FS, sb *superblock.Superblock) (int, *storage.AlignedBuffer) {
	t.Helper()
	reg := readRegion(t, fsys, sb.SnapshotRegOffset, sb.SnapshotRegSize)
	at := bytes.Index(reg, binary.LittleEndian.AppendUint32(nil, snapshot.DescriptorMagic))
	if at < 0 {
		t.Fatal("no snapshot descriptor in the registry")
	}
	start := at / storage.SectorSize * storage.SectorSize
	sector := storage.NewAlignedBuffer(storage.SectorSize)
	copy(sector.Bytes(), reg[start:start+storage.SectorSize])
	return at, sector
}
