package fsck_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	fuzzBlocks       = 29
	fuzzBitmap       = 1 << 20
	fuzzInodes       = 2 << 20
	fuzzRegistry     = 3 << 20
	fuzzRefcounts    = 9 << 20
	fuzzSnapshots    = 10 << 20
	fuzzDataOffset   = 11 << 20
	fuzzSnapshotSize = 1 << 20
)

// fuzzTarget is a metadata sector the fuzzer overwrites. fix recomputes its
// checksums so that the bytes reach the parsers behind them.
type fuzzTarget struct {
	name string
	off  int64
	fix  func([]byte)
}

func sectorCRC(b []byte) {
	binary.LittleEndian.PutUint32(b[storage.SectorSize-4:], storage.CRC32C(b[:storage.SectorSize-4]))
}

func inodeCRCs(b []byte) {
	for i := 0; i < storage.SectorSize; i += inode.InodeSize {
		binary.LittleEndian.PutUint32(b[i+inode.InodeSize-4:], storage.CRC32C(b[i:i+inode.InodeSize-4]))
	}
}

func snapshotCRCs(b []byte) {
	for i := 0; i+snapshot.DescriptorSize <= snapshot.BytesPerSector; i += snapshot.DescriptorSize {
		d := b[i : i+snapshot.DescriptorSize]
		binary.LittleEndian.PutUint32(d[snapshot.DescriptorSize-4:], storage.CRC32C(d[:snapshot.DescriptorSize-4]))
	}
	sectorCRC(b)
}

func treeNodeCRC(b []byte) {
	const header, entry = 64, 32
	count := int(binary.LittleEndian.Uint32(b[12:16]))
	if count > (storage.SectorSize-header)/entry {
		return
	}
	crc := storage.CRC32CUpdate(storage.CRC32C(b[:60]), b[header:header+count*entry])
	binary.LittleEndian.PutUint32(b[60:64], crc)
}

// fuzzImage builds a small filesystem with a directory, plain, shared and
// extent tree files, and a snapshot, and lists the sectors to corrupt.
func fuzzImage(tb testing.TB) (*storage.Device, []fuzzTarget) {
	dev, cleanup := testDevice(tb, 40)
	tb.Cleanup(cleanup)
	writeSuperblock(tb, dev, fuzzBlocks)

	ctx := context.Background()
	var uuid [16]byte
	must := func(err error) {
		tb.Helper()
		if err != nil {
			tb.Fatal(err)
		}
	}
	bm := bitmap.NewRegion(dev, fuzzBitmap, fuzzBlocks, nil, uuid)
	tbl := inode.NewTable(dev, fuzzInodes, 4096*inode.InodeSize, nil, uuid)
	mgr := extent.NewManager(dev, bm, nil, fuzzDataOffset)
	tbl.SetTreeFlusher(mgr)
	refs := refcount.NewRegion(dev, fuzzRefcounts, refcount.SizeForBlocks(fuzzBlocks), fuzzBlocks, nil)
	mgr.SetRefcounts(refs)
	dirs := dir.NewManager(dev, tbl, mgr, nil, fuzzDataOffset)

	must(tbl.InitRootInode(ctx))
	root, err := tbl.Read(inode.InodeRoot)
	must(err)
	must(dirs.InitDirectory(ctx, root, uint64(inode.InodeRoot)))
	must(tbl.Write(ctx, root))

	newFile := func(name string) *inode.Inode {
		num, err := tbl.FindFreeSlot()
		must(err)
		ino, err := tbl.ClaimSlot(ctx, num)
		must(err)
		ino.Mode, ino.LinkCount = inode.S_IFREG|0o644, 1
		root, err := tbl.Read(inode.InodeRoot)
		must(err)
		must(dirs.Add(ctx, root, name, uint64(num), dir.DT_REG))
		return ino
	}
	plain, a, b := newFile("plain"), newFile("a"), newFile("b")
	_, err = mgr.AllocateBlocks(ctx, plain, 0, 2)
	must(err)
	for blk := uint64(0); blk < 3; blk++ {
		for _, ino := range []*inode.Inode{a, b} {
			_, err := mgr.AllocateBlocks(ctx, ino, blk, 1)
			must(err)
		}
	}
	for _, ino := range []*inode.Inode{plain, a, b} {
		ino.Size = 2 * extent.BlockSize
		must(tbl.Write(ctx, ino))
	}
	if !a.HasExtentTree() {
		tb.Fatal("inode a should use an extent tree")
	}

	blocks, err := mgr.DataBlocks(plain)
	must(err)
	must(refs.Share(ctx, blocks))
	clone := newFile("clone")
	must(mgr.CopyExtentsForClone(ctx, plain, clone))
	clone.Size = plain.Size
	must(tbl.Write(ctx, clone))
	must(mgr.MarkAllShared(plain))
	must(tbl.Write(ctx, plain))

	snaps := snapshot.NewRegistry(dev, fuzzSnapshots, fuzzSnapshotSize, nil)
	_, err = snaps.Create(ctx, "snap", plain.InodeNum, 0, 0, 2, plain.Size, snapshot.FlagReadOnly)
	must(err)

	root, err = tbl.Read(inode.InodeRoot)
	must(err)
	rootBlock, err := mgr.LookupPhysical(root, 0)
	must(err)
	targets := []fuzzTarget{
		{"superblock", 0, sectorCRC},
		{"bitmap", fuzzBitmap, sectorCRC},
		{"inodes", fuzzInodes, inodeCRCs},
		{"node registry", fuzzRegistry, sectorCRC},
		{"refcounts", fuzzRefcounts, sectorCRC},
		{"snapshots", fuzzSnapshots, snapshotCRCs},
		{"extent tree", extent.TreeNodeOffset(fuzzDataOffset, a.ExtentTreeRoot, a.ExtentTreeGen), treeNodeCRC},
		{"root directory", int64(fuzzDataOffset + rootBlock*extent.BlockSize), func([]byte) {}},
	}
	return dev, targets
}

// FuzzCheck corrupts one metadata sector and runs a check and a repair dry
// run over the image. Neither may panic or hang.
func FuzzCheck(f *testing.F) {
	f.Cleanup(fsck.SetHeartbeatWatch(0, 0))
	dev, targets := fuzzImage(f)
	original := make([][]byte, len(targets))
	for i, tgt := range targets {
		buf := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
		if _, err := dev.ReadAt(buf, tgt.off); err != nil {
			f.Fatal(err)
		}
		original[i] = buf
		f.Add(uint8(i), false, bytes.Clone(buf))
		f.Add(uint8(i), true, []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0xff})
	}
	if r := fsck.New(dev, fsck.Options{Output: &bytes.Buffer{}}).Check(); r.UnrepairedErrors() > 0 || r.Aborted {
		for _, p := range r.Problems {
			f.Log(p.String())
		}
		f.Fatalf("fuzz image is not clean before corruption (exit %d)", r.ExitCode())
	}

	f.Fuzz(func(t *testing.T, target uint8, fix bool, data []byte) {
		i := int(target) % len(targets)
		buf := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
		copy(buf, original[i])
		copy(buf, data)
		if fix {
			targets[i].fix(buf)
		}
		if _, err := dev.WriteAt(buf, targets[i].off); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := dev.WriteAt(original[i], targets[i].off); err != nil {
				t.Fatal(err)
			}
		}()

		fsck.New(dev, fsck.Options{Output: &bytes.Buffer{}, StopOnFatal: true}).Check()
		fsck.New(dev, fsck.Options{Output: &bytes.Buffer{}, Mode: fsck.Repair, DryRun: true}).Check()
	})
}
