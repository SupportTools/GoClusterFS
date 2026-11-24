package quota

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

func formattedDeviceAt(t *testing.T, blockSize uint64) (*storage.Device, *superblock.Superblock) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fs.img")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, superblock.MinDeviceSize+64<<20); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	sb, err := superblock.Format(dev, blockSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	return dev, sb
}

// Enable, as fsck -enable-quotas runs it with no node mounted, creates the table inode with 1 MiB of blocks in a row,
// 16,065 project records at every block size, then names it in the superblock and sets RequiredProjectQuotas. A
// second Enable changes nothing.
func TestEnableCreatesTheTable(t *testing.T) {
	for name, bs := range map[string]uint64{"default blocks": 0, "64 KB blocks": 64 << 10, "8 KB blocks": superblock.MinBlockSize} {
		t.Run(name, func(t *testing.T) { enableCreatesTheTable(t, bs) })
	}
}

func enableCreatesTheTable(t *testing.T, blockSize uint64) {
	dev, sb := formattedDeviceAt(t, blockSize)
	ctx := context.Background()
	inodes := inode.NewTable(dev, sb.InodeTableOffset, sb.InodeTableSize, nil, [16]byte{})
	blocks := bitmap.NewRegion(dev, sb.BlockBitmapOffset, sb.TotalBlocks, nil, [16]byte{})
	if err := Enable(ctx, dev, sb, inodes, blocks); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	onDisk, err := superblock.Read(dev)
	if err != nil {
		t.Fatal(err)
	}
	bs := uint64(onDisk.BlockSize)
	n := TableBlocks(bs)
	if onDisk.RequiredFlags&superblock.RequiredProjectQuotas == 0 || onDisk.QuotaInode == 0 || onDisk.QuotaFlushMs != DefaultFlushMs || uint64(onDisk.QuotaTableBlocks) != n {
		t.Fatalf("superblock flags %#x, quota inode %d, flush %d ms, %d table blocks; want quotas on with %d blocks",
			onDisk.RequiredFlags, onDisk.QuotaInode, onDisk.QuotaFlushMs, onDisk.QuotaTableBlocks, n)
	}
	ino, err := inodes.Read(onDisk.QuotaInode)
	if err != nil || ino.AllocatedBlocks != n || ino.InlineExtents[0].Length != n || ino.ProjectID != 0 {
		t.Fatalf("quota inode %+v, %v; want %d blocks in one extent and no project", ino, err, n)
	}
	block := ino.InlineExtents[0].PhysicalBlock
	for b := block; b < block+n; b++ {
		if allocated, err := blocks.IsAllocated(b); err != nil || !allocated {
			t.Fatalf("table block %d allocated %v, %v; want allocated", b, allocated, err)
		}
	}
	tbl, err := Open(dev, inodes, onDisk.QuotaInode, onDisk.DataRegionOffset, bs)
	if err != nil || tbl.Capacity() != 16065 {
		t.Fatalf("the table holds %d records, %v; want 16065", tbl.Capacity(), err)
	}
	if h, err := tbl.Header(); err != nil || h.NextProjectID != 1 {
		t.Errorf("header %+v, %v; want NextProjectID 1", h, err)
	}
	if r, err := tbl.Record(tbl.Capacity() - 1); err != nil || r.Flags&RecordInUse != 0 {
		t.Errorf("last record: %+v, %v; want unused", r, err)
	}

	if err := Enable(ctx, dev, onDisk, inodes, blocks); !errors.Is(err, ErrEnabled) {
		t.Errorf("a second Enable: %v, want ErrEnabled", err)
	}
}

// FreeID finds the first record not in use from where it starts, across record sectors, and wraps to project 1.
func TestFreeIDCrossesSectorsAndWraps(t *testing.T) {
	dev, sb := formattedDeviceAt(t, superblock.MinBlockSize)
	ctx := context.Background()
	inodes := inode.NewTable(dev, sb.InodeTableOffset, sb.InodeTableSize, nil, [16]byte{})
	if err := Enable(ctx, dev, sb, inodes, bitmap.NewRegion(dev, sb.BlockBitmapOffset, sb.TotalBlocks, nil, [16]byte{})); err != nil {
		t.Fatal(err)
	}
	tbl, err := Open(dev, inodes, sb.QuotaInode, sb.DataRegionOffset, uint64(sb.BlockSize))
	if err != nil {
		t.Fatal(err)
	}
	last := tbl.Capacity() - 1
	for _, id := range append([]uint32{last}, func() (ids []uint32) {
		for id := uint32(1); id <= 2*RecordsPerSector+5; id++ {
			ids = append(ids, id)
		}
		return ids
	}()...) {
		if err := tbl.PutRecord(id, &Record{Flags: RecordInUse}); err != nil {
			t.Fatal(err)
		}
	}
	for from, want := range map[uint32]uint32{1: 2*RecordsPerSector + 6, 70: 2*RecordsPerSector + 6, last: 2*RecordsPerSector + 6, last - 1: last - 1} {
		if got, err := tbl.FreeID(from); err != nil || got != want {
			t.Errorf("FreeID(%d) = %d, %v; want %d", from, got, err, want)
		}
	}
}
