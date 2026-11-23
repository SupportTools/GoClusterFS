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

func formattedDevice(t *testing.T) (*storage.Device, *superblock.Superblock) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fs.img")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, superblock.MinDeviceSize); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	sb, err := superblock.Format(dev, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return dev, sb
}

// Enable, as fsck -enable-quotas runs it with no node mounted, creates the table inode and its first block, then
// names it in the superblock and sets RequiredProjectQuotas. A second Enable changes nothing.
func TestEnableCreatesTheTable(t *testing.T) {
	dev, sb := formattedDevice(t)
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
	if onDisk.RequiredFlags&superblock.RequiredProjectQuotas == 0 || onDisk.QuotaInode == 0 || onDisk.QuotaFlushMs != DefaultFlushMs || onDisk.QuotaTableBlocks != 1 {
		t.Fatalf("superblock flags %#x, quota inode %d, flush %d ms, %d table blocks; want quotas on with one block",
			onDisk.RequiredFlags, onDisk.QuotaInode, onDisk.QuotaFlushMs, onDisk.QuotaTableBlocks)
	}
	ino, err := inodes.Read(onDisk.QuotaInode)
	if err != nil || ino.AllocatedBlocks != 1 || ino.InlineExtents[0].Length != 1 || ino.ProjectID != 0 {
		t.Fatalf("quota inode %+v, %v; want one block and no project", ino, err)
	}
	block := ino.InlineExtents[0].PhysicalBlock
	if allocated, err := blocks.IsAllocated(block); err != nil || !allocated {
		t.Errorf("table block %d allocated %v, %v; want allocated", block, allocated, err)
	}
	sector := storage.NewAlignedBuffer(storage.SectorSize)
	base := int64(onDisk.DataRegionOffset + block*uint64(onDisk.BlockSize))
	if _, err := dev.ReadAt(sector.Bytes(), base); err != nil {
		t.Fatal(err)
	}
	if h, err := DecodeHeader(sector.Bytes()); err != nil || h.NextProjectID != 1 {
		t.Errorf("header %+v, %v; want NextProjectID 1", h, err)
	}
	if _, err := dev.ReadAt(sector.Bytes(), base+int64(onDisk.BlockSize)-storage.SectorSize); err != nil {
		t.Fatal(err)
	}
	if r, err := GetRecord(sector.Bytes(), 0); err != nil || r.Flags&RecordInUse != 0 {
		t.Errorf("last record sector: %+v, %v; want unused records", r, err)
	}

	if err := Enable(ctx, dev, onDisk, inodes, blocks); !errors.Is(err, ErrEnabled) {
		t.Errorf("a second Enable: %v, want ErrEnabled", err)
	}
}
