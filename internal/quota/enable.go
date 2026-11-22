package quota

import (
	"context"
	"errors"
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// DefaultFlushMs is how often a node flushes its usage deltas into the table.
const DefaultFlushMs = 1000

// ErrEnabled means the volume has a quota table already.
var ErrEnabled = errors.New("the volume has project quotas already")

// Enable creates the quota table on a volume no node has mounted, or that this node has just formatted: a hidden
// inode whose first data block holds the header and empty records. The superblock names the inode and gets
// RequiredProjectQuotas last, so an interrupted Enable leaves only an unnamed inode, which fsck -repair frees.
func Enable(ctx context.Context, dev *storage.Device, sb *superblock.Superblock, inodes *inode.Table, blocks *bitmap.Region) error {
	if sb.RequiredFlags&superblock.RequiredProjectQuotas != 0 {
		return ErrEnabled
	}
	blockSize := uint64(sb.BlockSize)
	if blockSize < 2*storage.SectorSize || blockSize%storage.SectorSize != 0 {
		return fmt.Errorf("a %d-byte block cannot hold the quota table's header and a record sector", blockSize)
	}
	num, err := inodes.FindFreeSlot()
	if err != nil {
		return fmt.Errorf("no free inode for the quota table: %w", err)
	}
	ino, err := inodes.ClaimSlotAs(ctx, num, func(ino *inode.Inode) { ino.Mode = inode.S_IFREG | 0o600 })
	if err != nil {
		return fmt.Errorf("claiming inode %d for the quota table: %w", num, err)
	}
	got, err := blocks.Alloc(ctx, 1)
	if err != nil {
		return fmt.Errorf("allocating the quota table's first block: %w", err)
	}
	block := got[0]

	buf := storage.NewAlignedBuffer(int(blockSize))
	EncodeHeader(&Header{NextProjectID: 1}, buf.Bytes()[:storage.SectorSize])
	empty := EmptyRecordSector()
	for off := storage.SectorSize; off < int(blockSize); off += storage.SectorSize {
		copy(buf.Bytes()[off:], empty)
	}
	if _, err := dev.WriteAt(buf.Bytes(), int64(sb.DataRegionOffset+block*blockSize)); err != nil {
		return fmt.Errorf("writing the quota table: %w", err)
	}

	ino.InlineExtents[0] = inode.InlineExtent{LogicalBlock: 0, PhysicalBlock: block, Length: 1}
	ino.ExtentCount, ino.AllocatedBlocks, ino.WrittenBlocks, ino.Size = 1, 1, 1, blockSize
	if err := inodes.Write(ctx, ino); err != nil {
		return fmt.Errorf("writing quota table inode %d: %w", num, err)
	}
	if err := dev.Sync(); err != nil {
		return err
	}

	sb.QuotaInode, sb.QuotaFlushMs, sb.QuotaTableBlocks = num, DefaultFlushMs, 1
	sb.RequiredFlags |= superblock.RequiredProjectQuotas
	return superblock.Write(dev, sb)
}
