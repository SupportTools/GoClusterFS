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

// extentAllocated is extent.FlagAllocated, which package extent cannot give without an import cycle once it counts usage.
const extentAllocated = 1 << 0

// ErrEnabled means the volume has a quota table already.
var ErrEnabled = errors.New("the volume has project quotas already")

// TableBytes is the size of the quota table Enable makes: 16,065 projects, whatever the block size.
const TableBytes = 1 << 20

// TableBlocks is how many blocks of blockSize the table Enable makes takes.
func TableBlocks(blockSize uint64) uint64 {
	return max(1, (TableBytes+blockSize-1)/blockSize)
}

// Enable creates the quota table on a volume no node has mounted, or that this node has just formatted: a hidden
// inode whose TableBytes of blocks in a row hold the header and empty records. The superblock names the inode and gets
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
	n := TableBlocks(blockSize)
	block, err := blocks.AllocContiguous(ctx, int(n))
	if err != nil {
		return fmt.Errorf("allocating %d blocks in a row for the quota table: %w", n, err)
	}

	buf := storage.NewAlignedBuffer(int(n * blockSize))
	EncodeHeader(&Header{NextProjectID: 1}, buf.Bytes()[:storage.SectorSize])
	empty := EmptyRecordSector()
	for off := storage.SectorSize; off < len(buf.Bytes()); off += storage.SectorSize {
		copy(buf.Bytes()[off:], empty)
	}
	if _, err := dev.WriteAt(buf.Bytes(), int64(sb.DataRegionOffset+block*blockSize)); err != nil {
		return fmt.Errorf("writing the quota table: %w", err)
	}

	ino.InlineExtents[0] = inode.InlineExtent{LogicalBlock: 0, PhysicalBlock: block, Length: n, Flags: extentAllocated}
	ino.ExtentCount, ino.AllocatedBlocks, ino.WrittenBlocks, ino.Size = 1, n, n, n*blockSize
	if err := inodes.Write(ctx, ino); err != nil {
		return fmt.Errorf("writing quota table inode %d: %w", num, err)
	}
	if err := dev.Sync(); err != nil {
		return err
	}

	sb.QuotaInode, sb.QuotaFlushMs, sb.QuotaTableBlocks = num, DefaultFlushMs, uint32(n)
	sb.RequiredFlags |= superblock.RequiredProjectQuotas
	return superblock.Write(dev, sb)
}
