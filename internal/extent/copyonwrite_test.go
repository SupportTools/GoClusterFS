package extent

import (
	"context"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/refcount"
)

// CopyOnWrite performs copy-on-write for the shared block at logicalBlock.
// It allocates a new block, copies the data, and maps just that block to the
// copy, splitting a multi-block extent if needed. On a copy, ext is updated to
// the new single-block mapping.
// The caller must decrement the old block's refcount after this returns.
func (m *Manager) CopyOnWrite(ctx context.Context, ino *inode.Inode, ext *Extent,
	logicalBlock uint64, dataOffset uint64, refcountReg *refcount.Region) (*CoWResult, error) {

	cur, offset, err := m.Lookup(ino, logicalBlock)
	if err != nil {
		return nil, err
	}
	result := &CoWResult{
		OldPhysBlock: cur.PhysicalBlock + offset,
		Uncounted:    cur.Flags&FlagShared != 0 && cur.Flags&FlagCounted == 0,
	}
	block := Extent{logicalBlock, result.OldPhysBlock, 1, cur.Flags}

	needsCoW, err := m.NeedsCoW(ctx, &block, refcountReg)
	if err != nil {
		return nil, err
	}
	if !needsCoW {
		result.NewPhysBlock = result.OldPhysBlock
		return result, nil
	}

	mapped, err := m.MapCopy(ctx, ino, logicalBlock)
	if err != nil {
		return nil, err
	}
	if err := m.CopyBlockData(mapped, dataOffset); err != nil {
		m.DiscardPending(ctx, ino)
		m.bitmap.Free(ctx, mapped.NewPhysBlock)
		return nil, err
	}
	*ext = Extent{logicalBlock, mapped.NewPhysBlock, 1, cur.Flags &^ (FlagShared | FlagCounted)}
	return mapped, nil
}
