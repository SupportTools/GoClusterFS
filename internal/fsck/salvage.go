package fsck

import (
	"context"
	"errors"
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/quota"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// Salvage says how far a salvaging release replayed a slot's journal.
type Salvage struct {
	Replayed  map[uint64]bool // the sequence numbers of the transactions it applied
	FailedSeq uint64          // the transaction that failed to apply; 0 when none did
	Err       error           // why the replay stopped before the end; nil when it reached the end
}

// salvage replays slot i's journal until a transaction fails to apply or a
// record cannot be read. It returns an error, leaving the journal for the next
// run, when renew fails or another registry slot changes: another node may then
// hold the slot or write beside the replay, which takes no cluster locks.
func (c *Checker) salvage(i int, renew func() error) (*Salvage, error) {
	s := &Salvage{Replayed: make(map[uint64]bool)}
	region, err := c.slotJournal(i)
	switch {
	case errors.Is(err, journal.ErrJournalUninitialized):
		return s, nil
	case err != nil:
		s.Err = rootCause(err)
		return s, nil
	}
	if !region.NeedsRecovery() {
		return s, nil
	}
	rctx := c.replayContext()
	if err := c.foldQuota(i, rctx.InodeTbl, region); err != nil {
		s.Err = fmt.Errorf("folding the journal's quota usage: %w", err)
		return s, nil
	}
	apply := journal.CreateTxnRecoveryHandler(rctx)
	handler := func(txn *journal.RecoveredTxn) error {
		if err := apply(txn); err != nil {
			s.FailedSeq = txn.Header.Seq
			return err
		}
		s.Replayed[txn.Header.Seq] = true
		return nil
	}
	var stopErr error
	check := func() error {
		if stopErr = renew(); stopErr == nil {
			stopErr = c.othersUnchanged(i)
		}
		return stopErr
	}
	_, s.Err = journal.RecoverReleasing(region, handler, check, rctx.ReleaseReserved(context.Background()))
	if stopErr != nil {
		return nil, stopErr
	}
	return s, nil
}

// foldQuota folds what slot i's journal owes the quota table, without locks like the rest of the salvage.
func (c *Checker) foldQuota(i int, inodes *inode.Table, region *journal.Region) error {
	sb := c.sbData
	if sb.RequiredFlags&superblock.RequiredProjectQuotas == 0 {
		return nil
	}
	open := func() (quota.Table, error) {
		return quota.Open(c.dev, inodes, sb.QuotaInode, sb.DataRegionOffset, uint64(sb.BlockSize))
	}
	return quota.NewGuard(nil).Fold(context.Background(), open, i, i, [16]byte{}, region)
}

// othersUnchanged fails once a registry slot other than i differs from what planning read, which means a node started.
func (c *Checker) othersUnchanged(i int) error {
	now, err := c.readRegistry()
	if err != nil {
		return fmt.Errorf("read the node registry: %w", err)
	}
	for _, changed := range changedSlots(c.registry, now) {
		if changed != i {
			slot, _ := node.ParseSlot(now[changed*node.SlotSize : (changed+1)*node.SlotSize])
			return fmt.Errorf("salvage stopped: %s changed its node registry slot, so a node may be running; "+
				"the journal was not reset. Unmount every node and run again once the slot's lease lapses", slotName(changed, slot))
		}
	}
	return nil
}

// replayContext builds the managers a replay needs, without locks: fsck replays only while no node runs.
func (c *Checker) replayContext() *journal.RecoveryContext {
	sb := c.sbData
	var uuid [16]byte
	bm := bitmap.NewRegion(c.dev, sb.BlockBitmapOffset, sb.TotalBlocks, nil, uuid)
	tbl := inode.NewTable(c.dev, sb.InodeTableOffset, sb.InodeTableSize, nil, uuid)
	mgr := extent.NewManager(c.dev, bm, nil, sb.DataRegionOffset, uint64(sb.BlockSize))
	mgr.SetRefcounts(refcount.NewRegion(c.dev, sb.RefCountTableOffset, sb.RefCountTableSize, sb.TotalBlocks, nil))
	tbl.SetTreeFlusher(mgr)
	return &journal.RecoveryContext{Dev: c.dev, InodeTbl: tbl, BitmapMgr: bm, DirMgr: dir.NewManager(c.dev, tbl, mgr, nil, sb.DataRegionOffset), ExtentMgr: mgr}
}
